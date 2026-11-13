//! The client: configuration, transport, retries and timeouts.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use serde::de::DeserializeOwned;

use crate::error::{ApiError, Error, Result};
use crate::meta::{DEFAULT_BASE_URL, USER_AGENT as SDK_USER_AGENT};
use crate::pagination::{CursorPage, ItemStream, Page};
use crate::request::{
    encode_path_segment, encode_query, parse_header, Auth, Request, RequestOptions,
};
use crate::response::{snippet, RawResponse, Response, ResponseMeta};
use crate::retry::{self, FailureCause, RetryDecision};
use crate::sse::{EventStream, StreamBody};

/// The default timeout of an ordinary call.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// The default timeout of a non-streaming chat turn.
pub const DEFAULT_LONG_TIMEOUT: Duration = Duration::from_secs(600);

/// The default time allowed to establish a connection.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The default time a stream may stay silent.
pub const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const API_KEY: HeaderName = HeaderName::from_static("x-api-key");

/// The Tungo Platform API client.
///
/// A client is immutable, cheap to clone (clones share one connection pool)
/// and safe to use from many tasks at once.
///
/// ```no_run
/// use tungo_sdk::Tungo;
///
/// # async fn run() -> tungo_sdk::Result<()> {
/// let client = Tungo::builder()
///     .api_key("tg_...")
///     .company_id("co_...")
///     .build()?;
/// let company = client.companies().get().await?;
/// println!("{}", company.name);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Tungo {
    inner: Arc<Inner>,
    options: RequestOptions,
    /// Whether the idempotency key of `options` has been sent with a request
    /// (see [`Tungo::with_options`]); shared by the clones of this client.
    key_used: Arc<AtomicBool>,
}

struct Inner {
    http: reqwest::Client,
    api_key: Option<String>,
    base_url: String,
    webhook_secret: Option<String>,
    company_id: Option<String>,
    workspace_id: Option<String>,
    timeout: Option<Duration>,
    max_retries: u32,
    default_headers: HeaderMap,
    stream_idle_timeout: Duration,
}

impl fmt::Debug for Tungo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = |secret: &Option<String>| secret.as_ref().map(|_| "[redacted]");
        f.debug_struct("Tungo")
            .field("base_url", &self.inner.base_url)
            .field("api_key", &redacted(&self.inner.api_key))
            .field("webhook_secret", &redacted(&self.inner.webhook_secret))
            .field("company_id", &self.inner.company_id)
            .field("workspace_id", &self.inner.workspace_id)
            .field("timeout", &self.inner.timeout)
            .field("max_retries", &self.inner.max_retries)
            .finish_non_exhaustive()
    }
}

/// Configures and builds a [`Tungo`] client.
///
/// The API key, the base URL and the webhook secret fall back to the
/// environment variables `TUNGO_API_KEY`, `TUNGO_BASE_URL` and
/// `TUNGO_WEBHOOK_SECRET` when they are not set explicitly.
#[derive(Clone, Default)]
#[must_use = "call `build()` to create the client"]
pub struct TungoBuilder {
    api_key: Option<String>,
    base_url: Option<String>,
    webhook_secret: Option<String>,
    company_id: Option<String>,
    workspace_id: Option<String>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    stream_idle_timeout: Option<Duration>,
    max_retries: Option<u32>,
    default_headers: Vec<(String, String)>,
    http_client: Option<reqwest::Client>,
    /// Why the client of `http_client_builder` could not be created.
    http_client_error: Option<String>,
    ignore_env: bool,
}

impl fmt::Debug for TungoBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TungoBuilder")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("company_id", &self.company_id)
            .field("workspace_id", &self.workspace_id)
            .finish_non_exhaustive()
    }
}

impl TungoBuilder {
    /// Sets the API key (otherwise `TUNGO_API_KEY`).
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Sets the base URL (otherwise `TUNGO_BASE_URL`, then `https://api.tungo.kz`).
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Sets the secret used by [`Tungo::verify_webhook`] (otherwise `TUNGO_WEBHOOK_SECRET`).
    pub fn webhook_secret(mut self, secret: impl Into<String>) -> Self {
        self.webhook_secret = Some(secret.into());
        self
    }

    /// Sets the company that company-scoped calls address.
    pub fn company_id(mut self, company_id: impl Into<String>) -> Self {
        self.company_id = Some(company_id.into());
        self
    }

    /// Sets the workspace that workspace-scoped calls address.
    pub fn workspace_id(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }

    /// Sets the total time allowed for one attempt of any call. Without it
    /// the timeout is 60 s, and 600 s for non-streaming chat turns.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the time allowed to establish a connection (30 s by default).
    /// It is ignored when a custom HTTP client is supplied.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets how long a stream may stay silent (120 s by default).
    pub fn stream_idle_timeout(mut self, timeout: Duration) -> Self {
        self.stream_idle_timeout = Some(timeout);
        self
    }

    /// Sets how many times a failed request is retried (2 by default).
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Adds a header sent with every request.
    pub fn default_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_headers.push((name.into(), value.into()));
        self
    }

    /// Uses a preconfigured `reqwest::Client` (proxy, TLS, pool settings).
    /// Prefer [`TungoBuilder::http_client_builder`], which cannot get the
    /// redirect policy wrong.
    ///
    /// # Security: the client must not follow redirects
    ///
    /// **Build the client with
    /// `.redirect(reqwest::redirect::Policy::none())`.** The API key travels
    /// in the `x-api-key` header, and reqwest keeps a custom header when it
    /// follows a redirect to another host: a client with the default policy
    /// hands the key to whatever host a redirect names. The SDK cannot read
    /// or change the policy of a client it did not create.
    ///
    /// As a safety net, every response is checked: when it comes from another
    /// origin (scheme, host or port) than the request was sent to, the call
    /// fails with [`Error::Config`] and the response is discarded. By then
    /// the key has already been sent to that origin, so treat this error as
    /// a leaked key: revoke it and fix the client.
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self.http_client_error = None;
        self
    }

    /// Uses a `reqwest::ClientBuilder` you configured (proxy, TLS, pool
    /// settings); the SDK finishes it. This is the safe way to customise the
    /// HTTP client: the SDK itself turns redirects off before building it, so
    /// the API key can never be forwarded to another host, whatever policy
    /// the builder carried. Like with [`TungoBuilder::http_client`], the
    /// connect timeout is the one of your builder.
    ///
    /// ```no_run
    /// # fn main() -> tungo_sdk::Result<()> {
    /// let http = tungo_sdk::reqwest::Client::builder().pool_max_idle_per_host(4);
    /// let client = tungo_sdk::Tungo::builder()
    ///     .api_key("tg_...")
    ///     .http_client_builder(http)
    ///     .build()?;
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    pub fn http_client_builder(mut self, builder: reqwest::ClientBuilder) -> Self {
        // The API key travels in a custom header, which is not removed when
        // a redirect leads to another host: a 3xx is an API error.
        match builder.redirect(reqwest::redirect::Policy::none()).build() {
            Ok(client) => {
                self.http_client = Some(client);
                self.http_client_error = None;
            }
            Err(error) => {
                self.http_client = None;
                self.http_client_error = Some(error.to_string());
            }
        }
        self
    }

    /// Does not read the `TUNGO_*` environment variables.
    pub fn ignore_env(mut self) -> Self {
        self.ignore_env = true;
        self
    }

    /// Builds the client. Fails when the base URL or a default header is not
    /// valid, or when the HTTP client cannot be created.
    pub fn build(self) -> Result<Tungo> {
        let env = |name: &str| {
            if self.ignore_env {
                return None;
            }
            std::env::var(name).ok().filter(|value| !value.is_empty())
        };
        let api_key = self
            .api_key
            .filter(|key| !key.is_empty())
            .or_else(|| env("TUNGO_API_KEY"));
        if let Some(key) = &api_key {
            HeaderValue::from_str(key)
                .map_err(|_| Error::Config("the API key contains invalid characters".to_owned()))?;
        }
        let base_url = self
            .base_url
            .filter(|url| !url.is_empty())
            .or_else(|| env("TUNGO_BASE_URL"))
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_owned());
        let base_url = base_url.trim_end_matches('/').to_owned();
        let parsed = reqwest::Url::parse(&base_url)
            .map_err(|e| Error::Config(format!("the base URL `{base_url}` is not valid: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(Error::Config(format!(
                "the base URL `{base_url}` must start with http:// or https://"
            )));
        }
        let webhook_secret = self
            .webhook_secret
            .filter(|secret| !secret.is_empty())
            .or_else(|| env("TUNGO_WEBHOOK_SECRET"));
        let mut default_headers = HeaderMap::new();
        for (name, value) in &self.default_headers {
            let (name, value) =
                parse_header(name, value).map_err(|e| Error::Config(e.to_string()))?;
            default_headers.insert(name, value);
        }
        if let Some(error) = self.http_client_error {
            return Err(Error::Config(format!(
                "cannot create the HTTP client: {error}"
            )));
        }
        let http = match self.http_client {
            Some(client) => client,
            None => reqwest::Client::builder()
                .connect_timeout(self.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT))
                // The API key travels in a custom header, which is not removed
                // when a redirect leads to another host: a 3xx is an API error.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| Error::Config(format!("cannot create the HTTP client: {e}")))?,
        };
        Ok(Tungo {
            inner: Arc::new(Inner {
                http,
                api_key,
                base_url,
                webhook_secret,
                company_id: self.company_id.filter(|id| !id.is_empty()),
                workspace_id: self.workspace_id.filter(|id| !id.is_empty()),
                timeout: self.timeout,
                max_retries: self.max_retries.unwrap_or(retry::DEFAULT_MAX_RETRIES),
                default_headers,
                stream_idle_timeout: self
                    .stream_idle_timeout
                    .unwrap_or(DEFAULT_STREAM_IDLE_TIMEOUT),
            }),
            options: RequestOptions::default(),
            key_used: Arc::new(AtomicBool::new(false)),
        })
    }
}

/// A request with everything resolved, ready to be sent (and re-sent).
struct Prepared {
    method: reqwest::Method,
    url: reqwest::Url,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
    timeout: Duration,
    idle_timeout: Duration,
    max_retries: u32,
    has_key: bool,
}

impl Tungo {
    /// Starts configuring a client.
    pub fn builder() -> TungoBuilder {
        TungoBuilder::default()
    }

    /// Creates a client with the given API key and default settings.
    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::builder().api_key(api_key).build()
    }

    /// Creates a client configured from `TUNGO_API_KEY`, `TUNGO_BASE_URL`
    /// and `TUNGO_WEBHOOK_SECRET`.
    pub fn from_env() -> Result<Self> {
        Self::builder().build()
    }

    /// Returns a client that applies `options` to every call made through
    /// it. The original client is not changed; both share one connection pool.
    ///
    /// ```no_run
    /// # use tungo_sdk::{RequestOptions, Tungo};
    /// # async fn run(client: Tungo) -> tungo_sdk::Result<()> {
    /// let options = RequestOptions::new().idempotency_key("order-1532");
    /// client.with_options(options).proposals().approve("P-000042", None).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// An idempotency key in `options` is good for **one request**: the
    /// returned client (and its clones) sends it with the first operation
    /// that accepts an `Idempotency-Key`, and a second such operation fails
    /// with [`Error::Validation`] instead of reusing the key. Operations that
    /// do not accept the header, such as every `GET`, never carry it. Call
    /// `with_options` again for the next request, also to repeat the same
    /// one with the same key.
    #[must_use]
    pub fn with_options(&self, options: RequestOptions) -> Tungo {
        let key_used = if options.idempotency_key.is_some() {
            Arc::new(AtomicBool::new(false))
        } else {
            Arc::clone(&self.key_used)
        };
        Tungo {
            inner: Arc::clone(&self.inner),
            options: self.options.merged(&options),
            key_used,
        }
    }

    /// The base URL the client talks to.
    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    /// The default company id.
    pub fn company_id(&self) -> Option<&str> {
        self.inner.company_id.as_deref()
    }

    /// The default workspace id.
    pub fn workspace_id(&self) -> Option<&str> {
        self.inner.workspace_id.as_deref()
    }

    pub(crate) fn webhook_secret(&self) -> Option<&str> {
        self.inner.webhook_secret.as_deref()
    }

    /// Sends a request and decodes the JSON answer.
    pub async fn execute<T: DeserializeOwned>(&self, request: Request) -> Result<Response<T>> {
        self.send(request).await?.json()
    }

    /// Sends a request whose answer has no content.
    pub async fn execute_empty(&self, request: Request) -> Result<Response<()>> {
        Ok(self.send(request).await?.empty())
    }

    /// Sends a request and returns the complete, undecoded response.
    pub async fn send(&self, request: Request) -> Result<RawResponse> {
        let prepared = self.prepare(request, false)?;
        self.send_buffered(&prepared).await
    }

    /// Sends a request and returns the server-sent events of the answer as
    /// a stream of `T`. There is no total timeout; the stream fails when no
    /// bytes arrive for the idle timeout.
    pub async fn execute_stream<T: DeserializeOwned>(
        &self,
        request: Request,
    ) -> Result<EventStream<T>> {
        let prepared = self.prepare(request, true)?;
        let (body, meta) = self.open_stream(&prepared).await?;
        Ok(EventStream::new(body, meta, prepared.idle_timeout))
    }

    /// Sends a list request and returns one page that can fetch the next one.
    pub async fn execute_page<P: CursorPage>(&self, request: Request) -> Result<Page<P>> {
        let response = self.execute::<P>(request.clone()).await?;
        Ok(Page::new(self.clone(), request, response))
    }

    /// Returns a lazy stream over the items of every page of a list request.
    pub fn paginate<P>(&self, request: Result<Request>) -> ItemStream<P::Item>
    where
        P: CursorPage + Send + 'static,
        P::Item: Send + 'static,
    {
        ItemStream::new::<P>(self.clone(), request)
    }

    fn prepare(&self, request: Request, stream: bool) -> Result<Prepared> {
        let inner = &self.inner;
        let options = &self.options;
        let mut path = request.path;
        for (placeholder, label, explicit, default) in [
            ("{co}", "company", &options.company_id, &inner.company_id),
            (
                "{ws}",
                "workspace",
                &options.workspace_id,
                &inner.workspace_id,
            ),
        ] {
            if !path.contains(placeholder) {
                continue;
            }
            let id = explicit.as_deref().or(default.as_deref()).ok_or_else(|| {
                Error::Config(format!(
                    "this call needs a {label} id: set it on the client builder or in the request options"
                ))
            })?;
            path = path.replace(placeholder, &encode_path_segment(label, id)?);
        }
        if path.contains('{') || path.contains('}') {
            return Err(Error::Validation(format!(
                "the path `{path}` has an unfilled placeholder"
            )));
        }
        let mut url = format!("{}{}", inner.base_url, path);
        if !request.query.is_empty() {
            url.push('?');
            url.push_str(&encode_query(&request.query));
        }

        let url = reqwest::Url::parse(&url)
            .map_err(|e| Error::Validation(format!("the request URL is not valid: {e}")))?;

        let mut headers = inner.default_headers.clone();
        for (name, value) in &options.headers {
            headers.insert(name.clone(), value.clone());
        }
        // An idempotency key belongs to one request: it is never taken from
        // headers that many calls share (use `RequestOptions::idempotency_key`).
        headers.remove(IDEMPOTENCY_KEY);
        for (name, value) in &request.headers {
            headers.insert(name.clone(), value.clone());
        }
        headers.insert(USER_AGENT, HeaderValue::from_static(SDK_USER_AGENT));
        let accept = if stream {
            "text/event-stream"
        } else {
            "application/json"
        };
        headers.insert(ACCEPT, HeaderValue::from_static(accept));
        if let Some(content_type) = &request.content_type {
            let value = HeaderValue::from_str(content_type)
                .map_err(|_| Error::Validation("the content type is not valid".to_owned()))?;
            headers.insert(CONTENT_TYPE, value);
        }
        match request.auth {
            Auth::ApiKey => {
                let key = inner.api_key.as_deref().ok_or_else(|| {
                    Error::Config(
                        "no API key: pass one to the client builder or set TUNGO_API_KEY"
                            .to_owned(),
                    )
                })?;
                let mut value = HeaderValue::from_str(key).map_err(|_| {
                    Error::Config("the API key contains invalid characters".to_owned())
                })?;
                value.set_sensitive(true);
                headers.insert(API_KEY, value);
            }
            Auth::None => {
                headers.remove(API_KEY);
                headers.remove(reqwest::header::AUTHORIZATION);
            }
        }
        // The key goes only to an operation that accepts it; anywhere else it
        // would be ignored by the API and make a retry look safe when it is not.
        let explicit = headers.contains_key(IDEMPOTENCY_KEY);
        let own_key = options
            .idempotency_key
            .as_ref()
            .filter(|_| request.idempotent && !explicit);
        let key = match own_key {
            Some(key) => Some(key.clone()),
            None if request.idempotent && !explicit => Some(
                request
                    .default_key
                    .clone()
                    .filter(|key| !key.is_empty())
                    .or_else(|| {
                        body_idempotency_key(
                            request.body.as_deref(),
                            request.content_type.as_deref(),
                        )
                    })
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            ),
            None => None,
        };
        if let Some(key) = &key {
            let value = HeaderValue::from_str(key).map_err(|_| {
                Error::Validation("the idempotency key contains invalid characters".to_owned())
            })?;
            headers.insert(IDEMPOTENCY_KEY, value);
        }
        // Nothing below can fail, so a key is used up only by a request that is sent.
        if own_key.is_some() && self.key_used.swap(true, Ordering::SeqCst) {
            return Err(Error::Validation(
                "the idempotency key of these request options was already sent with another \
                 request; call `with_options` again for every request that carries a key"
                    .to_owned(),
            ));
        }

        let default_timeout = if request.long_running {
            DEFAULT_LONG_TIMEOUT
        } else {
            DEFAULT_TIMEOUT
        };
        Ok(Prepared {
            has_key: explicit || key.is_some(),
            method: request.method,
            url,
            headers,
            body: request.body,
            timeout: options.timeout.or(inner.timeout).unwrap_or(default_timeout),
            idle_timeout: options
                .stream_idle_timeout
                .unwrap_or(inner.stream_idle_timeout),
            max_retries: options.max_retries.unwrap_or(inner.max_retries),
        })
    }

    fn start(&self, prepared: &Prepared) -> reqwest::RequestBuilder {
        let mut builder = self
            .inner
            .http
            .request(prepared.method.clone(), prepared.url.clone())
            .headers(prepared.headers.clone());
        if let Some(body) = &prepared.body {
            builder = builder.body(body.clone());
        }
        builder
    }

    /// Sends the request with retries and reads the whole body. The timeout
    /// covers one attempt from sending the request to the last body byte.
    async fn send_buffered(&self, prepared: &Prepared) -> Result<RawResponse> {
        let mut attempt = 0u32;
        loop {
            let exchange = async {
                let response = self.start(prepared).send().await?;
                if let Some(error) = foreign_origin(prepared, &response) {
                    return Ok(Err(error));
                }
                let status = response.status().as_u16();
                let headers = response.headers().clone();
                let body = response.bytes().await?;
                Ok::<_, reqwest::Error>(Ok((status, headers, body)))
            };
            let what = format!("the request did not complete within {:?}", prepared.timeout);
            let outcome = match deadline(prepared.timeout, &what, exchange).await {
                Ok(Ok(answer)) => Ok(answer),
                // Never retried: the request must not be repeated through this client.
                Ok(Err(redirected)) => return Err(redirected),
                Err(error) => Err(error),
            };
            let failure = match outcome {
                Ok((status, headers, body)) if (200..300).contains(&status) => {
                    let meta = ResponseMeta::new(status, headers);
                    return Ok(RawResponse::new(meta, body.to_vec()));
                }
                Ok((status, headers, body)) => Failure::Status(Box::new(
                    ApiError::from_response_at(status, &headers, &body, SystemTime::now()),
                )),
                Err(error) => Failure::Transport(error),
            };
            match self.next_delay(prepared, attempt, &failure) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Err(failure.into_error()),
            }
            attempt += 1;
        }
    }

    /// Opens a streaming response. Retries happen only here, before the
    /// first byte of the stream is handed to the caller.
    async fn open_stream(&self, prepared: &Prepared) -> Result<(StreamBody, ResponseMeta)> {
        let idle = prepared.idle_timeout;
        let what = format!("the stream did not start within {idle:?}");
        let mut attempt = 0u32;
        loop {
            let opening = deadline(idle, &what, self.start(prepared).send()).await;
            if let Ok(response) = &opening {
                if let Some(error) = foreign_origin(prepared, response) {
                    return Err(error);
                }
            }
            let failure = match opening {
                Ok(mut response) if response.status().is_success() => {
                    let status = response.status().as_u16();
                    let mut headers = response.headers().clone();
                    if is_replayed_json(&headers) {
                        // A chat turn repeated with the same Idempotency-Key after it
                        // finished: the server answers the stored ChatTurnResult instead
                        // of a stream. The caller gets the events of that turn.
                        let body = deadline(idle, &what, response.bytes()).await?;
                        let request_id = headers
                            .get("x-request-id")
                            .and_then(|value| value.to_str().ok())
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned);
                        return match replay_as_event_stream(
                            &body,
                            request_id.as_deref(),
                            &mut headers,
                        ) {
                            Some(events) => Ok((
                                StreamBody::Replayed(events),
                                ResponseMeta::new(status, headers),
                            )),
                            None => Err(Error::Decode {
                                message: "the replayed response is not a chat turn result"
                                    .to_owned(),
                                status: Some(status),
                                request_id,
                                body: snippet(&body),
                                source: None,
                            }),
                        };
                    }
                    let meta = ResponseMeta::new(status, headers);
                    let content_type = meta.header("content-type").unwrap_or("").to_owned();
                    if !is_event_stream(&content_type) {
                        // A success that is not a stream (a JSON answer, a proxy
                        // page) must not look like a stream without events.
                        let start = match deadline(idle, &what, response.chunk()).await {
                            Ok(Some(chunk)) => snippet(&chunk),
                            _ => String::new(),
                        };
                        return Err(Error::Decode {
                            message: format!(
                                "expected a text/event-stream response, got content type `{content_type}`"
                            ),
                            status: Some(meta.status()),
                            request_id: meta.request_id().map(str::to_owned),
                            body: start,
                            source: None,
                        });
                    }
                    return Ok((StreamBody::Live(response), meta));
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    let headers = response.headers().clone();
                    match deadline(idle, &what, response.bytes()).await {
                        Ok(body) => Failure::Status(Box::new(ApiError::from_response_at(
                            status,
                            &headers,
                            &body,
                            SystemTime::now(),
                        ))),
                        Err(error) => Failure::Transport(error),
                    }
                }
                Err(error) => Failure::Transport(error),
            };
            match self.next_delay(prepared, attempt, &failure) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Err(failure.into_error()),
            }
            attempt += 1;
        }
    }

    /// The delay before the next attempt, or `None` when the failure is final.
    fn next_delay(&self, prepared: &Prepared, attempt: u32, failure: &Failure) -> Option<Duration> {
        if attempt >= prepared.max_retries {
            return None;
        }
        let (cause, retry_after) = match failure {
            Failure::Status(error) => (FailureCause::Status(error.status), error.retry_after),
            Failure::Transport(Error::Timeout { .. }) => (FailureCause::Timeout, None),
            Failure::Transport(_) => (FailureCause::Connection, None),
        };
        if !retry::should_retry(&prepared.method, prepared.has_key, cause) {
            return None;
        }
        match retry::retry_delay(retry_after, attempt, retry::random_jitter()) {
            RetryDecision::Sleep(delay) => Some(delay),
            RetryDecision::GiveUp => None,
        }
    }
}

enum Failure {
    Status(Box<ApiError>),
    Transport(Error),
}

impl Failure {
    fn into_error(self) -> Error {
        match self {
            Failure::Status(error) => Error::Api(error),
            Failure::Transport(error) => error,
        }
    }
}

/// Whether a `Content-Type` value names `text/event-stream`.
fn is_event_stream(content_type: &str) -> bool {
    let media_type = content_type.split(';').next().unwrap_or("");
    media_type.trim().eq_ignore_ascii_case("text/event-stream")
}

/// Whether the answer to a stream call is a stored result replayed by
/// Idempotency-Key: `application/json` with `Idempotent-Replayed: true`.
fn is_replayed_json(headers: &HeaderMap) -> bool {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
    };
    let media_type = header("content-type").split(';').next().unwrap_or("");
    media_type.trim().eq_ignore_ascii_case("application/json")
        && header("idempotent-replayed")
            .trim()
            .eq_ignore_ascii_case("true")
}

/// Turns a replayed `ChatTurnResult` into the event stream of the turn it
/// stands for: text (the answer), proposal, question, citation, error and
/// choice events, then done. The dialog id and the turn number, which a
/// replay carries only in the body, become the `X-Dialog-Id` and `X-Turn-No`
/// headers when they are missing. `None` when the body is not a chat turn
/// result.
fn replay_as_event_stream(
    body: &[u8],
    request_id: Option<&str>,
    headers: &mut HeaderMap,
) -> Option<Vec<u8>> {
    use serde_json::{Map, Value};

    fn push(out: &mut Vec<u8>, kind: &str, mut event: Map<String, Value>) {
        event.insert("type".to_owned(), Value::String(kind.to_owned()));
        out.extend_from_slice(b"event: ");
        out.extend_from_slice(kind.as_bytes());
        out.extend_from_slice(b"\ndata: ");
        out.extend_from_slice(Value::Object(event).to_string().as_bytes());
        out.extend_from_slice(b"\n\n");
    }

    let Ok(Value::Object(result)) = serde_json::from_slice::<Value>(body) else {
        return None;
    };
    let dialog_id = result.get("dialog_id")?.as_str()?;
    let missing =
        |headers: &HeaderMap, name: &str| headers.get(name).is_none_or(HeaderValue::is_empty);
    if missing(headers, "x-dialog-id") {
        if let Ok(value) = HeaderValue::from_str(dialog_id) {
            headers.insert(HeaderName::from_static("x-dialog-id"), value);
        }
    }
    if let Some(turn_no) = result.get("turn_no").and_then(Value::as_u64) {
        if missing(headers, "x-turn-no") {
            headers.insert(
                HeaderName::from_static("x-turn-no"),
                HeaderValue::from(turn_no),
            );
        }
    }
    let entries = |key: &str| -> Vec<Map<String, Value>> {
        result
            .get(key)
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_object).cloned().collect())
            .unwrap_or_default()
    };

    let mut out = Vec::new();
    let answer = result.get("answer").and_then(Value::as_str).unwrap_or("");
    if !answer.is_empty() {
        let mut text = Map::new();
        text.insert("delta".to_owned(), Value::String(answer.to_owned()));
        push(&mut out, "text", text);
    }
    for mut proposal in entries("proposals") {
        let id = proposal.remove("id").unwrap_or(Value::Null);
        proposal.insert("proposal_id".to_owned(), id);
        push(&mut out, "proposal", proposal);
    }
    for question in entries("questions") {
        push(&mut out, "question", question);
    }
    for citation in entries("citations") {
        push(&mut out, "citation", citation);
    }
    for error in entries("errors") {
        let mut event = Map::new();
        event.insert("error".to_owned(), Value::Object(error));
        event.insert(
            "request_id".to_owned(),
            request_id.map_or(Value::Null, |id| Value::String(id.to_owned())),
        );
        push(&mut out, "error", event);
    }
    let mut done = Map::new();
    done.insert("answer".to_owned(), Value::String(answer.to_owned()));
    if let Some(reason) = result
        .get("stop_reason")
        .filter(|reason| reason.is_string())
    {
        done.insert("stop_reason".to_owned(), reason.clone());
    }
    if let Some(usage) = result.get("usage").and_then(Value::as_object) {
        let mut tokens = usage.clone();
        if let Some(cost) = tokens.remove("cost_tiyn") {
            done.insert("cost_tiyn".to_owned(), cost);
        }
        done.insert("usage".to_owned(), Value::Object(tokens));
    }
    if let Some(pending) = result.get("pending_question").and_then(Value::as_object) {
        push(&mut out, "choice", pending.clone());
        if let Some(choice_id) = pending.get("choice_id") {
            done.insert("choice_id".to_owned(), choice_id.clone());
        }
    }
    push(&mut out, "done", done);
    Some(out)
}

/// The error for a response that came from another origin than the request
/// was sent to: a caller-supplied HTTP client followed a redirect, and the
/// `x-api-key` header went with it. The response is not handed out.
fn foreign_origin(prepared: &Prepared, response: &reqwest::Response) -> Option<Error> {
    let landed = response.url();
    if landed.origin() == prepared.url.origin() {
        return None;
    }
    // Only the origin is named: a path may hold a secret (a hook token).
    Some(Error::Config(format!(
        "the HTTP client followed a redirect from {} to another origin ({}); the response was \
         discarded. The request headers, including the API key, were sent there: revoke the key, \
         and build the HTTP client with `redirect(Policy::none())` or pass a \
         `reqwest::ClientBuilder` to `TungoBuilder::http_client_builder`",
        prepared.url.origin().ascii_serialization(),
        landed.origin().ascii_serialization(),
    )))
}

/// Runs a transport future under a deadline and converts its errors.
async fn deadline<T>(
    limit: Duration,
    what: &str,
    future: impl Future<Output = std::result::Result<T, reqwest::Error>>,
) -> Result<T> {
    match tokio::time::timeout(limit, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Error::from_reqwest(error)),
        Err(_) => Err(Error::timeout(what, None)),
    }
}

/// The `idempotency_key` field of a JSON object body (approving a proposal
/// documents it as the same as the header), trimmed like the API trims it.
/// The API prefers the header over the field, so a generated header would make
/// the caller's own key useless. A key that cannot travel in a header (outside
/// printable ASCII) is ignored here.
fn body_idempotency_key(body: Option<&[u8]>, content_type: Option<&str>) -> Option<String> {
    if content_type != Some("application/json") {
        return None;
    }
    let body = body?;
    if !body.windows(15).any(|w| w == b"idempotency_key") {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let key = value.as_object()?.get("idempotency_key")?.as_str()?.trim();
    (!key.is_empty() && key.bytes().all(|b| (0x20..=0x7e).contains(&b))).then(|| key.to_owned())
}
