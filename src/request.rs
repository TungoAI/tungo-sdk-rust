//! Request description and per-request options.

use std::time::Duration;

use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;
use serde::Serialize;

use crate::error::{Error, Result};

/// Everything except the RFC 3986 unreserved characters is percent-encoded.
const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encodes a value for use as one path segment.
///
/// An empty value, `.` and `..` are rejected: they would address a different
/// resource than the caller meant.
pub fn encode_path_segment(name: &str, value: &str) -> Result<String> {
    if value.is_empty() {
        return Err(Error::Validation(format!(
            "path parameter `{name}` must not be empty"
        )));
    }
    if value == "." || value == ".." {
        return Err(Error::Validation(format!(
            "path parameter `{name}` must not be `.` or `..`"
        )));
    }
    Ok(utf8_percent_encode(value, ENCODE_SET).to_string())
}

/// Encodes query pairs as `name=value&name=value` (without the leading `?`).
pub fn encode_query(pairs: &[(String, String)]) -> String {
    let mut out = String::new();
    for (name, value) in pairs {
        if !out.is_empty() {
            out.push('&');
        }
        out.extend(utf8_percent_encode(name, ENCODE_SET));
        out.push('=');
        out.extend(utf8_percent_encode(value, ENCODE_SET));
    }
    out
}

/// A value that can be written as a query parameter.
///
/// `None` writes nothing, a `Vec` repeats the key, booleans are `true` /
/// `false`, and strings (including RFC 3339 dates) are sent as they are.
pub trait QueryValue {
    /// Appends the `name=value` pairs of this value to `out`.
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>);
}

impl QueryValue for str {
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
        out.push((name.to_owned(), self.to_owned()));
    }
}

impl QueryValue for String {
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
        out.push((name.to_owned(), self.clone()));
    }
}

macro_rules! display_query_value {
    ($($ty:ty),*) => {
        $(impl QueryValue for $ty {
            fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
                out.push((name.to_owned(), self.to_string()));
            }
        })*
    };
}

display_query_value!(bool, i32, i64, u32, u64, f64);

impl<T: QueryValue + ?Sized> QueryValue for &T {
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
        (**self).append_to(name, out);
    }
}

impl<T: QueryValue> QueryValue for Option<T> {
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
        if let Some(value) = self {
            value.append_to(name, out);
        }
    }
}

impl<T: QueryValue> QueryValue for Vec<T> {
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
        for value in self {
            value.append_to(name, out);
        }
    }
}

impl<T: QueryValue> QueryValue for [T] {
    fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
        for value in self {
            value.append_to(name, out);
        }
    }
}

/// How a request authenticates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Auth {
    /// The API key is sent in the `x-api-key` header.
    ApiKey,
    /// No credentials are sent (public endpoints and incoming hooks).
    None,
}

/// One API request: method, path, query, headers and body.
///
/// The resource methods build requests for you. Use it directly with
/// [`Tungo::execute`](crate::Tungo::execute) to call an endpoint the SDK has
/// no method for.
///
/// ```
/// use tungo_sdk::request::Request;
/// use tungo_sdk::Method;
///
/// # fn main() -> tungo_sdk::Result<()> {
/// let mut request = Request::new(Method::GET, "/v1/companies/{co}/chat/{id}");
/// request.path_param("id", "dialog 1")?;
/// request.query("limit", &10);
/// assert_eq!(request.path(), "/v1/companies/{co}/chat/dialog%201");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Request {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) content_type: Option<String>,
    pub(crate) auth: Auth,
    pub(crate) idempotent: bool,
    /// The `Idempotency-Key` used when the caller gives none (the event id of
    /// a hook event): the API reads `Idempotency-Key` before `X-Event-Id`, so
    /// a random key would override the event id.
    pub(crate) default_key: Option<String>,
    pub(crate) long_running: bool,
}

/// Header names with their values hidden: headers carry credentials
/// (`Authorization`, signatures), so `Debug` never prints a value.
pub(crate) struct RedactedHeaders<'a>(pub(crate) &'a HeaderMap);

impl std::fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|name| (name.as_str(), "[redacted]")))
            .finish()
    }
}

/// `Debug` shows the header names without their values and the size of the
/// body instead of its content.
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("headers", &RedactedHeaders(&self.headers))
            .field("body_bytes", &self.body.as_ref().map(Vec::len))
            .field("content_type", &self.content_type)
            .field("auth", &self.auth)
            .field("idempotent", &self.idempotent)
            .field("long_running", &self.long_running)
            .finish()
    }
}

impl Request {
    /// Starts a request. `path` is a template such as
    /// `/v1/companies/{co}/chat/{id}`; fill the placeholders with
    /// [`Request::path_param`]. `{co}` and `{ws}` may be left in place: they
    /// are filled from the client's company and workspace ids.
    pub fn new(method: Method, path: &str) -> Self {
        Request {
            method,
            path: path.to_owned(),
            query: Vec::new(),
            headers: HeaderMap::new(),
            body: None,
            content_type: None,
            auth: Auth::ApiKey,
            idempotent: false,
            default_key: None,
            long_running: false,
        }
    }

    /// The HTTP method.
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// The path with the placeholders filled so far.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The query pairs added so far.
    pub fn query_pairs(&self) -> &[(String, String)] {
        &self.query
    }

    /// Replaces the placeholder `{name}` with the percent-encoded `value`.
    pub fn path_param(&mut self, name: &str, value: &str) -> Result<&mut Self> {
        let placeholder = format!("{{{name}}}");
        if !self.path.contains(&placeholder) {
            return Err(Error::Validation(format!(
                "the path has no placeholder `{placeholder}`"
            )));
        }
        let encoded = encode_path_segment(name, value)?;
        self.path = self.path.replace(&placeholder, &encoded);
        Ok(self)
    }

    /// Adds a query parameter; `None` adds nothing.
    pub fn query<V: QueryValue + ?Sized>(&mut self, name: &str, value: &V) -> &mut Self {
        value.append_to(name, &mut self.query);
        self
    }

    /// Replaces a query parameter (used for the pagination cursor).
    pub(crate) fn set_query(&mut self, name: &str, value: &str) {
        self.query.retain(|(key, _)| key != name);
        self.query.push((name.to_owned(), value.to_owned()));
    }

    /// Adds a request header.
    pub fn header(&mut self, name: &str, value: &str) -> Result<&mut Self> {
        let (name, value) = parse_header(name, value)?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Sets a JSON body.
    pub fn json<T: Serialize + ?Sized>(&mut self, body: &T) -> Result<&mut Self> {
        let bytes = serde_json::to_vec(body)
            .map_err(|e| Error::Validation(format!("cannot serialize the request body: {e}")))?;
        self.body = Some(bytes);
        self.content_type = Some("application/json".to_owned());
        Ok(self)
    }

    /// Sets a JSON body and forces its `stream` property, which selects
    /// between the JSON and the event-stream answer of a chat turn.
    pub(crate) fn json_with_stream<T: Serialize + ?Sized>(
        &mut self,
        body: &T,
        stream: bool,
    ) -> Result<&mut Self> {
        let mut value = serde_json::to_value(body)
            .map_err(|e| Error::Validation(format!("cannot serialize the request body: {e}")))?;
        match value.as_object_mut() {
            Some(object) => {
                object.insert("stream".to_owned(), serde_json::Value::Bool(stream));
            }
            None => {
                return Err(Error::Validation(
                    "the request body must be a JSON object".to_owned(),
                ))
            }
        }
        self.json(&value)
    }

    /// Sets a body of arbitrary bytes with the given content type.
    pub fn bytes(&mut self, body: impl Into<Vec<u8>>, content_type: &str) -> &mut Self {
        self.body = Some(body.into());
        self.content_type = Some(content_type.to_owned());
        self
    }

    /// Chooses how the request authenticates (the default is the API key).
    pub fn auth(&mut self, auth: Auth) -> &mut Self {
        self.auth = auth;
        self
    }

    /// Marks an operation that accepts the `Idempotency-Key` header: the SDK
    /// generates a key when the caller gave none, and may retry the request.
    pub fn idempotent(&mut self) -> &mut Self {
        self.idempotent = true;
        self
    }

    /// Marks a long-running operation (a non-streaming chat turn): its
    /// default timeout is 600 s instead of 60 s.
    pub fn long_running(&mut self) -> &mut Self {
        self.long_running = true;
        self
    }
}

pub(crate) fn parse_header(name: &str, value: &str) -> Result<(HeaderName, HeaderValue)> {
    let header_name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| Error::Validation(format!("`{name}` is not a valid header name")))?;
    let header_value = HeaderValue::from_str(value)
        .map_err(|_| Error::Validation(format!("the value of header `{name}` is not valid")))?;
    Ok((header_name, header_value))
}

/// Options of a single request. They override the client's settings.
///
/// Apply them with [`Tungo::with_options`](crate::Tungo::with_options):
///
/// ```no_run
/// use std::time::Duration;
/// use tungo_sdk::{RequestOptions, Tungo};
///
/// # async fn run() -> tungo_sdk::Result<()> {
/// let client = Tungo::from_env()?;
/// let options = RequestOptions::new()
///     .timeout(Duration::from_secs(10))
///     .max_retries(0);
/// let company = client.with_options(options).companies().get().await?;
/// # let _ = company;
/// # Ok(())
/// # }
/// ```
///
/// `Debug` prints the names of the extra headers, never their values.
///
/// # Idempotency keys
///
/// An [`idempotency_key`](RequestOptions::idempotency_key) belongs to one
/// request. The client returned by `with_options` therefore sends it with
/// the first operation that accepts an `Idempotency-Key` and refuses, with
/// [`Error::Validation`], to send a second such operation: two different
/// requests under one key would make the API answer the second with the
/// result of the first. Operations that do not accept the header (every
/// `GET`, for example) never carry the key and do not use it up. To repeat
/// the same request with the same key, call `with_options` again.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct RequestOptions {
    /// The total time allowed for one attempt, including reading the body.
    pub timeout: Option<Duration>,
    /// How many times a failed request is retried.
    pub max_retries: Option<u32>,
    /// Extra headers; they win over the client's default headers.
    pub headers: HeaderMap,
    /// The `Idempotency-Key` to send instead of a generated one, with one
    /// request only (see "Idempotency keys" above).
    pub idempotency_key: Option<String>,
    /// The company the call addresses, instead of the client's company id.
    pub company_id: Option<String>,
    /// The workspace the call addresses, instead of the client's workspace id.
    pub workspace_id: Option<String>,
    /// How long a stream may stay silent before it fails with a timeout.
    pub stream_idle_timeout: Option<Duration>,
}

impl std::fmt::Debug for RequestOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestOptions")
            .field("timeout", &self.timeout)
            .field("max_retries", &self.max_retries)
            .field("headers", &RedactedHeaders(&self.headers))
            .field("idempotency_key", &self.idempotency_key)
            .field("company_id", &self.company_id)
            .field("workspace_id", &self.workspace_id)
            .field("stream_idle_timeout", &self.stream_idle_timeout)
            .finish()
    }
}

impl RequestOptions {
    /// Options that change nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the total time allowed for one attempt.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets how many times a failed request is retried (0 disables retries).
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Adds a header to the request.
    pub fn header(mut self, name: &str, value: &str) -> Result<Self> {
        let (name, value) = parse_header(name, value)?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Sets the `Idempotency-Key` of one request: the first operation that
    /// accepts the header and is sent through the client these options are
    /// applied to. A second one fails instead of reusing the key.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Addresses another company than the client's default one.
    pub fn company_id(mut self, company_id: impl Into<String>) -> Self {
        self.company_id = Some(company_id.into());
        self
    }

    /// Addresses another workspace than the client's default one.
    pub fn workspace_id(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }

    /// Sets how long a stream may stay silent before it fails.
    pub fn stream_idle_timeout(mut self, timeout: Duration) -> Self {
        self.stream_idle_timeout = Some(timeout);
        self
    }

    /// Layers `other` on top of these options: what `other` sets wins.
    pub(crate) fn merged(&self, other: &RequestOptions) -> RequestOptions {
        let mut headers = self.headers.clone();
        for (name, value) in &other.headers {
            headers.insert(name.clone(), value.clone());
        }
        RequestOptions {
            timeout: other.timeout.or(self.timeout),
            max_retries: other.max_retries.or(self.max_retries),
            headers,
            idempotency_key: other
                .idempotency_key
                .clone()
                .or_else(|| self.idempotency_key.clone()),
            company_id: other.company_id.clone().or_else(|| self.company_id.clone()),
            workspace_id: other
                .workspace_id
                .clone()
                .or_else(|| self.workspace_id.clone()),
            stream_idle_timeout: other.stream_idle_timeout.or(self.stream_idle_timeout),
        }
    }
}
