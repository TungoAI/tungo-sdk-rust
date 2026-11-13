//! Error types returned by the SDK.

use std::fmt;
use std::time::Duration;

use reqwest::header::HeaderMap;
use serde_json::{Map, Value};

use crate::money::MoneyError;
use crate::types::FieldError;
use crate::webhooks::WebhookVerificationError;

/// A boxed error used as the source of an SDK error.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The result type of every fallible SDK operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every error the SDK can return.
///
/// Cancellation has no variant: an operation is cancelled by dropping its
/// future (for example with `tokio::select!` or `tokio::time::timeout`).
///
/// The `Debug` output never shows the body kept by [`Error::Decode`]: a
/// response that fails to decode may still carry a one-time webhook secret or
/// a hook token, and `{:?}` ends up in logs (`unwrap`, `expect`, `main`
/// returning the error). The body stays readable in the `body` field.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The API answered with an HTTP error status.
    #[error(transparent)]
    Api(Box<ApiError>),

    /// An `error` event arrived inside an event stream, or a chat turn
    /// finished with an error. The fields describe the event; `status` is the
    /// status of the response that carried it.
    #[error("the stream reported an error: {0}")]
    Stream(Box<ApiError>),

    /// The request could not be sent or the connection broke.
    #[error("connection error: {message}")]
    Connection {
        /// What went wrong.
        message: String,
        /// The underlying transport error.
        #[source]
        source: Option<BoxError>,
    },

    /// The request, an idle stream or a wait helper ran out of time.
    #[error("timed out: {message}")]
    Timeout {
        /// What timed out and after how long.
        message: String,
        /// The underlying transport error, when there is one.
        #[source]
        source: Option<BoxError>,
    },

    /// A successful response could not be decoded.
    #[error("cannot decode the response: {message}")]
    Decode {
        /// What could not be decoded.
        message: String,
        /// The HTTP status of the response.
        status: Option<u16>,
        /// The request id of the response.
        request_id: Option<String>,
        /// The beginning of the response body.
        body: String,
        /// The underlying decoding error.
        #[source]
        source: Option<BoxError>,
    },

    /// The client is not configured for the call (for example, no API key).
    /// Raised before any request is made, with one exception: a
    /// caller-supplied HTTP client that followed a redirect to another
    /// origin is detected on the response (see
    /// [`TungoBuilder::http_client`](crate::TungoBuilder::http_client)).
    #[error("configuration error: {0}")]
    Config(String),

    /// An argument is not valid (for example, an empty path identifier).
    /// Raised before any request is made.
    #[error("invalid argument: {0}")]
    Validation(String),

    /// The server returned a pagination cursor it had already returned.
    #[error("pagination error: {0}")]
    Pagination(String),

    /// A webhook could not be verified.
    #[error(transparent)]
    WebhookVerification(#[from] WebhookVerificationError),

    /// A money amount could not be converted.
    #[error(transparent)]
    Money(#[from] MoneyError),
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Api(error) => f.debug_tuple("Api").field(error).finish(),
            Error::Stream(error) => f.debug_tuple("Stream").field(error).finish(),
            Error::Connection { message, source } => f
                .debug_struct("Connection")
                .field("message", message)
                .field("source", source)
                .finish(),
            Error::Timeout { message, source } => f
                .debug_struct("Timeout")
                .field("message", message)
                .field("source", source)
                .finish(),
            Error::Decode {
                message,
                status,
                request_id,
                body,
                source,
            } => f
                .debug_struct("Decode")
                .field("message", message)
                .field("status", status)
                .field("request_id", request_id)
                .field("body", &format_args!("[{} bytes, not shown]", body.len()))
                .field("source", source)
                .finish(),
            Error::Config(message) => f.debug_tuple("Config").field(message).finish(),
            Error::Validation(message) => f.debug_tuple("Validation").field(message).finish(),
            Error::Pagination(message) => f.debug_tuple("Pagination").field(message).finish(),
            Error::WebhookVerification(error) => {
                f.debug_tuple("WebhookVerification").field(error).finish()
            }
            Error::Money(error) => f.debug_tuple("Money").field(error).finish(),
        }
    }
}

impl Error {
    /// The API error behind `Error::Api` or `Error::Stream`.
    pub fn as_api(&self) -> Option<&ApiError> {
        match self {
            Error::Api(error) | Error::Stream(error) => Some(error),
            _ => None,
        }
    }

    /// The HTTP status of the response, when the error has one.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api(error) | Error::Stream(error) => Some(error.status),
            Error::Decode { status, .. } => *status,
            _ => None,
        }
    }

    /// The request id to quote to support, when the error has one.
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Error::Api(error) | Error::Stream(error) => error.request_id.as_deref(),
            Error::Decode { request_id, .. } => request_id.as_deref(),
            _ => None,
        }
    }

    /// The classification of an API error.
    pub fn kind(&self) -> Option<ApiErrorKind> {
        self.as_api().map(|error| error.kind)
    }

    /// Whether the error is a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Error::Timeout { .. })
    }

    /// Whether the error is a connection failure.
    pub fn is_connection(&self) -> bool {
        matches!(self, Error::Connection { .. })
    }

    pub(crate) fn connection(message: impl Into<String>, source: Option<BoxError>) -> Self {
        Error::Connection {
            message: message.into(),
            source,
        }
    }

    pub(crate) fn timeout(message: impl Into<String>, source: Option<BoxError>) -> Self {
        Error::Timeout {
            message: message.into(),
            source,
        }
    }

    pub(crate) fn from_reqwest(error: reqwest::Error) -> Self {
        // `without_url` keeps query strings out of error messages.
        let error = error.without_url();
        if error.is_timeout() {
            Error::timeout("the request timed out", Some(Box::new(error)))
        } else {
            Error::connection(describe(&error), Some(Box::new(error)))
        }
    }
}

/// Renders an error with its chain of sources on one line.
fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(inner) = source {
        let part = inner.to_string();
        if !text.contains(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        source = inner.source();
    }
    text
}

/// The classification of an API error: the `error.type` of the response body
/// when it is a known value, otherwise derived from the HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ApiErrorKind {
    /// `invalid_request_error`: the request is malformed or fails validation.
    InvalidRequest,
    /// `authentication_error`: the API key is missing, wrong or revoked.
    Authentication,
    /// `payment_required_error`: the balance or the plan does not allow the call.
    PaymentRequired,
    /// `permission_error`: the key lacks a permission.
    Permission,
    /// `not_found_error`: the object does not exist.
    NotFound,
    /// `conflict_error`: the object is in a state that forbids the action.
    Conflict,
    /// `gone_error`: the object existed but is no longer available.
    Gone,
    /// `limit_error`: a rate limit or a plan limit is exhausted.
    Limit,
    /// `api_error`: an internal error of the API.
    Api,
    /// `unavailable_error`: the API or a dependency is temporarily unavailable.
    Unavailable,
}

impl ApiErrorKind {
    /// The `error.type` value of this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            ApiErrorKind::InvalidRequest => "invalid_request_error",
            ApiErrorKind::Authentication => "authentication_error",
            ApiErrorKind::PaymentRequired => "payment_required_error",
            ApiErrorKind::Permission => "permission_error",
            ApiErrorKind::NotFound => "not_found_error",
            ApiErrorKind::Conflict => "conflict_error",
            ApiErrorKind::Gone => "gone_error",
            ApiErrorKind::Limit => "limit_error",
            ApiErrorKind::Api => "api_error",
            ApiErrorKind::Unavailable => "unavailable_error",
        }
    }

    /// Parses a known `error.type` value.
    pub fn from_type(value: &str) -> Option<Self> {
        Some(match value {
            "invalid_request_error" => ApiErrorKind::InvalidRequest,
            "authentication_error" => ApiErrorKind::Authentication,
            "payment_required_error" => ApiErrorKind::PaymentRequired,
            "permission_error" => ApiErrorKind::Permission,
            "not_found_error" => ApiErrorKind::NotFound,
            "conflict_error" => ApiErrorKind::Conflict,
            "gone_error" => ApiErrorKind::Gone,
            "limit_error" => ApiErrorKind::Limit,
            "api_error" => ApiErrorKind::Api,
            "unavailable_error" => ApiErrorKind::Unavailable,
            _ => return None,
        })
    }

    /// The kind used when the response body is not the API error envelope.
    pub fn from_status(status: u16) -> Self {
        match status {
            401 => ApiErrorKind::Authentication,
            402 => ApiErrorKind::PaymentRequired,
            403 => ApiErrorKind::Permission,
            404 => ApiErrorKind::NotFound,
            409 => ApiErrorKind::Conflict,
            410 => ApiErrorKind::Gone,
            429 => ApiErrorKind::Limit,
            502..=504 => ApiErrorKind::Unavailable,
            500..=599 => ApiErrorKind::Api,
            _ => ApiErrorKind::InvalidRequest,
        }
    }
}

impl fmt::Display for ApiErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An error reported by the API: an HTTP error response, or an `error` event
/// of a stream.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ApiError {
    /// The classification to branch on.
    pub kind: ApiErrorKind,
    /// The HTTP status of the response.
    pub status: u16,
    /// `error.type` exactly as the API sent it (it may be a value newer than
    /// this SDK), or the status-derived type when the body is not the envelope.
    pub error_type: String,
    /// `error.code`: the stable machine-readable reason.
    pub code: Option<String>,
    /// `error.message`: the human-readable explanation.
    pub message: String,
    /// The request id from the body, else from the `x-request-id` header.
    pub request_id: Option<String>,
    /// Per-field validation errors (`error.fields`).
    pub fields: Vec<FieldError>,
    /// The delay the server asked for (`retry-after-ms` or `Retry-After`).
    pub retry_after: Option<Duration>,
    /// Every other property of `error` (for example `resets_at`, `permission`).
    pub extra: Map<String, Value>,
    /// Whether the body was the API error envelope.
    pub envelope: bool,
    /// The response headers.
    pub headers: HeaderMap,
    /// The raw response body.
    pub body: String,
}

impl ApiError {
    /// Builds the error for an HTTP error response. Never fails: a body that
    /// is not the API error envelope (HTML, empty, truncated JSON) yields an
    /// error classified by the status.
    pub fn from_response(status: u16, headers: &HeaderMap, body: &[u8]) -> Self {
        Self::from_response_at(status, headers, body, std::time::SystemTime::now())
    }

    /// Like [`ApiError::from_response`], with an explicit clock for the
    /// HTTP-date form of `Retry-After`.
    pub fn from_response_at(
        status: u16,
        headers: &HeaderMap,
        body: &[u8],
        now: std::time::SystemTime,
    ) -> Self {
        let fallback = ApiErrorKind::from_status(status);
        let text = String::from_utf8_lossy(body).into_owned();
        let mut error = ApiError {
            kind: fallback,
            status,
            error_type: fallback.as_str().to_owned(),
            code: None,
            message: default_message(status, &text),
            request_id: header_text(headers, "x-request-id"),
            fields: Vec::new(),
            retry_after: crate::retry::parse_retry_after(headers, now),
            extra: Map::new(),
            envelope: false,
            headers: headers.clone(),
            body: text,
        };
        if let Ok(Value::Object(mut root)) = serde_json::from_slice::<Value>(body) {
            let request_id = root
                .get("request_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned);
            if let Some(Value::Object(detail)) = root.remove("error") {
                error.apply_detail(detail, request_id);
            }
        }
        error
    }

    /// Builds the error for an `error` event of a stream: the envelope form
    /// of chat streams (`{type: "error", error: {...}, request_id}`) or the
    /// flat form of the feed (`{error: "feed_failed", message}`).
    pub(crate) fn from_event(status: u16, headers: &HeaderMap, event: &Value) -> Self {
        let mut error = ApiError {
            kind: ApiErrorKind::Api,
            status,
            error_type: ApiErrorKind::Api.as_str().to_owned(),
            code: None,
            message: "the stream reported an error".to_owned(),
            request_id: header_text(headers, "x-request-id"),
            fields: Vec::new(),
            retry_after: None,
            extra: Map::new(),
            envelope: false,
            headers: headers.clone(),
            body: event.to_string(),
        };
        let request_id = event
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        match event.get("error") {
            Some(Value::Object(detail)) => error.apply_detail(detail.clone(), request_id),
            Some(Value::String(code)) => {
                error.code = Some(code.clone());
                if let Some(message) = event.get("message").and_then(Value::as_str) {
                    error.message = message.to_owned();
                }
                if request_id.is_some() {
                    error.request_id = request_id;
                }
                if let Value::Object(props) = event {
                    for (key, value) in props {
                        if !matches!(key.as_str(), "error" | "message" | "request_id") {
                            error.extra.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
            _ => {}
        }
        error
    }

    fn apply_detail(&mut self, detail: Map<String, Value>, request_id: Option<String>) {
        let (Some(kind), Some(message)) = (
            detail.get("type").and_then(Value::as_str),
            detail.get("message").and_then(Value::as_str),
        ) else {
            return;
        };
        self.envelope = true;
        self.error_type = kind.to_owned();
        if let Some(known) = ApiErrorKind::from_type(kind) {
            self.kind = known;
        }
        self.message = message.to_owned();
        if request_id.is_some() {
            self.request_id = request_id;
        }
        for (key, value) in detail {
            match key.as_str() {
                "type" | "message" => {}
                "code" => self.code = value.as_str().map(str::to_owned),
                "fields" => {
                    if let Value::Array(items) = value {
                        self.fields = items
                            .into_iter()
                            .filter_map(|item| serde_json::from_value(item).ok())
                            .collect();
                    }
                }
                _ => {
                    self.extra.insert(key, value);
                }
            }
        }
    }

    /// Whether the status is one the SDK retries (408, 429, 500, 502, 503, 504).
    pub fn is_retryable_status(&self) -> bool {
        crate::retry::is_retryable_status(self.status)
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (status {}, type {}",
            self.message, self.status, self.error_type
        )?;
        if let Some(code) = &self.code {
            write!(f, ", code {code}")?;
        }
        if let Some(request_id) = &self.request_id {
            write!(f, ", request id {request_id}")?;
        }
        f.write_str(")")
    }
}

impl std::error::Error for ApiError {}

impl From<ApiError> for Error {
    fn from(error: ApiError) -> Self {
        Error::Api(Box::new(error))
    }
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn default_message(status: u16, body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return format!("the API returned HTTP {status} with an empty body");
    }
    if trimmed.starts_with('<') {
        return format!("the API returned HTTP {status} with a non-JSON body");
    }
    let mut snippet: String = trimmed.chars().take(200).collect();
    if snippet.len() < trimmed.len() {
        snippet.push('…');
    }
    format!("the API returned HTTP {status}: {snippet}")
}
