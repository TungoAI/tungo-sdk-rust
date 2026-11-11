//! Response metadata: status, headers, request id and rate-limit state.

use std::ops::{Deref, DerefMut};

use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// The rate-limit state reported by one response (`x-ratelimit-*` headers).
/// Each field is `None` when its header is absent or not a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RateLimit {
    /// `x-ratelimit-limit`: requests allowed in the window.
    pub limit: Option<u64>,
    /// `x-ratelimit-remaining`: requests left in the window.
    pub remaining: Option<u64>,
    /// `x-ratelimit-reset`: seconds until the window resets.
    pub reset: Option<u64>,
}

/// The status and headers of an HTTP response.
#[derive(Debug, Clone)]
pub struct ResponseMeta {
    status: u16,
    headers: HeaderMap,
}

impl ResponseMeta {
    /// Describes a response by its status and headers.
    pub fn new(status: u16, headers: HeaderMap) -> Self {
        ResponseMeta { status, headers }
    }

    /// The HTTP status code.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// All response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// One response header as text (the name is case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    /// The `x-request-id` of the response.
    pub fn request_id(&self) -> Option<&str> {
        self.header("x-request-id").filter(|id| !id.is_empty())
    }

    /// The rate-limit state of this response; `None` when the response has
    /// none of the `x-ratelimit-*` headers.
    pub fn rate_limit(&self) -> Option<RateLimit> {
        let number = |name: &str| self.header(name).and_then(|v| v.trim().parse::<u64>().ok());
        let info = RateLimit {
            limit: number("x-ratelimit-limit"),
            remaining: number("x-ratelimit-remaining"),
            reset: number("x-ratelimit-reset"),
        };
        if info.limit.is_none() && info.remaining.is_none() && info.reset.is_none() {
            return None;
        }
        Some(info)
    }
}

/// A decoded response together with its status and headers.
///
/// It dereferences to the decoded value, so fields are read directly; use
/// [`Response::into_inner`] to take the value out.
#[derive(Debug, Clone)]
pub struct Response<T> {
    data: T,
    meta: ResponseMeta,
}

impl<T> Response<T> {
    pub(crate) fn new(data: T, meta: ResponseMeta) -> Self {
        Response { data, meta }
    }

    /// The decoded value.
    pub fn into_inner(self) -> T {
        self.data
    }

    /// The status and headers of the response.
    pub fn meta(&self) -> &ResponseMeta {
        &self.meta
    }

    /// The HTTP status code.
    pub fn status(&self) -> u16 {
        self.meta.status()
    }

    /// All response headers.
    pub fn headers(&self) -> &HeaderMap {
        self.meta.headers()
    }

    /// The `x-request-id` of the response.
    pub fn request_id(&self) -> Option<&str> {
        self.meta.request_id()
    }

    /// The rate-limit state of this response.
    pub fn rate_limit(&self) -> Option<RateLimit> {
        self.meta.rate_limit()
    }

    /// Converts the decoded value, keeping the metadata.
    pub fn map<U>(self, convert: impl FnOnce(T) -> U) -> Response<U> {
        Response {
            data: convert(self.data),
            meta: self.meta,
        }
    }

    pub(crate) fn into_parts(self) -> (T, ResponseMeta) {
        (self.data, self.meta)
    }
}

impl<T> Deref for Response<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> DerefMut for Response<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

/// A complete response whose body has not been decoded yet.
#[derive(Debug, Clone)]
pub struct RawResponse {
    meta: ResponseMeta,
    body: Vec<u8>,
}

impl RawResponse {
    pub(crate) fn new(meta: ResponseMeta, body: Vec<u8>) -> Self {
        RawResponse { meta, body }
    }

    /// The status and headers of the response.
    pub fn meta(&self) -> &ResponseMeta {
        &self.meta
    }

    /// The HTTP status code.
    pub fn status(&self) -> u16 {
        self.meta.status()
    }

    /// The raw body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Decodes the body as JSON. A body that is empty, truncated or not JSON
    /// is a [`Error::Decode`], never a success with empty data.
    pub fn json<T: DeserializeOwned>(self) -> Result<Response<T>> {
        match serde_json::from_slice::<T>(&self.body) {
            Ok(data) => Ok(Response::new(data, self.meta)),
            Err(error) => Err(Error::Decode {
                message: format!("the body is not the expected JSON: {error}"),
                status: Some(self.meta.status()),
                request_id: self.meta.request_id().map(str::to_owned),
                body: snippet(&self.body),
                source: Some(Box::new(error)),
            }),
        }
    }

    /// Decodes the body as JSON and wraps it with `wrap` (used for
    /// operations whose answer depends on the status code).
    pub fn json_as<T: DeserializeOwned, U>(self, wrap: impl FnOnce(T) -> U) -> Result<Response<U>> {
        self.json::<T>().map(|response| response.map(wrap))
    }

    /// Discards the body (for operations that answer without content).
    pub fn empty(self) -> Response<()> {
        Response::new((), self.meta)
    }
}

/// The first characters of a body, for error messages.
pub(crate) fn snippet(body: &[u8]) -> String {
    const LIMIT: usize = 500;
    let text = String::from_utf8_lossy(body);
    if text.chars().count() <= LIMIT {
        return text.into_owned();
    }
    let mut cut: String = text.chars().take(LIMIT).collect();
    cut.push('…');
    cut
}
