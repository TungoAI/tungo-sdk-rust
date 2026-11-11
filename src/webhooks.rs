//! Webhook verification (Standard Webhooks / Svix) and hook signatures.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::header::HeaderMap;
use sha2::Sha256;

use crate::types::WebhookEvent;

type HmacSha256 = Hmac<Sha256>;

/// The default tolerance for the timestamp of a webhook: 300 s in both directions.
pub const DEFAULT_TOLERANCE: Duration = Duration::from_secs(300);

/// Why a webhook was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebhookVerificationError {
    /// The tolerance is zero. A tolerance cannot be used to switch the check off.
    #[error("webhook verification failed: the timestamp tolerance must be positive")]
    InvalidTolerance,
    /// No secret was given and the client has none.
    #[error("webhook verification failed: no webhook secret is configured")]
    MissingSecret,
    /// The secret is empty or is not `whsec_` followed by base64.
    #[error("webhook verification failed: the webhook secret is empty or malformed")]
    InvalidSecret,
    /// A required header is absent.
    #[error("webhook verification failed: the `{0}` header is missing")]
    MissingHeader(&'static str),
    /// The timestamp header is not a whole number of seconds.
    #[error("webhook verification failed: the timestamp header is malformed")]
    MalformedTimestamp,
    /// The message is older than the tolerance allows.
    #[error("webhook verification failed: the message timestamp is too old")]
    TimestampTooOld,
    /// The message is dated further in the future than the tolerance allows.
    #[error("webhook verification failed: the message timestamp is too far in the future")]
    TimestampTooNew,
    /// No signature in the header matches the body.
    #[error("webhook verification failed: no signature matches the body")]
    SignatureMismatch,
    /// The signature is valid but the body is not a JSON event.
    #[error("webhook verification failed: the body is not a JSON event: {0}")]
    InvalidPayload(String),
}

impl WebhookVerificationError {
    /// A stable machine-readable code of the reason.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidTolerance => "invalid_tolerance",
            Self::MissingSecret | Self::InvalidSecret => "invalid_secret",
            Self::MissingHeader(_) => "missing_header",
            Self::MalformedTimestamp => "malformed_timestamp",
            Self::TimestampTooOld => "timestamp_too_old",
            Self::TimestampTooNew => "timestamp_too_new",
            Self::SignatureMismatch => "signature_mismatch",
            Self::InvalidPayload(_) => "invalid_payload",
        }
    }
}

/// A container of HTTP headers the verifier can read.
///
/// It is implemented for `http::HeaderMap` (used by reqwest, axum, hyper and
/// actix-web's `http` types), for string maps and for slices of pairs.
/// Lookups are case-insensitive.
pub trait WebhookHeaders {
    /// The value of the header `name` (given in lower case), if present.
    fn header(&self, name: &str) -> Option<&str>;
}

impl WebhookHeaders for HeaderMap {
    fn header(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|value| value.to_str().ok())
    }
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for [(K, V)] {
    fn header(&self, name: &str) -> Option<&str> {
        self.iter()
            .find(|(key, _)| key.as_ref().eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_ref())
    }
}

impl<K: AsRef<str>, V: AsRef<str>, const N: usize> WebhookHeaders for [(K, V); N] {
    fn header(&self, name: &str) -> Option<&str> {
        self.as_slice().header(name)
    }
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for Vec<(K, V)> {
    fn header(&self, name: &str) -> Option<&str> {
        self.as_slice().header(name)
    }
}

impl<K: AsRef<str>, V: AsRef<str>, S> WebhookHeaders for HashMap<K, V, S> {
    fn header(&self, name: &str) -> Option<&str> {
        self.iter()
            .find(|(key, _)| key.as_ref().eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_ref())
    }
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for BTreeMap<K, V> {
    fn header(&self, name: &str) -> Option<&str> {
        self.iter()
            .find(|(key, _)| key.as_ref().eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_ref())
    }
}

impl<T: WebhookHeaders + ?Sized> WebhookHeaders for &T {
    fn header(&self, name: &str) -> Option<&str> {
        (**self).header(name)
    }
}

/// Verifies webhooks signed with one secret.
///
/// ```
/// use tungo_sdk::webhooks::WebhookVerifier;
///
/// let verifier = WebhookVerifier::new("whsec_MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")?;
/// let headers = [
///     ("webhook-id", "msg_1"),
///     ("webhook-timestamp", "1793974568"),
///     ("webhook-signature", "v1,AAAA"),
/// ];
/// assert!(verifier.verify(b"{}", &headers).is_err());
/// # Ok::<(), tungo_sdk::webhooks::WebhookVerificationError>(())
/// ```
#[derive(Clone)]
pub struct WebhookVerifier {
    key: Vec<u8>,
    tolerance: Duration,
    now: Option<SystemTime>,
}

impl std::fmt::Debug for WebhookVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookVerifier")
            .field("key", &"[redacted]")
            .field("tolerance", &self.tolerance)
            .finish_non_exhaustive()
    }
}

impl WebhookVerifier {
    /// Creates a verifier for a secret of the form `whsec_<base64>`.
    pub fn new(secret: &str) -> Result<Self, WebhookVerificationError> {
        let encoded = secret.strip_prefix("whsec_").unwrap_or(secret);
        if encoded.is_empty() {
            return Err(WebhookVerificationError::InvalidSecret);
        }
        let key = STANDARD
            .decode(encoded)
            .map_err(|_| WebhookVerificationError::InvalidSecret)?;
        if key.is_empty() {
            return Err(WebhookVerificationError::InvalidSecret);
        }
        Ok(WebhookVerifier {
            key,
            tolerance: DEFAULT_TOLERANCE,
            now: None,
        })
    }

    /// Sets how far the message timestamp may be from the clock, in either
    /// direction (300 s by default). A zero tolerance makes every
    /// verification fail with `InvalidTolerance`.
    #[must_use]
    pub fn tolerance(mut self, tolerance: Duration) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Replaces the system clock with a fixed time (for tests).
    #[must_use]
    pub fn now(mut self, now: SystemTime) -> Self {
        self.now = Some(now);
        self
    }

    /// Checks the signature and the timestamp of a webhook and returns the
    /// decoded event. `body` must be the raw request body, byte for byte.
    pub fn verify<H: WebhookHeaders + ?Sized>(
        &self,
        body: impl AsRef<[u8]>,
        headers: &H,
    ) -> Result<WebhookEvent, WebhookVerificationError> {
        let body = body.as_ref();
        self.check(body, headers)?;
        serde_json::from_slice(body)
            .map_err(|e| WebhookVerificationError::InvalidPayload(e.to_string()))
    }

    /// Checks the signature and the timestamp without decoding the body.
    pub fn check<H: WebhookHeaders + ?Sized>(
        &self,
        body: &[u8],
        headers: &H,
    ) -> Result<(), WebhookVerificationError> {
        if self.tolerance.is_zero() {
            return Err(WebhookVerificationError::InvalidTolerance);
        }
        let get = |svix: &str, standard: &'static str| {
            headers
                .header(svix)
                .or_else(|| headers.header(standard))
                .ok_or(WebhookVerificationError::MissingHeader(standard))
        };
        let id = get("svix-id", "webhook-id")?;
        let timestamp = get("svix-timestamp", "webhook-timestamp")?;
        let signatures = get("svix-signature", "webhook-signature")?;

        if timestamp.is_empty() || !timestamp.bytes().all(|b| b.is_ascii_digit()) {
            return Err(WebhookVerificationError::MalformedTimestamp);
        }
        let sent: u64 = timestamp
            .parse()
            .map_err(|_| WebhookVerificationError::MalformedTimestamp)?;
        let now = self
            .now
            .unwrap_or_else(SystemTime::now)
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        let tolerance = self.tolerance.as_secs().max(1);
        if now > sent && now - sent > tolerance {
            return Err(WebhookVerificationError::TimestampTooOld);
        }
        if sent > now && sent - now > tolerance {
            return Err(WebhookVerificationError::TimestampTooNew);
        }

        for candidate in signatures.split_ascii_whitespace() {
            let Some(("v1", encoded)) = candidate.split_once(',') else {
                continue;
            };
            let Ok(signature) = STANDARD.decode(encoded) else {
                continue;
            };
            let mut mac = HmacSha256::new_from_slice(&self.key)
                .map_err(|_| WebhookVerificationError::InvalidSecret)?;
            mac.update(id.as_bytes());
            mac.update(b".");
            mac.update(timestamp.as_bytes());
            mac.update(b".");
            mac.update(body);
            // `verify_slice` compares in constant time.
            if mac.verify_slice(&signature).is_ok() {
                return Ok(());
            }
        }
        Err(WebhookVerificationError::SignatureMismatch)
    }
}

/// Verifies a webhook with the given secret, the default tolerance of 300 s
/// and the system clock, and returns the decoded event.
pub fn verify_webhook<H: WebhookHeaders + ?Sized>(
    body: impl AsRef<[u8]>,
    headers: &H,
    secret: &str,
) -> Result<WebhookEvent, WebhookVerificationError> {
    WebhookVerifier::new(secret)?.verify(body, headers)
}

/// The secret of an incoming hook was empty.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the hook secret must not be empty")]
pub struct EmptySecretError;

/// Computes the signature of an incoming-hook body: the lower-case hex
/// HMAC-SHA256 of the exact bytes, keyed with the hook secret.
pub fn sign_hook_payload(body: &[u8], secret: &str) -> Result<String, EmptySecretError> {
    if secret.is_empty() {
        return Err(EmptySecretError);
    }
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| EmptySecretError)?;
    mac.update(body);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

/// The value of the `X-Signature` header for an incoming-hook body: `sha256=<hex>`.
pub fn hook_signature_header(body: &[u8], secret: &str) -> Result<String, EmptySecretError> {
    sign_hook_payload(body, secret).map(|digest| format!("sha256={digest}"))
}

/// Checks the signature of an incoming-hook body in constant time.
///
/// `signature` is the hex digest in either case, with or without the
/// `sha256=` prefix (which is accepted in either case too). An empty secret
/// never verifies.
pub fn verify_hook_signature(body: &[u8], signature: &str, secret: &str) -> bool {
    if secret.is_empty() {
        return false;
    }
    // The prefix is matched without regard to case, like the hex digest itself.
    let digest = match signature.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case("sha256=") => &signature[7..],
        _ => signature,
    };
    let Ok(expected) = hex::decode(digest) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}
