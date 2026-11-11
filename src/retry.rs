//! The retry policy: which failures are retried, and how long to wait.

use std::time::{Duration, SystemTime};

use reqwest::header::HeaderMap;
use reqwest::Method;

/// The number of retries when none is configured (3 attempts in total).
pub const DEFAULT_MAX_RETRIES: u32 = 2;

/// The first backoff delay.
pub const BACKOFF_BASE: Duration = Duration::from_millis(500);

/// The longest backoff delay.
pub const BACKOFF_MAX: Duration = Duration::from_secs(8);

/// The lower bound of the random factor applied to a backoff delay (the upper bound is 1).
pub const JITTER_MIN: f64 = 0.75;

/// The longest server-requested delay the SDK sleeps for. When the server
/// asks for more, the error is returned at once with `retry_after` filled in.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Whether an HTTP status is retried: 408, 429, 500, 502, 503 and 504.
pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

/// Whether a request may be repeated: `GET`, `HEAD`, `PUT` and `DELETE`
/// always, anything else only when it carries an `Idempotency-Key`.
pub fn is_repeatable(method: &Method, has_idempotency_key: bool) -> bool {
    has_idempotency_key
        || matches!(
            *method,
            Method::GET | Method::HEAD | Method::PUT | Method::DELETE
        )
}

/// Why an attempt failed, for [`should_retry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureCause {
    /// The server answered with this HTTP status.
    Status(u16),
    /// The request could not be sent or the connection broke.
    Connection,
    /// The attempt ran out of time.
    Timeout,
}

/// Whether a failed attempt is retried: the request must be repeatable (see
/// [`is_repeatable`]) and the failure must be a connection error, a timeout
/// or a retryable status (see [`is_retryable_status`]).
pub fn should_retry(method: &Method, has_idempotency_key: bool, cause: FailureCause) -> bool {
    let transient = match cause {
        FailureCause::Status(status) => is_retryable_status(status),
        FailureCause::Connection | FailureCause::Timeout => true,
    };
    transient && is_repeatable(method, has_idempotency_key)
}

/// The backoff delay before retry number `attempt` (0-based):
/// `min(0.5 s * 2^attempt, 8 s) * jitter`. `jitter` is clamped to `[0.75, 1]`.
pub fn backoff_delay(attempt: u32, jitter: f64) -> Duration {
    let exponent = attempt.min(16);
    let raw = BACKOFF_BASE
        .saturating_mul(1u32 << exponent)
        .min(BACKOFF_MAX);
    raw.mul_f64(jitter.clamp(JITTER_MIN, 1.0))
}

/// A random factor in `[0.75, 1]`.
pub(crate) fn random_jitter() -> f64 {
    let bits = (uuid::Uuid::new_v4().as_u128() >> 75) as f64;
    let unit = bits / ((1u64 << 53) as f64);
    JITTER_MIN + unit * (1.0 - JITTER_MIN)
}

/// Parses the delay the server asked for: `retry-after-ms` (milliseconds)
/// wins over `Retry-After` (seconds, possibly fractional, or an HTTP-date,
/// measured from `now`). Returns `None` when there is no usable value.
///
/// A value that asks for no wait at all (zero, or a date that has passed) is
/// not usable either: the caller then waits its computed backoff instead of
/// retrying at once against a server that is in trouble.
pub fn parse_retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let text = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let positive = |delay: Duration| (!delay.is_zero()).then_some(delay);
    if let Some(delay) = text("retry-after-ms")
        .and_then(parse_non_negative)
        .map(|ms| ms / 1000.0)
        .and_then(to_duration)
        .and_then(positive)
    {
        return Some(delay);
    }
    let value = text("retry-after")?;
    if let Some(seconds) = parse_non_negative(value) {
        return to_duration(seconds).and_then(positive);
    }
    let at = httpdate::parse_http_date(value.trim()).ok()?;
    at.duration_since(now).ok().and_then(positive)
}

/// Accepts only `digits[.digits]`.
fn parse_non_negative(value: &str) -> Option<f64> {
    let (whole, fraction) = match value.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (value, None),
    };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(whole) || !fraction.is_none_or(digits) {
        return None;
    }
    value.parse::<f64>().ok()
}

fn to_duration(seconds: f64) -> Option<Duration> {
    Duration::try_from_secs_f64(seconds).ok()
}

/// What to do after a retryable failure.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetryDecision {
    /// Wait this long, then try again.
    Sleep(Duration),
    /// The server asked for a delay above [`MAX_RETRY_AFTER`]: give up.
    GiveUp,
}

/// Chooses the delay before retry number `attempt`: the server's value when
/// it sent one, otherwise the jittered backoff.
pub fn retry_delay(retry_after: Option<Duration>, attempt: u32, jitter: f64) -> RetryDecision {
    match retry_after {
        Some(delay) if delay > MAX_RETRY_AFTER => RetryDecision::GiveUp,
        Some(delay) => RetryDecision::Sleep(delay),
        None => RetryDecision::Sleep(backoff_delay(attempt, jitter)),
    }
}
