//! The official Rust library for the Tungo Platform API.
//!
//! The crate is generated from the API's OpenAPI contract: the resource
//! methods in [`resources`] and the models in [`types`] always match the
//! contract the release was built from.
//!
//! # Quick start
//!
//! ```no_run
//! use tungo_sdk::Tungo;
//!
//! #[tokio::main]
//! async fn main() -> tungo_sdk::Result<()> {
//!     // Reads TUNGO_API_KEY when no key is given.
//!     let client = Tungo::builder().company_id("co_...").build()?;
//!     let turn = client.ask("What is the VAT for October?").await?;
//!     println!("{}", turn.answer);
//!     Ok(())
//! }
//! ```
//!
//! # What is where
//!
//! | Need | Item |
//! | --- | --- |
//! | Create and configure a client | [`Tungo`], [`TungoBuilder`] |
//! | Options of one request | [`RequestOptions`], [`Tungo::with_options`] |
//! | Call the API | the resource accessors on [`Tungo`], e.g. [`Tungo::chat`] |
//! | Status, headers, request id, rate limit | [`Response`], [`Page`], [`EventStream::response`] |
//! | Errors | [`Error`], [`ApiError`], [`ApiErrorKind`] |
//! | Pagination | [`Page`], [`ItemStream`] |
//! | Streaming | [`EventStream`], [`ChatAccumulator`] |
//! | Webhooks and hooks | [`webhooks`], [`Tungo::verify_webhook`], [`Tungo::send_hook`] |
//! | Money | [`money`] |
//!
//! # Runtime
//!
//! The client is asynchronous and needs a Tokio runtime with the time
//! driver enabled (`#[tokio::main]` does that). An operation is cancelled by
//! dropping its future.
//!
//! # Cargo features
//!
//! - `rustls-tls` (default): TLS through rustls.
//! - `native-tls`: TLS through the platform's native library.
//! - `rust_decimal`: conversions between `rust_decimal::Decimal` and tiyn.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]

mod client;
mod error;
mod helpers;
mod meta;
pub mod money;
mod pagination;
pub mod request;
pub mod resources;
mod response;
pub mod retry;
pub mod serde_util;
pub mod sse;
pub mod types;
pub mod webhooks;

pub use client::{
    Tungo, TungoBuilder, DEFAULT_CONNECT_TIMEOUT, DEFAULT_LONG_TIMEOUT,
    DEFAULT_STREAM_IDLE_TIMEOUT, DEFAULT_TIMEOUT,
};
pub use error::{ApiError, ApiErrorKind, BoxError, Error, Result};
pub use helpers::{ChatAccumulator, ChatTurn, HookPayload, WaitOptions};
pub use meta::{CONTRACT_SHA256, CONTRACT_VERSION, DEFAULT_BASE_URL, VERSION};
pub use money::{MoneyError, TungoMoney};
pub use pagination::{CursorPage, ItemStream, Page};
pub use request::{Request, RequestOptions};
pub use response::{RateLimit, RawResponse, Response, ResponseMeta};
pub use sse::{EventStream, RawEventStream, SseEvent};
pub use webhooks::{WebhookVerificationError, WebhookVerifier};

/// The HTTP client the SDK is built on, re-exported so that a custom client
/// passed to [`TungoBuilder::http_client`] has the matching version.
pub use reqwest;

pub use futures_util::{StreamExt, TryStreamExt};
pub use reqwest::header::HeaderMap;
pub use reqwest::Method;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
