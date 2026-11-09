//! Официальный Rust SDK для платформы **Tungo AI** (`https://api.tungo.kz`).
//!
//! Не требует внешних крейтов (`0 dependencies`), поддерживает как высокоуровневые методы
//! в одну строчку (`ask`, `send_hook`, `verify_webhook`, `is_valid_bin_iin`, `to_tiyn`, `format_tiyn`),
//! так и все 13 ресурсов и 82 метода OpenAPI-контракта Tungo Platform API.

pub mod client;
pub mod crypto;
pub mod json;
pub mod resources;
pub mod types;

pub use client::{
    accumulate_chat_stream, is_valid_bin_iin, parse_api_error, parse_sse, AskInput, AskOutput,
    ChatStreamAccumulated, RateLimitInfo, RequestSpec, SSEEvent, TungoClient, TungoClientBuilder,
    TungoError, TungoErrorKind, TungoMoney, WaitOptions, WebhookEnvelope,
};
pub use crypto::{sign_hook_payload, verify_hook_signature, verify_webhook};
pub use json::JsonValue;
