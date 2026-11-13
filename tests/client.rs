//! Requests, authentication, errors, retries and timeouts, checked against
//! the mock server over real HTTP.

mod common;

use std::error::Error as _;
use std::time::{Duration, Instant};

use common::{Mock, Reply};
use tungo_sdk::types::{
    ApproveBatchCompanyProposalsResponse, ApproveRequest, AutomationStateInput,
    BatchApproveRequest, ChatDialogPatch, ChatMessageRequest, WebhookEndpointCreate,
    WebhookEndpointPatch,
};
use tungo_sdk::{ApiErrorKind, Error, HookPayload, RequestOptions, Tungo};

const COMPANY: &str = r#"{"id":"co_1","workspace_id":"ws_1","name":"ТОО Ромашка","bin":"123456789012",
    "legal_form":"too","status":"active","role":"owner","role_name":"Владелец","permissions":["chat.use"],
    "created_at":"2026-11-09T15:00:00.000Z","future_field":{"added":"later"}}"#;
const ENDPOINT: &str = r#"{"id":"ep_1","url":"https://example.com/hook","description":"","enabled":true,
    "channels":null,"event_types":["proposal.written"],"created_at":"2026-11-09T15:00:00.000Z",
    "updated_at":"2026-11-09T15:00:00.000Z"}"#;
const STOPPED: &str = r#"{"id":"dlg_1","stopped":true}"#;
const TURN: &str = r#"{"answer":"НДС — 5 400 ₸.","citations":[],"dialog_id":"dlg_1","errors":[],
    "pending_question":null,"proposals":[],"questions":[],"stop_reason":"end_turn","turn_no":1}"#;
const STATE: &str = r#"{"paused":true,"paused_at":null,"queued":0,"reason":null,"running":0}"#;

fn message(text: &str) -> ChatMessageRequest {
    ChatMessageRequest {
        message: Some(text.to_owned()),
        ..Default::default()
    }
}

// ---------------------------------------------------------------- requests

#[tokio::test]
async fn sends_the_api_key_and_the_standard_headers() {
    let mock = Mock::always(200, COMPANY).await;
    let company = mock.client().companies().get().await.expect("company");

    assert_eq!(company.name, "ТОО Ромашка");
    assert_eq!(company.legal_form.as_str(), "too");
    assert_eq!(company.status(), 200);

    let request = mock.last();
    assert_eq!(request.method, "GET");
    assert_eq!(request.target, "/v1/companies/co_1");
    assert_eq!(request.header("x-api-key"), Some("tg_test_SECRETKEY123"));
    assert_eq!(
        request.header("authorization"),
        None,
        "the key goes only in x-api-key"
    );
    assert_eq!(request.header("accept"), Some("application/json"));
    assert_eq!(
        request.header("user-agent"),
        Some(format!("tungo-sdk-rust/{}", tungo_sdk::VERSION).as_str())
    );
    assert_eq!(
        request.header("content-type"),
        None,
        "a request without a body has no type"
    );
    assert_eq!(
        request.header("idempotency-key"),
        None,
        "GET needs no idempotency key"
    );
}

#[tokio::test]
async fn the_version_matches_the_crate_version() {
    assert_eq!(tungo_sdk::VERSION, env!("CARGO_PKG_VERSION"));
    assert_eq!(tungo_sdk::DEFAULT_BASE_URL, "https://api.tungo.kz");
    assert_eq!(tungo_sdk::CONTRACT_SHA256.len(), 64);
}

#[tokio::test]
async fn public_operations_do_not_send_the_api_key() {
    let mock = Mock::always(200, r#"{"data":[]}"#).await;
    // Even with default headers that carry credentials.
    let client = mock
        .builder()
        .default_header("authorization", "Bearer leaked")
        .build()
        .expect("client");
    client.platform().list_usage_rates().await.expect("rates");
    let request = mock.last();
    assert_eq!(request.header("x-api-key"), None);
    assert_eq!(request.header("authorization"), None);

    // A public operation works without any key at all.
    let anonymous = Tungo::builder()
        .ignore_env()
        .base_url(&mock.url)
        .build()
        .expect("client");
    anonymous
        .platform()
        .list_usage_rates()
        .await
        .expect("rates");
    assert_eq!(mock.count(), 2);
}

#[tokio::test]
async fn a_missing_api_key_fails_before_any_request() {
    let mock = Mock::always(200, COMPANY).await;
    let client = Tungo::builder()
        .ignore_env()
        .base_url(&mock.url)
        .company_id("co_1")
        .build()
        .expect("client");
    let error = client.companies().get().await.expect_err("no key");
    assert!(
        matches!(&error, Error::Config(text) if text.contains("TUNGO_API_KEY")),
        "{error}"
    );
    assert_eq!(mock.count(), 0);
}

#[tokio::test]
async fn a_missing_company_or_workspace_fails_before_any_request() {
    let mock = Mock::always(200, COMPANY).await;
    let client = Tungo::builder()
        .ignore_env()
        .base_url(&mock.url)
        .api_key("tg_test_1")
        .build()
        .expect("client");
    let error = client.companies().get().await.expect_err("no company");
    assert!(
        matches!(&error, Error::Config(text) if text.contains("company")),
        "{error}"
    );
    let error = client
        .webhooks()
        .get_usage()
        .await
        .expect_err("no workspace");
    assert!(
        matches!(&error, Error::Config(text) if text.contains("workspace")),
        "{error}"
    );
    assert_eq!(mock.count(), 0);

    // Per-call options supply them.
    let scoped = client.with_options(RequestOptions::new().company_id("co_9"));
    scoped.companies().get().await.expect("company");
    assert_eq!(mock.last().target, "/v1/companies/co_9");
}

#[tokio::test]
async fn path_parameters_are_escaped_and_validated() {
    let mock = Mock::always(200, STOPPED).await;
    let client = mock.client();

    client.chat().stop("a/b c?d#e%").await.expect("stop");
    assert_eq!(
        mock.last().target,
        "/v1/companies/co_1/chat/a%2Fb%20c%3Fd%23e%25/stop"
    );

    for bad in ["", ".", ".."] {
        let error = client.chat().stop(bad).await.expect_err("bad id");
        assert!(matches!(error, Error::Validation(_)), "{bad:?}: {error}");
    }
    assert_eq!(mock.count(), 1, "invalid ids never reach the network");

    // The company id from the configuration is escaped the same way.
    let odd = mock
        .builder()
        .company_id("co/../x")
        .build()
        .expect("client");
    odd.chat().stop("dlg_1").await.expect("stop");
    assert_eq!(
        mock.last().target,
        "/v1/companies/co%2F..%2Fx/chat/dlg_1/stop"
    );
}

#[tokio::test]
async fn query_parameters_are_encoded_and_absent_ones_omitted() {
    let mock = Mock::always(200, r#"{"data":[],"next_cursor":null}"#).await;
    let client = mock.client();

    let params = tungo_sdk::resources::ChatListParams::new()
        .cursor("a+b&c=d e/ж")
        .limit(50);
    client.chat().list(&params).await.expect("list");
    assert_eq!(
        mock.last().query(),
        "cursor=a%2Bb%26c%3Dd%20e%2F%D0%B6&limit=50"
    );

    client.chat().list(&Default::default()).await.expect("list");
    assert_eq!(mock.last().target, "/v1/companies/co_1/chat");
}

#[tokio::test]
async fn bodies_tell_null_from_absent() {
    let mock = Mock::always(200, ENDPOINT).await;
    let client = mock.client();

    // Absent: the property is not sent, the server keeps its value.
    let patch = WebhookEndpointPatch {
        enabled: Some(false),
        ..Default::default()
    };
    client
        .webhooks()
        .update_endpoint("ep_1", &patch)
        .await
        .expect("patch");
    let request = mock.last();
    assert_eq!(request.method, "PATCH");
    assert_eq!(request.text(), r#"{"enabled":false}"#);
    assert_eq!(request.header("content-type"), Some("application/json"));

    // Explicit null: the server clears the value.
    let patch = WebhookEndpointPatch {
        channels: Some(None),
        event_types: Some(Some(vec!["proposal.written".to_owned()])),
        ..Default::default()
    };
    client
        .webhooks()
        .update_endpoint("ep_1", &patch)
        .await
        .expect("patch");
    assert_eq!(
        mock.last().json(),
        serde_json::json!({"channels": null, "event_types": ["proposal.written"]})
    );

    // A required property is always sent; a required-only body has no extras.
    let rename = ChatDialogPatch::new("Квартал");
    let _ = client.chat().update("dlg_1", &rename).await;
    assert_eq!(mock.last().text(), r#"{"title":"Квартал"}"#);
}

#[tokio::test]
async fn an_optional_body_is_omitted_entirely() {
    let mock = Mock::always(200, r#"{"message":"ok","ok":true,"repeated":false}"#).await;
    let decision = mock
        .client()
        .proposals()
        .approve("P-1", None)
        .await
        .expect("approve");
    assert!(decision.ok);
    let request = mock.last();
    assert!(request.body.is_empty());
    assert_eq!(request.header("content-type"), None);
    assert_eq!(request.header("content-length").unwrap_or("0"), "0");
}

#[tokio::test]
async fn dual_operations_force_the_stream_flag() {
    let mock = Mock::always(200, TURN).await;
    let mut request = message("Какой НДС?");
    request.stream = Some(true); // a contradictory value is overridden
    let turn = mock.client().chat().create(&request).await.expect("turn");
    assert_eq!(turn.answer, "НДС — 5 400 ₸.");
    assert_eq!(
        mock.last().json(),
        serde_json::json!({"message": "Какой НДС?", "stream": false})
    );
    assert_eq!(mock.last().header("accept"), Some("application/json"));
}

#[tokio::test]
async fn a_response_without_content_is_accepted() {
    let mock = Mock::start(|_, _| Reply::Full {
        status: 204,
        headers: vec![("x-request-id".into(), "req_204".into())],
        body: Vec::new(),
    })
    .await;
    let response = mock
        .client()
        .webhooks()
        .delete_endpoint("ep_1")
        .await
        .expect("delete");
    assert_eq!(response.status(), 204);
    assert_eq!(response.request_id(), Some("req_204"));
    assert_eq!(mock.last().method, "DELETE");
}

#[tokio::test]
async fn the_response_type_follows_the_status() {
    let sync_body = r#"{"approved":1,"counts":{},"excluded":0,"failed":0,"limit":50,"message":"ok",
        "ok":true,"requested":1,"results":[]}"#;
    let job_body = r#"{"async":true,"events":0,"id":"job_1","kind":"approve_batch","message":"started",
        "requested":120,"running":true,"started_at":"2026-11-09T15:00:00.000Z","title":"Пакет"}"#;
    let mock = Mock::start(move |_, index| match index {
        0 => Reply::json(200, sync_body),
        _ => Reply::json(202, job_body),
    })
    .await;
    let client = mock.client();
    let body = BatchApproveRequest::default();

    let done = client.proposals().approve_batch(&body).await.expect("sync");
    assert!(matches!(
        &*done,
        ApproveBatchCompanyProposalsResponse::BatchApproveResponse(result) if result.ok
    ));
    let job = client
        .proposals()
        .approve_batch(&body)
        .await
        .expect("async");
    assert_eq!(job.status(), 202);
    assert!(matches!(
        &*job,
        ApproveBatchCompanyProposalsResponse::BatchApproveJob(job) if job.id == "job_1"
    ));
}

#[tokio::test]
async fn response_metadata_is_exposed() {
    let mock = Mock::start(|_, _| {
        Reply::json(200, COMPANY)
            .with_header("x-request-id", "req_meta_1")
            .with_header("x-ratelimit-limit", "60")
            .with_header("x-ratelimit-remaining", "59")
            .with_header("x-ratelimit-reset", "12")
    })
    .await;
    let company = mock.client().companies().get().await.expect("company");
    assert_eq!(company.request_id(), Some("req_meta_1"));
    let limit = company.rate_limit().expect("rate limit");
    assert_eq!(
        (limit.limit, limit.remaining, limit.reset),
        (Some(60), Some(59), Some(12))
    );
    assert_eq!(
        company
            .headers()
            .get("x-ratelimit-limit")
            .map(|v| v.as_bytes()),
        Some(&b"60"[..])
    );
    let owned = company.into_inner();
    assert_eq!(owned.id, "co_1");
}

#[tokio::test]
async fn options_and_default_headers_are_applied_per_call() {
    let mock = Mock::always(200, COMPANY).await;
    let client = mock
        .builder()
        .default_header("x-trace", "base")
        .default_header("x-tenant", "t1")
        .build()
        .expect("client");
    let scoped = client.with_options(
        RequestOptions::new()
            .header("x-trace", "call")
            .expect("header"),
    );
    scoped.companies().get().await.expect("company");
    let request = mock.last();
    assert_eq!(request.header("x-trace"), Some("call"));
    assert_eq!(request.header("x-tenant"), Some("t1"));

    // The original client is not changed by `with_options`.
    client.companies().get().await.expect("company");
    assert_eq!(mock.last().header("x-trace"), Some("base"));

    assert!(RequestOptions::new().header("bad header", "x").is_err());
    assert!(mock
        .builder()
        .default_header("bad\nname", "x")
        .build()
        .is_err());
}

#[tokio::test]
async fn configuration_is_validated() {
    for url in ["ftp://example.com", "not a url", "example.com"] {
        let error = Tungo::builder()
            .ignore_env()
            .base_url(url)
            .build()
            .expect_err("bad URL");
        assert!(matches!(error, Error::Config(_)), "{url}: {error}");
    }
    let error = Tungo::builder()
        .ignore_env()
        .api_key("line\nbreak")
        .build()
        .expect_err("bad key");
    assert!(matches!(error, Error::Config(_)));

    // A trailing slash and a path prefix in the base URL are kept correctly.
    let mock = Mock::always(200, COMPANY).await;
    let client = mock
        .builder()
        .base_url(format!("{}/proxy/", mock.url))
        .build()
        .expect("client");
    assert_eq!(client.base_url(), format!("{}/proxy", mock.url));
    client.companies().get().await.expect("company");
    assert_eq!(mock.last().target, "/proxy/v1/companies/co_1");
}

#[tokio::test]
async fn debug_output_hides_secrets() {
    let client = Tungo::builder()
        .ignore_env()
        .api_key("tg_live_TOPSECRET")
        .webhook_secret("whsec_TOPSECRET")
        .build()
        .expect("client");
    let builder = Tungo::builder()
        .api_key("tg_live_TOPSECRET")
        .webhook_secret("whsec_TOPSECRET");
    let payload = HookPayload::bytes("{}", "application/json").secret("hksec_TOPSECRET");
    let verifier = tungo_sdk::WebhookVerifier::new("whsec_VE9QU0VDUkVU").expect("verifier");
    for text in [
        format!("{client:?}"),
        format!("{client:#?}"),
        format!("{builder:?}"),
        format!("{payload:?}"),
        format!("{verifier:?}"),
    ] {
        assert!(
            !text.contains("TOPSECRET") && !text.contains("VE9QU0VDUkVU"),
            "{text}"
        );
    }
    assert!(format!("{client:?}").contains("api.tungo.kz"));
}

#[tokio::test]
async fn the_client_is_clone_send_sync_and_usable_from_tasks() {
    fn assert_traits<T: Clone + Send + Sync + 'static>() {}
    assert_traits::<Tungo>();
    fn assert_send<T: Send>(_: &T) {}

    let mock = Mock::always(200, COMPANY).await;
    let client = mock.client();
    let future = client.companies().get();
    assert_send(&future);
    drop(future);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .companies()
                .get()
                .await
                .map(|company| company.id.clone())
        }));
    }
    for task in tasks {
        assert_eq!(task.await.expect("join").expect("company"), "co_1");
    }
    assert_eq!(mock.count(), 8);
}

#[tokio::test]
async fn connections_are_reused() {
    let mock = Mock::always(200, COMPANY).await;
    let client = mock.client();
    for _ in 0..5 {
        client.companies().get().await.expect("company");
    }
    assert_eq!(mock.count(), 5);
    assert_eq!(
        mock.connections(),
        1,
        "sequential calls share one connection"
    );
}

// ------------------------------------------------------------------ errors

#[tokio::test]
async fn api_errors_are_typed() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::json(
            400,
            r#"{"type":"error","error":{"type":"invalid_request_error","code":"validation_failed",
            "message":"Проверьте поля","fields":[{"path":"url","code":"invalid_format","message":"Нужен https"}]},
            "request_id":"req_400"}"#,
        ),
        1 => Reply::error(404, "not_found_error", "not_found", "Нет такого диалога"),
        2 => Reply::json(
            403,
            r#"{"type":"error","error":{"type":"permission_error","code":"missing_permission",
            "message":"Нет права","permission":"chat.use"},"request_id":"req_403"}"#,
        ),
        _ => Reply::Full {
            status: 502,
            headers: vec![("content-type".into(), "text/html".into())],
            body: b"<html><h1>502 Bad Gateway</h1></html>".to_vec(),
        },
    })
    .await;
    let client = mock.builder().max_retries(0).build().expect("client");

    let error = client
        .webhooks()
        .create_endpoint(&WebhookEndpointCreate::new("http://x"))
        .await
        .expect_err("400");
    assert_eq!(error.status(), Some(400));
    assert_eq!(error.kind(), Some(ApiErrorKind::InvalidRequest));
    assert_eq!(error.request_id(), Some("req_400"));
    let api = error.as_api().expect("an API error");
    assert_eq!(api.code.as_deref(), Some("validation_failed"));
    assert_eq!(api.message, "Проверьте поля");
    assert_eq!(api.fields.len(), 1);
    assert_eq!(api.fields[0].path, "url");
    assert_eq!(api.fields[0].code.as_str(), "invalid_format");
    assert!(error.to_string().contains("Проверьте поля"), "{error}");
    assert!(error.to_string().contains("req_400"), "{error}");

    let error = client.chat().get("dlg_404").await.expect_err("404");
    assert!(matches!(&error, Error::Api(api) if api.kind == ApiErrorKind::NotFound));

    let error = client.companies().get().await.expect_err("403");
    let api = error.as_api().expect("an API error");
    assert_eq!(api.kind, ApiErrorKind::Permission);
    assert_eq!(
        api.extra.get("permission").and_then(|v| v.as_str()),
        Some("chat.use")
    );

    let error = client.companies().get().await.expect_err("502");
    let api = error.as_api().expect("an API error");
    assert_eq!(
        (api.status, api.kind, api.envelope),
        (502, ApiErrorKind::Unavailable, false)
    );
    assert!(api.body.contains("502 Bad Gateway"), "the raw body is kept");
    assert!(!api.message.is_empty());
}

#[tokio::test]
async fn an_undecodable_success_is_a_decode_error() {
    let mock = Mock::start(|_, index| match index {
        0 => {
            Reply::json(200, "<html>captive portal</html>").with_header("x-request-id", "req_html")
        }
        1 => Reply::json(200, r#"{"id":"co_1","name":["not","a","string"]}"#),
        2 => Reply::json(200, r#"{"id":"co_1","workspace_id":"#),
        _ => Reply::json(200, ""),
    })
    .await;
    let client = mock.client();
    for case in ["html", "wrong type", "truncated", "empty"] {
        let error = client.companies().get().await.expect_err(case);
        let Error::Decode { status, source, .. } = &error else {
            panic!("{case}: expected a decode error, got {error:?}");
        };
        assert_eq!(*status, Some(200), "{case}");
        assert!(source.is_some(), "{case}: the serde error is the source");
        assert!(error.source().is_some(), "{case}");
    }
    assert_eq!(mock.count(), 4, "decode errors are not retried");
    let first = client.companies().get().await.expect_err("empty");
    assert_eq!(first.status(), Some(200));
}

#[tokio::test]
async fn a_decode_error_keeps_the_request_id_and_a_body_snippet() {
    let mock = Mock::start(|_, _| {
        Reply::json(200, "<html>captive portal</html>").with_header("x-request-id", "req_html")
    })
    .await;
    let error = mock.client().companies().get().await.expect_err("html");
    assert_eq!(error.request_id(), Some("req_html"));
    assert!(matches!(&error, Error::Decode { body, .. } if body.contains("captive portal")));
}

#[tokio::test]
async fn a_refused_connection_is_a_connection_error_with_its_cause() {
    // Bind a port, then free it: nothing listens there.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    drop(listener);
    let client = Tungo::builder()
        .ignore_env()
        .base_url(url)
        .api_key("tg_test_1")
        .company_id("co_1")
        .max_retries(0)
        .build()
        .expect("client");
    let error = client.companies().get().await.expect_err("refused");
    assert!(error.is_connection(), "{error:?}");
    assert!(
        error.source().is_some(),
        "the transport error is the source"
    );
    assert!(!error.to_string().contains("tg_test_1"));
}

#[tokio::test]
async fn a_redirect_is_never_followed_and_the_key_stays_home() {
    let other = Mock::always(200, COMPANY).await;
    let location = format!("{}/v1/elsewhere", other.url);
    let mock =
        Mock::start(move |_, _| Reply::json(302, "").with_header("location", &location)).await;
    let client = mock.builder().max_retries(0).build().expect("client");

    let error = client.companies().get().await.expect_err("a redirect");
    assert!(
        matches!(&error, Error::Api(api) if api.status == 302),
        "{error:?}"
    );
    let Err(error) = client.chat().create_stream(&message("?")).await else {
        panic!("the stream must not open");
    };
    assert!(
        matches!(&error, Error::Api(api) if api.status == 302),
        "{error:?}"
    );
    assert_eq!(mock.count(), 2);
    assert_eq!(other.count(), 0, "the API key must not reach another host");
}

#[tokio::test]
async fn the_hook_token_never_appears_in_an_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    drop(listener);
    let client = Tungo::builder()
        .ignore_env()
        .base_url(url)
        .max_retries(0)
        .build()
        .expect("client");
    let token = "SECRET_HOOK_TOKEN_123";
    let payload = HookPayload::json(&serde_json::json!({"id": "evt_1"})).expect("payload");
    let error = client.send_hook(token, payload).await.expect_err("refused");
    assert!(error.is_connection(), "{error:?}");
    let mut text = format!("{error} {error:?}");
    let mut source = error.source();
    while let Some(inner) = source {
        text.push_str(&format!(" {inner} {inner:?}"));
        source = inner.source();
    }
    assert!(!text.contains(token), "{text}");
}

// ----------------------------------------------------------------- retries

#[tokio::test]
async fn retryable_statuses_are_retried_then_succeed() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::error(503, "unavailable_error", "busy", "busy")
            .with_header("retry-after-ms", "10"),
        1 => Reply::error(429, "limit_error", "rate_limited", "slow")
            .with_header("retry-after-ms", "10"),
        _ => Reply::json(200, COMPANY),
    })
    .await;
    let company = mock.client().companies().get().await.expect("company");
    assert_eq!(company.id, "co_1");
    assert_eq!(mock.count(), 3);
}

#[tokio::test]
async fn a_retry_after_of_zero_or_in_the_past_uses_the_backoff() {
    // "Retry at once" from a server in trouble must not become a tight loop.
    for (name, value) in [
        ("retry-after", "0"),
        ("retry-after", "0.0"),
        ("retry-after", "Mon, 09 Nov 2020 14:59:00 GMT"),
        ("retry-after-ms", "0"),
    ] {
        let mock = Mock::start(move |_, index| match index {
            0 => Reply::error(503, "unavailable_error", "busy", "busy").with_header(name, value),
            _ => Reply::json(200, COMPANY),
        })
        .await;
        let started = Instant::now();
        mock.client().companies().get().await.expect("company");
        let waited = started.elapsed();
        // The first backoff is 0.5 s with a jitter factor of 0.75..1.
        assert!(
            waited >= Duration::from_millis(370),
            "{name}: {value}: {waited:?}"
        );
        assert_eq!(mock.count(), 2);
    }
    let headers = |name: &'static str, value: &'static str| {
        let mut map = tungo_sdk::HeaderMap::new();
        map.insert(name, value.parse().expect("value"));
        map
    };
    let now = std::time::SystemTime::now();
    let parse = tungo_sdk::retry::parse_retry_after;
    assert_eq!(parse(&headers("retry-after", "0"), now), None);
    assert_eq!(parse(&headers("retry-after-ms", "0"), now), None);
    assert_eq!(parse(&headers("retry-after", "-1"), now), None);
    assert_eq!(
        parse(&headers("retry-after", "2"), now),
        Some(Duration::from_secs(2))
    );
}

#[tokio::test]
async fn the_retry_budget_is_respected() {
    for (max_retries, expected) in [(0u32, 1usize), (1, 2), (3, 4)] {
        let mock = Mock::start(|_, _| {
            Reply::error(500, "api_error", "boom", "boom").with_header("retry-after-ms", "1")
        })
        .await;
        let client = mock
            .builder()
            .max_retries(max_retries)
            .build()
            .expect("client");
        let error = client.companies().get().await.expect_err("500");
        assert_eq!(error.status(), Some(500));
        assert_eq!(mock.count(), expected, "max_retries={max_retries}");
    }
    // The default is 2 retries: 3 attempts.
    let mock = Mock::start(|_, _| {
        Reply::error(500, "api_error", "boom", "boom").with_header("retry-after-ms", "1")
    })
    .await;
    mock.client().companies().get().await.expect_err("500");
    assert_eq!(mock.count(), 3);

    // Per-call override.
    let mock = Mock::start(|_, _| {
        Reply::error(500, "api_error", "boom", "boom").with_header("retry-after-ms", "1")
    })
    .await;
    let once = mock
        .client()
        .with_options(RequestOptions::new().max_retries(0));
    once.companies().get().await.expect_err("500");
    assert_eq!(mock.count(), 1);
}

#[tokio::test]
async fn other_statuses_are_not_retried() {
    for status in [400u16, 401, 403, 404, 409, 422, 501] {
        let mock = Mock::start(move |_, _| Reply::error(status, "api_error", "x", "x")).await;
        let error = mock.client().companies().get().await.expect_err("error");
        assert_eq!(error.status(), Some(status));
        assert_eq!(mock.count(), 1, "status {status}");
    }
}

#[tokio::test]
async fn a_post_without_an_idempotency_key_is_never_repeated() {
    // `chat.stop` is a POST the contract does not mark as idempotent.
    let mock = Mock::start(|_, _| {
        Reply::error(503, "unavailable_error", "busy", "busy").with_header("retry-after-ms", "1")
    })
    .await;
    let error = mock.client().chat().stop("dlg_1").await.expect_err("503");
    assert_eq!(error.status(), Some(503));
    assert_eq!(mock.count(), 1);
    assert_eq!(mock.last().header("idempotency-key"), None);

    // A dropped connection is not repeated either.
    let mock = Mock::start(|_, _| Reply::Drop).await;
    let error = mock
        .client()
        .chat()
        .stop("dlg_1")
        .await
        .expect_err("dropped");
    assert!(error.is_connection(), "{error:?}");
    assert_eq!(mock.count(), 1);

    // A key in the request options does not change that: the operation does
    // not accept an Idempotency-Key, so the API would ignore it and a retry
    // would only look safe. The key is not sent and the call is not repeated.
    let mock = Mock::start(|_, _| Reply::Drop).await;
    let keyed = mock
        .client()
        .with_options(RequestOptions::new().idempotency_key("stop-1"));
    let error = keyed.chat().stop("dlg_1").await.expect_err("dropped");
    assert!(error.is_connection(), "{error:?}");
    assert_eq!(mock.count(), 1);
    assert_eq!(mock.last().header("idempotency-key"), None);
}

#[tokio::test]
async fn the_idempotency_key_of_an_approval_body_is_the_header() {
    // The API prefers the header over the documented body field: a generated
    // header would make a repeat of the same click a new approval.
    let mock = Mock::always(200, r#"{"message":"ok","ok":true,"repeated":false}"#).await;
    let client = mock.client();
    let body = ApproveRequest {
        idempotency_key: Some(" click-7 ".to_owned()),
        ..Default::default()
    };
    for _ in 0..2 {
        client
            .proposals()
            .approve("P-1", Some(&body))
            .await
            .expect("approve");
        assert_eq!(mock.last().header("idempotency-key"), Some("click-7"));
    }
    client
        .with_options(RequestOptions::new().idempotency_key("own"))
        .proposals()
        .approve("P-1", Some(&body))
        .await
        .expect("approve");
    assert_eq!(mock.last().header("idempotency-key"), Some("own"));
    client
        .proposals()
        .approve("P-1", Some(&ApproveRequest::default()))
        .await
        .expect("approve");
    assert_eq!(
        mock.last().header("idempotency-key").map(str::len),
        Some(36)
    );
}

#[tokio::test]
async fn an_idempotency_key_in_the_options_is_good_for_one_request() {
    let mock = Mock::start(|request, _| match request.method.as_str() {
        "GET" => Reply::json(200, COMPANY),
        _ => Reply::json(200, ENDPOINT),
    })
    .await;
    let body = WebhookEndpointCreate::new("https://example.com/hook");
    let options = RequestOptions::new().idempotency_key("endpoint-1");
    let keyed = mock.client().with_options(options.clone());

    // A GET through the same client never carries the key and does not use it up.
    keyed.companies().get().await.expect("company");
    assert_eq!(mock.last().header("idempotency-key"), None);

    // The first operation that accepts a key gets it.
    keyed
        .webhooks()
        .create_endpoint(&body)
        .await
        .expect("created");
    assert_eq!(mock.last().header("idempotency-key"), Some("endpoint-1"));

    // A second one must not go out under the same key: the API would answer
    // it with the result of the first. Nothing is sent.
    let sent = mock.count();
    for client in [
        keyed.clone(),
        keyed.with_options(RequestOptions::new().max_retries(0)),
    ] {
        let error = client
            .webhooks()
            .create_endpoint(&body)
            .await
            .expect_err("the key was used");
        assert!(matches!(error, Error::Validation(_)), "{error:?}");
        assert!(error.to_string().contains("idempotency key"), "{error}");
    }
    assert_eq!(mock.count(), sent);
    keyed.companies().get().await.expect("reads still work");

    // Applying the options again is how the same request is repeated with the same key.
    let again = mock.client().with_options(options);
    again
        .webhooks()
        .create_endpoint(&body)
        .await
        .expect("created");
    assert_eq!(mock.last().header("idempotency-key"), Some("endpoint-1"));

    // A key cannot be smuggled in as a header shared by many calls.
    let shared = mock
        .builder()
        .default_header("Idempotency-Key", "shared-1")
        .build()
        .expect("client")
        .with_options(
            RequestOptions::new()
                .header("idempotency-key", "shared-2")
                .expect("header"),
        );
    shared.companies().get().await.expect("company");
    assert_eq!(mock.last().header("idempotency-key"), None);
    shared
        .webhooks()
        .create_endpoint(&body)
        .await
        .expect("created");
    let first = mock
        .last()
        .header("idempotency-key")
        .expect("key")
        .to_owned();
    shared
        .webhooks()
        .create_endpoint(&body)
        .await
        .expect("created");
    let second = mock
        .last()
        .header("idempotency-key")
        .expect("key")
        .to_owned();
    assert!(first.len() == 36 && second.len() == 36 && first != second);
}

#[tokio::test]
async fn debug_output_of_requests_and_options_hides_header_values() {
    let options = RequestOptions::new()
        .header("Authorization", "Bearer SECRET123")
        .expect("header")
        .header("X-Trace", "billing-job")
        .expect("header")
        .max_retries(1);
    let text = format!("{options:?} {options:#?}");
    assert!(!text.contains("SECRET123"), "{text}");
    assert!(!text.contains("billing-job"), "{text}");
    assert!(text.contains("authorization"), "the names stay: {text}");
    assert!(text.contains("max_retries"), "{text}");

    let mut request = tungo_sdk::Request::new(tungo_sdk::Method::POST, "/v1/x");
    request
        .header("X-Signature", "sha256=SECRET123")
        .expect("header");
    request.bytes("card=SECRET123", "text/plain");
    let text = format!("{request:?}");
    assert!(!text.contains("SECRET123"), "{text}");
    assert!(text.contains("x-signature"), "{text}");
}

#[tokio::test]
async fn a_caller_supplied_client_that_follows_redirects_is_stopped() {
    let other = Mock::always(200, COMPANY).await;
    let location = format!("{}/v1/elsewhere", other.url);
    let mock =
        Mock::start(move |_, _| Reply::json(302, "").with_header("location", &location)).await;

    // A reqwest client with the default policy follows the redirect and takes
    // the x-api-key header along. The SDK cannot prevent that, but it must not
    // hand out what the other host answered, and it must say what happened.
    let following = tungo_sdk::reqwest::Client::builder()
        .build()
        .expect("http client");
    let client = mock
        .builder()
        .http_client(following)
        .build()
        .expect("client");
    let error = client
        .companies()
        .get()
        .await
        .expect_err("a foreign answer");
    assert!(matches!(error, Error::Config(_)), "{error:?}");
    let text = error.to_string();
    assert!(
        text.contains("redirect") && text.contains("revoke"),
        "{text}"
    );
    assert!(
        !text.contains("/v1/elsewhere"),
        "only the origin is named: {text}"
    );
    assert!(!text.contains("tg_test_SECRETKEY123"), "{text}");
    assert_eq!(mock.count(), 1, "the call is not repeated");
    let Err(error) = client.chat().create_stream(&message("?")).await else {
        panic!("the stream must not open");
    };
    assert!(matches!(error, Error::Config(_)), "{error:?}");

    // A redirect that stays on the same origin is not a leak.
    let same = Mock::start(|request, _| match request.path() {
        "/v1/companies/co_1" => Reply::json(307, "").with_header("location", "/v1/moved"),
        _ => Reply::json(200, COMPANY),
    })
    .await;
    let following = tungo_sdk::reqwest::Client::builder()
        .build()
        .expect("http client");
    let client = same
        .builder()
        .http_client(following)
        .build()
        .expect("client");
    assert_eq!(client.companies().get().await.expect("company").id, "co_1");

    // The safe way to customise the HTTP client: the SDK turns redirects off itself.
    let other = Mock::always(200, COMPANY).await;
    let location = format!("{}/v1/elsewhere", other.url);
    let mock =
        Mock::start(move |_, _| Reply::json(302, "").with_header("location", &location)).await;
    let client = mock
        .builder()
        .http_client_builder(tungo_sdk::reqwest::Client::builder())
        .max_retries(0)
        .build()
        .expect("client");
    let error = client.companies().get().await.expect_err("a redirect");
    assert!(
        matches!(&error, Error::Api(api) if api.status == 302),
        "{error:?}"
    );
    assert_eq!(other.count(), 0, "the API key must not reach another host");
}

#[test]
fn the_hook_signature_prefix_is_matched_without_regard_to_case() {
    use tungo_sdk::webhooks::{hook_signature_header, verify_hook_signature};
    let header = hook_signature_header(b"x", "s").expect("signature");
    assert!(verify_hook_signature(b"x", &header, "s"));
    assert!(verify_hook_signature(
        b"x",
        &header.replace("sha256=", "SHA256="),
        "s"
    ));
    assert!(verify_hook_signature(
        b"x",
        &header.replace("sha256=", "Sha256="),
        "s"
    ));
    assert!(verify_hook_signature(
        b"x",
        &header.replace("sha256=", ""),
        "s"
    ));
    assert!(!verify_hook_signature(
        b"x",
        &header.replace("sha256=", "sha512="),
        "s"
    ));
    assert!(!verify_hook_signature(b"y", &header, "s"));
    assert!(
        !verify_hook_signature(b"x", "é", "s"),
        "no panic on a short non-ASCII value"
    );
}

#[tokio::test]
async fn idempotent_posts_carry_one_key_across_retries() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::Drop,
        1 => Reply::error(502, "unavailable_error", "edge", "edge")
            .with_header("retry-after-ms", "1"),
        _ => Reply::json(200, ENDPOINT),
    })
    .await;
    let client = mock.client();
    let body = WebhookEndpointCreate::new("https://example.com/hook");
    client
        .webhooks()
        .create_endpoint(&body)
        .await
        .expect("created");

    let requests = mock.requests();
    assert_eq!(requests.len(), 3);
    let key = requests[0]
        .header("idempotency-key")
        .expect("a key")
        .to_owned();
    assert_eq!(key.len(), 36, "a UUID: {key}");
    assert_eq!(key.as_bytes()[14], b'4', "version 4: {key}");
    assert!(requests
        .iter()
        .all(|r| r.header("idempotency-key") == Some(key.as_str())));
    assert!(
        requests.iter().all(|r| r.body == requests[0].body),
        "the body is re-sent in full"
    );

    // A new call gets a new key.
    client
        .webhooks()
        .create_endpoint(&body)
        .await
        .expect("created");
    assert_ne!(mock.last().header("idempotency-key"), Some(key.as_str()));
}

#[tokio::test]
async fn put_and_delete_are_repeated_without_a_key() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::Drop,
        _ => Reply::json(200, STATE),
    })
    .await;
    let input = AutomationStateInput {
        paused: true,
        reason: None,
    };
    let state = mock
        .client()
        .automations()
        .put_state(&input)
        .await
        .expect("state");
    assert!(state.paused);
    assert_eq!(mock.count(), 2);
    assert_eq!(mock.last().text(), r#"{"paused":true}"#);
}

#[tokio::test]
async fn the_backoff_is_used_without_a_retry_after() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::error(500, "api_error", "boom", "boom"),
        _ => Reply::json(200, COMPANY),
    })
    .await;
    let started = Instant::now();
    mock.client().companies().get().await.expect("company");
    let waited = started.elapsed();
    // The first backoff is 0.5 s with a jitter factor of 0.75..1.
    assert!(waited >= Duration::from_millis(370), "{waited:?}");
    assert!(waited < Duration::from_millis(1500), "{waited:?}");
}

#[tokio::test]
async fn retry_after_is_honoured_and_capped() {
    // A short delay is slept.
    let mock = Mock::start(|_, index| match index {
        0 => {
            Reply::error(429, "limit_error", "rate_limited", "slow").with_header("retry-after", "1")
        }
        _ => Reply::json(200, COMPANY),
    })
    .await;
    let started = Instant::now();
    mock.client().companies().get().await.expect("company");
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(990), "{waited:?}");
    assert!(waited < Duration::from_millis(2500), "{waited:?}");

    // A delay above 60 s is not slept: the error comes back at once.
    let mock = Mock::start(|_, _| {
        Reply::error(429, "limit_error", "plan_limit_reached", "limit")
            .with_header("retry-after", "3600")
    })
    .await;
    let started = Instant::now();
    let error = mock.client().companies().get().await.expect_err("429");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(mock.count(), 1);
    let api = error.as_api().expect("an API error");
    assert_eq!(api.kind, ApiErrorKind::Limit);
    assert_eq!(api.retry_after, Some(Duration::from_secs(3600)));
}

// ---------------------------------------------------------------- timeouts

#[tokio::test]
async fn a_silent_server_times_out() {
    let mock = Mock::start(|_, _| Reply::Hang).await;
    let client = mock
        .builder()
        .timeout(Duration::from_millis(200))
        .max_retries(0)
        .build()
        .expect("client");
    let started = Instant::now();
    let error = client.companies().get().await.expect_err("timeout");
    assert!(error.is_timeout(), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(error.to_string().contains("200ms"), "{error}");
}

#[tokio::test]
async fn the_timeout_covers_the_body_too() {
    let mock = Mock::start(|_, _| Reply::StallBody {
        status: 200,
        body: br#"{"id":"co_1","#.to_vec(),
    })
    .await;
    let client = mock.builder().max_retries(0).build().expect("client");
    let scoped = client.with_options(RequestOptions::new().timeout(Duration::from_millis(250)));
    let started = Instant::now();
    let error = scoped.companies().get().await.expect_err("timeout");
    assert!(error.is_timeout(), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn timeouts_are_retried_for_repeatable_requests() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::Hang,
        _ => Reply::json(200, COMPANY),
    })
    .await;
    let client = mock
        .builder()
        .timeout(Duration::from_millis(150))
        .build()
        .expect("client");
    client
        .companies()
        .get()
        .await
        .expect("the second attempt succeeds");
    assert_eq!(mock.count(), 2);
}

#[tokio::test]
async fn dropping_the_future_cancels_the_call() {
    let mock = Mock::start(|_, _| Reply::Hang).await;
    let client = mock.client();
    let call = client.companies().get();
    let outcome = tokio::time::timeout(Duration::from_millis(100), call).await;
    assert!(
        outcome.is_err(),
        "the call was cancelled by the caller's own deadline"
    );
    assert_eq!(mock.count(), 1, "no retry happens after cancellation");
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(mock.count(), 1);
}

// ------------------------------------------------------------------- hooks

#[tokio::test]
async fn hooks_are_posted_without_the_api_key_and_signed() {
    let mock = Mock::always(
        202,
        r#"{"accepted":true,"duplicate":false,"event_id":"evt_1","run_ids":["run_1"]}"#,
    )
    .await;
    let client = mock.client();
    let body = serde_json::json!({"id": "order_1532", "amount_tiyn": 4_500_000});

    let payload = HookPayload::json(&body)
        .expect("payload")
        .secret("hksec_test")
        .event_id("order_1532");
    let signed = payload.body().to_vec();
    let accepted = client
        .send_hook("hk_tok/1", payload)
        .await
        .expect("accepted");
    assert!(accepted.accepted);
    assert_eq!(accepted.run_ids, ["run_1"]);

    let request = mock.last();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/hooks/hk_tok%2F1");
    assert_eq!(
        request.header("x-api-key"),
        None,
        "the token in the path is the credential"
    );
    assert_eq!(request.header("content-type"), Some("application/json"));
    assert_eq!(request.header("x-event-id"), Some("order_1532"));
    // The API reads Idempotency-Key before X-Event-Id: the event id must be the key.
    assert_eq!(request.header("idempotency-key"), Some("order_1532"));
    assert_eq!(request.body, signed, "the signed bytes are the sent bytes");
    let signature = request.header("x-signature").expect("signature");
    assert!(tungo_sdk::webhooks::verify_hook_signature(
        &request.body,
        signature,
        "hksec_test"
    ));

    // Raw bytes with their own content type, unsigned.
    let payload = HookPayload::bytes("a=1&b=2", "application/x-www-form-urlencoded");
    client
        .automations()
        .post_hook("hk_tok", payload)
        .await
        .expect("accepted");
    let request = mock.last();
    assert_eq!(
        request.header("content-type"),
        Some("application/x-www-form-urlencoded")
    );
    assert_eq!(request.header("x-signature"), None);
    assert_eq!(request.text(), "a=1&b=2");
    assert!(request.header("idempotency-key").is_some());

    // A key in the request options wins over the event id.
    let payload = HookPayload::bytes("{}", "application/json").event_id("order_1532");
    client
        .with_options(RequestOptions::new().idempotency_key("own"))
        .send_hook("hk_tok", payload)
        .await
        .expect("accepted");
    let request = mock.last();
    assert_eq!(request.header("idempotency-key"), Some("own"));
    assert_eq!(request.header("x-event-id"), Some("order_1532"));

    // A hook created with its own signature_header reads the signature only from it.
    let payload = HookPayload::bytes("{}", "application/json")
        .secret("hksec_test")
        .signature_header("x-shop-signature");
    client.send_hook("hk_tok", payload).await.expect("accepted");
    let request = mock.last();
    assert_eq!(request.header("x-signature"), None);
    let signature = request.header("x-shop-signature").expect("signature");
    assert!(tungo_sdk::webhooks::verify_hook_signature(
        &request.body,
        signature,
        "hksec_test"
    ));
    let payload = HookPayload::bytes("{}", "application/json")
        .secret("hksec_test")
        .signature_header("bad header");
    assert!(client.send_hook("hk_tok", payload).await.is_err());

    // An empty secret is an error, not an unsigned request.
    let payload = HookPayload::bytes("{}", "application/json").secret("");
    assert!(client.send_hook("hk_tok", payload).await.is_err());
    assert_eq!(mock.count(), 4);
}
