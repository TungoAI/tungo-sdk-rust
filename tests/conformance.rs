//! Runs every section of the shared conformance vectors
//! (`conformance/vectors.json`) against the SDK. Each test checks that it
//! ran at least one case, so an empty or renamed section fails loudly.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde_json::Value;
use tungo_sdk::money::{format_tiyn, format_tiyn_in, is_valid_bin_iin, to_tiyn, to_tiyn_f64};
use tungo_sdk::request::{encode_path_segment, encode_query, QueryValue};
use tungo_sdk::retry::{
    backoff_delay, parse_retry_after, retry_delay, should_retry, FailureCause, RetryDecision,
    JITTER_MIN,
};
use tungo_sdk::sse::{decode_event, parse_sse, SseEvent, SseParser};
use tungo_sdk::types::{ChatEvent, ChatMessageRequest, WebhookEvent};
use tungo_sdk::webhooks::{
    hook_signature_header, sign_hook_payload, verify_hook_signature, WebhookVerifier,
};
use tungo_sdk::{
    ApiError, ApiErrorKind, ChatAccumulator, Error, HeaderMap, Method, ResponseMeta, StreamExt,
};

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/conformance/vectors.json"
));

fn vectors() -> Value {
    serde_json::from_str(VECTORS).expect("vectors.json is JSON")
}

/// The cases at `path` (a dot-separated path into the vectors file).
fn cases(path: &str) -> Vec<Value> {
    let mut node = vectors();
    for key in path.split('.') {
        node = node
            .get(key)
            .unwrap_or_else(|| panic!("vectors.json has no `{path}`"))
            .clone();
    }
    let list = node
        .as_array()
        .unwrap_or_else(|| panic!("`{path}` is not a list"))
        .clone();
    assert!(!list.is_empty(), "`{path}` has no cases");
    list
}

fn name(case: &Value) -> &str {
    case["name"].as_str().unwrap_or("(unnamed)")
}

fn text<'a>(case: &'a Value, key: &str) -> &'a str {
    case[key]
        .as_str()
        .unwrap_or_else(|| panic!("`{key}` of `{}` is not a string", name(case)))
}

fn header_map(headers: &Value) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (key, value) in headers.as_object().expect("headers are an object") {
        map.insert(
            key.parse::<tungo_sdk::reqwest::header::HeaderName>()
                .expect("header name"),
            value
                .as_str()
                .expect("header value")
                .parse()
                .expect("header value"),
        );
    }
    map
}

fn body_bytes(case: &Value) -> Vec<u8> {
    STANDARD
        .decode(text(case, "body_base64"))
        .expect("body_base64 is base64")
}

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

#[test]
fn webhook_verification() {
    let root = vectors();
    let default_tolerance = root["webhook_verification"]["default_tolerance_seconds"]
        .as_u64()
        .expect("default tolerance");
    assert_eq!(
        tungo_sdk::webhooks::DEFAULT_TOLERANCE.as_secs(),
        default_tolerance
    );

    let mut ran = 0;
    for case in cases("webhook_verification.cases") {
        let headers: Vec<(String, String)> = case["headers"]
            .as_object()
            .expect("headers")
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().expect("value").to_owned()))
            .collect();
        let expect = &case["expect"];
        let outcome = WebhookVerifier::new(text(&case, "secret")).and_then(|mut verifier| {
            verifier = verifier.now(at(case["now"].as_u64().expect("now")));
            // A negative tolerance cannot be expressed: `Duration` is
            // unsigned. The closest spelling is zero, which is rejected.
            if let Some(seconds) = case.get("tolerance_seconds").and_then(Value::as_i64) {
                let seconds = u64::try_from(seconds).unwrap_or(0);
                verifier = verifier.tolerance(Duration::from_secs(seconds));
            }
            verifier.verify(text(&case, "body"), &headers)
        });
        match (expect["valid"].as_bool().expect("valid"), outcome) {
            (true, Ok(event)) => {
                assert_eq!(
                    event.tag(),
                    expect["event_type"].as_str(),
                    "{}",
                    name(&case)
                );
                // Only the deliberately incomplete vector may fall back to the unknown variant.
                if case.get("contract_valid").and_then(Value::as_bool) != Some(false) {
                    assert!(
                        !matches!(event, WebhookEvent::Unknown(_)),
                        "{}: a contract-valid event was decoded as unknown",
                        name(&case)
                    );
                }
            }
            (false, Err(error)) => {
                assert_eq!(error.reason(), text(expect, "reason"), "{}", name(&case));
            }
            (valid, outcome) => panic!("{}: expected valid={valid}, got {outcome:?}", name(&case)),
        }
        ran += 1;
    }
    assert!(ran > 0);
}

#[test]
fn hook_signature_sign() {
    for case in cases("hook_signature.sign") {
        let body = body_bytes(&case);
        let secret = text(&case, "secret");
        let expect = &case["expect"];
        if expect["error"].as_bool() == Some(true) {
            assert!(sign_hook_payload(&body, secret).is_err(), "{}", name(&case));
            assert!(
                hook_signature_header(&body, secret).is_err(),
                "{}",
                name(&case)
            );
            continue;
        }
        assert_eq!(
            sign_hook_payload(&body, secret).expect("signature"),
            text(expect, "hex"),
            "{}",
            name(&case)
        );
        assert_eq!(
            hook_signature_header(&body, secret).expect("header"),
            text(expect, "header"),
            "{}",
            name(&case)
        );
    }
}

#[test]
fn hook_signature_verify() {
    for case in cases("hook_signature.verify") {
        let valid = verify_hook_signature(
            &body_bytes(&case),
            text(&case, "signature"),
            text(&case, "secret"),
        );
        assert_eq!(
            Some(valid),
            case["expect"]["valid"].as_bool(),
            "{}",
            name(&case)
        );
    }
}

#[test]
fn money_to_tiyn() {
    for case in cases("money.to_tiyn") {
        let outcome = to_tiyn(text(&case, "input"));
        match case["expect"].get("tiyn") {
            Some(tiyn) => assert_eq!(
                outcome.as_ref().map(i64::to_string).ok().as_deref(),
                tiyn.as_str(),
                "{}: {outcome:?}",
                name(&case)
            ),
            None => assert!(outcome.is_err(), "{}: {outcome:?}", name(&case)),
        }
    }
}

#[test]
fn money_to_tiyn_float() {
    for case in cases("money.to_tiyn_float") {
        let input = match case.get("special").and_then(Value::as_str) {
            Some("NaN") => f64::NAN,
            Some("Infinity") => f64::INFINITY,
            Some("-Infinity") => f64::NEG_INFINITY,
            Some(other) => panic!("unknown special value `{other}`"),
            None => case["input"].as_f64().expect("input"),
        };
        let outcome = to_tiyn_f64(input);
        match case["expect"].get("tiyn") {
            Some(tiyn) => assert_eq!(
                outcome.as_ref().map(i64::to_string).ok().as_deref(),
                tiyn.as_str(),
                "{}: {outcome:?}",
                name(&case)
            ),
            None => assert!(outcome.is_err(), "{}: {outcome:?}", name(&case)),
        }
    }
}

#[test]
fn money_format_tiyn() {
    for case in cases("money.format_tiyn") {
        let tiyn: i64 = text(&case, "tiyn").parse().expect("tiyn");
        let formatted = match case.get("currency").and_then(Value::as_str) {
            Some(currency) => format_tiyn_in(tiyn, currency),
            None => format_tiyn(tiyn),
        };
        assert_eq!(formatted, text(&case["expect"], "text"), "{}", name(&case));
    }
}

#[test]
fn bin_iin() {
    for case in cases("bin_iin") {
        assert_eq!(
            Some(is_valid_bin_iin(text(&case, "input"))),
            case["expect"]["valid"].as_bool(),
            "{}",
            name(&case)
        );
    }
}

#[test]
fn errors() {
    for case in cases("errors.cases") {
        let status = u16::try_from(case["status"].as_u64().expect("status")).expect("status");
        let headers = header_map(&case["headers"]);
        let error = ApiError::from_response(status, &headers, text(&case, "body").as_bytes());
        let expect = &case["expect"];
        let label = name(&case);

        assert_eq!(error.status, status, "{label}");
        assert_eq!(
            Some(error.envelope),
            expect["envelope"].as_bool(),
            "{label}"
        );
        assert_eq!(error.kind.as_str(), text(expect, "class"), "{label}");
        assert_eq!(error.error_type, text(expect, "type"), "{label}");
        assert_eq!(error.code.as_deref(), expect["code"].as_str(), "{label}");
        assert_eq!(
            error.request_id.as_deref(),
            expect["request_id"].as_str(),
            "{label}"
        );
        assert_eq!(
            error.retry_after.map(|delay| delay.as_secs_f64()),
            expect["retry_after"].as_f64(),
            "{label}"
        );
        if let Some(message) = expect.get("message").and_then(Value::as_str) {
            assert_eq!(error.message, message, "{label}");
        } else {
            assert!(
                !error.message.is_empty(),
                "{label}: the message is never empty"
            );
        }
        let fields = serde_json::to_value(&error.fields).expect("fields");
        assert_eq!(fields, expect["fields"], "{label}");
        assert_eq!(
            Value::Object(error.extra.clone()),
            expect["extra"],
            "{label}"
        );
        assert_eq!(
            error.body,
            text(&case, "body"),
            "{label}: the raw body is kept"
        );
    }
}

#[test]
fn errors_status_fallback() {
    for case in cases("errors.status_fallback") {
        let status = u16::try_from(case["status"].as_u64().expect("status")).expect("status");
        let expected = text(&case["expect"], "type");
        assert_eq!(
            ApiErrorKind::from_status(status).as_str(),
            expected,
            "{status}"
        );
        let error = ApiError::from_response(status, &HeaderMap::new(), b"");
        assert_eq!(error.kind.as_str(), expected, "{status}");
        assert_eq!(error.error_type, expected, "{status}");
    }
}

#[test]
fn retry_after() {
    let root = vectors();
    let max_sleep = root["retry_after"]["max_sleep_seconds"]
        .as_u64()
        .expect("max");
    assert_eq!(tungo_sdk::retry::MAX_RETRY_AFTER.as_secs(), max_sleep);

    for case in cases("retry_after.cases") {
        let now = httpdate_from_rfc3339(text(&case, "now"));
        let parsed = parse_retry_after(&header_map(&case["headers"]), now);
        let expect = &case["expect"];
        // A delay of zero (the header says 0, or names a moment that has
        // passed) is not a usable value: the computed backoff applies, as
        // when the header is absent.
        let usable = expect["seconds"].as_f64().filter(|seconds| *seconds > 0.0);
        assert_eq!(
            parsed.map(|delay| delay.as_secs_f64()),
            usable,
            "{}",
            name(&case)
        );
        let sleeps = matches!(retry_delay(parsed, 0, 1.0), RetryDecision::Sleep(_));
        assert_eq!(Some(sleeps), expect["sleep"].as_bool(), "{}", name(&case));
        if let (Some(delay), true) = (parsed, sleeps) {
            assert_eq!(
                retry_delay(parsed, 0, 1.0),
                RetryDecision::Sleep(delay),
                "{}",
                name(&case)
            );
        }
    }
}

/// Parses `YYYY-MM-DDTHH:MM:SSZ` (the only form the vectors use).
fn httpdate_from_rfc3339(value: &str) -> SystemTime {
    let number = |range: std::ops::Range<usize>| -> u64 { value[range].parse().expect("digits") };
    assert!(
        value.ends_with('Z') && value.len() == 20,
        "unexpected time `{value}`"
    );
    let (year, month, day) = (number(0..4), number(5..7), number(8..10));
    // Days from the civil date (the algorithm of Howard Hinnant).
    let shifted_year = if month <= 2 { year - 1 } else { year };
    let era = shifted_year / 400;
    let year_of_era = shifted_year - era * 400;
    let shifted_month = (month + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    at(days * 86_400 + number(11..13) * 3600 + number(14..16) * 60 + number(17..19))
}

#[test]
fn backoff() {
    let root = vectors();
    let section = &root["backoff"];
    assert_eq!(
        Some(tungo_sdk::retry::BACKOFF_BASE.as_secs_f64()),
        section["base_seconds"].as_f64()
    );
    assert_eq!(
        Some(tungo_sdk::retry::BACKOFF_MAX.as_secs_f64()),
        section["max_seconds"].as_f64()
    );
    assert_eq!(Some(JITTER_MIN), section["jitter_min"].as_f64());
    assert_eq!(Some(1.0), section["jitter_max"].as_f64());

    for case in cases("backoff.cases") {
        let attempt = u32::try_from(case["attempt"].as_u64().expect("attempt")).expect("attempt");
        let expect = &case["expect"];
        let (min, max) = (
            expect["min_seconds"].as_f64().expect("min"),
            expect["max_seconds"].as_f64().expect("max"),
        );
        assert_eq!(
            backoff_delay(attempt, JITTER_MIN).as_secs_f64(),
            min,
            "{}",
            name(&case)
        );
        assert_eq!(
            backoff_delay(attempt, 1.0).as_secs_f64(),
            max,
            "{}",
            name(&case)
        );
        // Out-of-range jitter is clamped, and the delay the client really
        // sleeps stays inside the bounds.
        assert_eq!(
            backoff_delay(attempt, 0.0).as_secs_f64(),
            min,
            "{}",
            name(&case)
        );
        assert_eq!(
            backoff_delay(attempt, 9.0).as_secs_f64(),
            max,
            "{}",
            name(&case)
        );
        for step in 0..=10 {
            let jitter = JITTER_MIN + (1.0 - JITTER_MIN) * f64::from(step) / 10.0;
            let delay = backoff_delay(attempt, jitter).as_secs_f64();
            assert!((min..=max).contains(&delay), "{}: {delay}", name(&case));
        }
    }
}

#[test]
fn retry_policy() {
    for case in cases("retry_policy") {
        let method: Method = text(&case, "method").parse().expect("method");
        let has_key = case["idempotency_key"].as_bool().expect("idempotency_key");
        let cause = match &case["cause"] {
            Value::Number(status) => FailureCause::Status(
                u16::try_from(status.as_u64().expect("status")).expect("status"),
            ),
            Value::String(kind) if kind == "connection_error" => FailureCause::Connection,
            Value::String(kind) if kind == "timeout" => FailureCause::Timeout,
            other => panic!("unknown cause {other}"),
        };
        assert_eq!(
            Some(should_retry(&method, has_key, cause)),
            case["expect"]["retry"].as_bool(),
            "{}",
            name(&case)
        );
    }
}

#[test]
fn path_segment() {
    for case in cases("path_segment") {
        let outcome = encode_path_segment("id", text(&case, "input"));
        match case["expect"].get("encoded").and_then(Value::as_str) {
            Some(encoded) => assert_eq!(outcome.ok().as_deref(), Some(encoded), "{}", name(&case)),
            None => assert!(outcome.is_err(), "{}: {outcome:?}", name(&case)),
        }
    }
}

#[test]
fn query() {
    for case in cases("query") {
        let mut pairs: Vec<(String, String)> = Vec::new();
        for param in case["params"].as_array().expect("params") {
            let key = text(param, "name");
            // The values go through the same `QueryValue` conversions the
            // generated methods use.
            match (text(param, "type"), &param["value"]) {
                (_, Value::Null) => None::<String>.append_to(key, &mut pairs),
                ("boolean", Value::Bool(flag)) => flag.append_to(key, &mut pairs),
                ("integer", Value::Number(number)) => {
                    number.as_i64().expect("integer").append_to(key, &mut pairs);
                }
                ("integer", Value::String(digits)) => {
                    digits
                        .parse::<i64>()
                        .expect("int64")
                        .append_to(key, &mut pairs);
                }
                ("array", Value::Array(items)) => items
                    .iter()
                    .map(|item| item.as_str().expect("array item").to_owned())
                    .collect::<Vec<String>>()
                    .append_to(key, &mut pairs),
                ("string" | "date-time", Value::String(value)) => value.append_to(key, &mut pairs),
                (kind, value) => panic!("{}: unsupported {kind} value {value}", name(&case)),
            }
        }
        let encoded = encode_query(&pairs);
        let expect = &case["expect"];
        match expect.get("any_of").and_then(Value::as_array) {
            Some(allowed) => assert!(
                allowed
                    .iter()
                    .any(|item| item.as_str() == Some(encoded.as_str())),
                "{}: {encoded}",
                name(&case)
            ),
            None => assert_eq!(encoded, text(expect, "query"), "{}", name(&case)),
        }
    }
}

fn assert_events(case: &Value, events: &[SseEvent], how: &str) {
    let expected = case["expect"]["events"].as_array().expect("events");
    let label = format!("{} ({how})", name(case));
    assert_eq!(events.len(), expected.len(), "{label}: {events:?}");
    for (event, expect) in events.iter().zip(expected) {
        assert_eq!(event.event, text(expect, "event"), "{label}");
        assert_eq!(event.data, text(expect, "data"), "{label}");
        assert_eq!(event.id.as_deref(), expect["id"].as_str(), "{label}");
        assert_eq!(
            event.retry,
            expect.get("retry").and_then(Value::as_u64),
            "{label}"
        );
    }
}

#[test]
fn sse() {
    for case in cases("sse") {
        let raw = STANDARD
            .decode(text(&case, "raw_base64"))
            .expect("raw_base64");
        assert_events(&case, &parse_sse(&raw), "whole");

        // Byte by byte: no boundary may change the result.
        let mut parser = SseParser::new();
        let mut events = Vec::new();
        for byte in &raw {
            events.extend(parser.feed(std::slice::from_ref(byte)));
        }
        events.extend(parser.finish());
        assert_events(&case, &events, "byte by byte");

        if let Some(chunks) = case.get("chunks_base64").and_then(Value::as_array) {
            let mut parser = SseParser::new();
            let mut events = Vec::new();
            for chunk in chunks {
                let bytes = STANDARD
                    .decode(chunk.as_str().expect("chunk"))
                    .expect("chunk");
                events.extend(parser.feed(&bytes));
            }
            events.extend(parser.finish());
            assert_events(&case, &events, "given chunks");
        }
    }
}

#[test]
fn chat_stream() {
    for case in cases("chat_stream") {
        let label = name(&case);
        let status = u16::try_from(case["status"].as_u64().expect("status")).expect("status");
        let meta = ResponseMeta::new(status, header_map(&case["headers"]));
        let mut accumulator = ChatAccumulator::new(&meta);
        for event in parse_sse(text(&case, "raw").as_bytes()) {
            if let Some(decoded) = decode_event::<ChatEvent>(&event, &meta) {
                accumulator.push(decoded.unwrap_or_else(|error| panic!("{label}: {error}")));
            }
        }
        let turn = accumulator.finish();
        check_chat_turn(label, status, &turn, &case["expect"]);
    }
}

/// Checks a chat turn folded from a stream against `expect` of a
/// `chat_stream` (or `chat_stream_replay`) case.
fn check_chat_turn(label: &str, status: u16, turn: &tungo_sdk::ChatTurn, expect: &Value) {
    // Only a turn that saw its `done` event is complete.
    if let Some(types) = expect.get("event_types").and_then(Value::as_array) {
        let has_done = types.iter().any(|kind| kind == "done");
        assert_eq!(turn.done, has_done, "{label}");
    }
    assert_eq!(
        turn.dialog_id.as_deref(),
        expect["dialog_id"].as_str(),
        "{label}"
    );
    assert_eq!(turn.turn_no, expect["turn_no"].as_u64(), "{label}");
    assert_eq!(turn.text, text(expect, "text"), "{label}");
    assert_eq!(
        turn.stop_reason.as_deref(),
        expect["stop_reason"].as_str(),
        "{label}"
    );
    assert_eq!(
        Some(turn.events.len() as u64),
        expect["event_count"].as_u64(),
        "{label}"
    );
    assert_eq!(turn.cost_tiyn, expect["cost_tiyn"].as_i64(), "{label}");
    assert_eq!(
        turn.choice_id.as_deref(),
        expect["choice_id"].as_str(),
        "{label}"
    );
    let usage = serde_json::to_value(&turn.usage).expect("usage");
    assert_eq!(usage, expect["usage"], "{label}");
    if let Some(types) = expect.get("event_types").and_then(Value::as_array) {
        let seen: Vec<Value> = turn
            .events
            .iter()
            .map(|event| event.tag().map_or(Value::Null, Value::from))
            .collect();
        assert_eq!(&seen, types, "{label}");
    }
    match (&turn.error, &expect["error"]) {
        (None, Value::Null) => {}
        (Some(error), Value::Object(expected)) => {
            assert_eq!(
                Some(error.error_type.as_str()),
                expected["type"].as_str(),
                "{label}"
            );
            assert_eq!(error.code.as_deref(), expected["code"].as_str(), "{label}");
            assert_eq!(
                Some(error.message.as_str()),
                expected["message"].as_str(),
                "{label}"
            );
            assert_eq!(
                error.request_id.as_deref(),
                expected["request_id"].as_str(),
                "{label}"
            );
            assert_eq!(
                Value::Object(error.extra.clone()),
                expected["extra"],
                "{label}"
            );
            assert_eq!(error.status, status, "{label}");
        }
        (error, expected) => panic!("{label}: error {error:?}, expected {expected}"),
    }
}

/// A stream call repeated with the same Idempotency-Key after its turn
/// finished is answered with the stored `ChatTurnResult` as JSON: the stream
/// must yield the events of that turn, not a decode error.
#[tokio::test]
async fn chat_stream_replay() {
    let replays = cases("chat_stream_replay");
    for case in replays {
        let label = name(&case).to_owned();
        let status = u16::try_from(case["status"].as_u64().expect("status")).expect("status");
        let headers: Vec<(String, String)> = case["headers"]
            .as_object()
            .expect("headers")
            .iter()
            .map(|(key, value)| {
                (
                    key.to_lowercase(),
                    value.as_str().expect("header").to_owned(),
                )
            })
            .collect();
        let body = serde_json::to_vec(&case["body"]).expect("body");
        let mock = common::Mock::start(move |_, _| common::Reply::Full {
            status,
            headers: headers.clone(),
            body: body.clone(),
        })
        .await;
        let opened = mock
            .client()
            .chat()
            .create_stream(&ChatMessageRequest {
                message: Some("?".to_owned()),
                ..Default::default()
            })
            .await;
        let expect = &case["expect"];
        if expect["decode_error"] == Value::Bool(true) {
            match opened {
                Err(Error::Decode {
                    status: Some(got), ..
                }) => assert_eq!(got, status, "{label}"),
                other => panic!("{label}: want a decode error, got {:?}", other.map(|_| ())),
            }
            continue;
        }
        let mut stream = opened.unwrap_or_else(|error| panic!("{label}: {error}"));
        let mut accumulator = ChatAccumulator::new(stream.response());
        while let Some(event) = stream.next().await {
            accumulator.push(event.unwrap_or_else(|error| panic!("{label}: {error}")));
        }
        check_chat_turn(&label, status, &accumulator.finish(), expect);
    }
}

/// Every section of the vectors file has a test above: a new section must
/// not go unnoticed.
#[test]
fn every_section_is_covered() {
    let known = [
        "version",
        "description",
        "webhook_verification",
        "hook_signature",
        "money",
        "bin_iin",
        "errors",
        "retry_after",
        "backoff",
        "retry_policy",
        "path_segment",
        "query",
        "sse",
        "chat_stream",
        "chat_stream_replay",
    ];
    let root = vectors();
    for key in root.as_object().expect("vectors.json is an object").keys() {
        assert!(
            known.contains(&key.as_str()),
            "vectors section `{key}` has no test"
        );
    }
    assert_eq!(
        root["version"].as_u64(),
        Some(2),
        "the tests are written for version 2"
    );
}
