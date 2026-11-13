//! Event streams over real HTTP: incremental delivery, cancellation, errors
//! before and inside a stream, idle timeouts and long lines.

mod common;

use std::time::{Duration, Instant};

use common::{Mock, Reply};
use tungo_sdk::resources::FeedListEventsParams;
use tungo_sdk::types::{ChatEvent, ChatMessageRequest};
use tungo_sdk::{ApiErrorKind, ChatAccumulator, Error, RequestOptions, StreamExt};

fn message(text: &str) -> ChatMessageRequest {
    ChatMessageRequest {
        message: Some(text.to_owned()),
        ..Default::default()
    }
}

const TEXT_1: &str =
    "id: 1\nevent: text\ndata: {\"type\":\"text\",\"delta\":\"НДС \",\"step\":1}\n\n";
const TEXT_2: &str =
    "id: 2\nevent: text\ndata: {\"type\":\"text\",\"delta\":\"5 400 ₸.\",\"step\":1}\n\n";
const PING: &str = "event: ping\ndata: \n\n";
const DONE: &str = "id: 3\nevent: done\ndata: {\"type\":\"done\",\"stop_reason\":\"end_turn\",\"cost_tiyn\":185}\n\n";

fn chat_reply(delay_ms: u64, chunks: &[&str]) -> Reply {
    Reply::sse(delay_ms, chunks)
        .with_header("x-dialog-id", "dlg_1")
        .with_header("x-turn-no", "3")
        .with_header("x-request-id", "req_stream_1")
}

#[tokio::test]
async fn events_arrive_as_they_are_sent() {
    let mock = Mock::start(|_, _| chat_reply(300, &[TEXT_1, TEXT_2, PING, DONE])).await;
    let started = Instant::now();
    let mut stream = mock
        .client()
        .chat()
        .create_stream(&message("Какой НДС?"))
        .await
        .expect("stream");

    let request = mock.last();
    assert_eq!(request.header("accept"), Some("text/event-stream"));
    assert_eq!(
        request.json(),
        serde_json::json!({"message": "Какой НДС?", "stream": true})
    );
    assert_eq!(stream.dialog_id(), Some("dlg_1"));
    assert_eq!(stream.turn_no(), Some(3));
    assert_eq!(stream.response().request_id(), Some("req_stream_1"));

    let first = stream.next().await.expect("an event").expect("decoded");
    let first_at = started.elapsed();
    assert!(matches!(&first, ChatEvent::Text(text) if text.delta == "НДС "));
    assert_eq!(stream.last_event_id(), Some("1"));
    // The whole stream takes about 1.2 s; the first event must not wait for it.
    assert!(
        first_at < Duration::from_millis(800),
        "the first event came after {first_at:?}"
    );

    let mut rest = Vec::new();
    while let Some(event) = stream.next().await {
        rest.push(event.expect("decoded"));
    }
    assert!(
        started.elapsed() >= Duration::from_millis(1100),
        "the stream was really incremental"
    );
    assert_eq!(rest.len(), 2, "the ping heart-beat is skipped: {rest:?}");
    assert!(matches!(&rest[1], ChatEvent::Done(done) if done.cost_tiyn == Some(185)));
    assert_eq!(stream.last_event_id(), Some("3"));
}

#[tokio::test]
async fn accumulate_folds_the_turn() {
    let mock = Mock::start(|_, _| chat_reply(5, &[TEXT_1, TEXT_2, PING, DONE])).await;
    let turn = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect("turn");
    assert_eq!(turn.text, "НДС 5 400 ₸.");
    assert_eq!(turn.dialog_id.as_deref(), Some("dlg_1"));
    assert_eq!(turn.turn_no, Some(3));
    assert_eq!(turn.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(turn.cost_tiyn, Some(185));
    assert_eq!(turn.events.len(), 3);
}

#[tokio::test]
async fn the_accumulator_works_alongside_a_manual_loop() {
    let mock = Mock::start(|_, _| chat_reply(5, &[TEXT_1, TEXT_2, DONE])).await;
    let mut stream = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    let mut turn = ChatAccumulator::new(stream.response());
    let mut printed = String::new();
    while let Some(event) = stream.next().await {
        let event = event.expect("decoded");
        if let ChatEvent::Text(text) = &event {
            printed.push_str(&text.delta);
        }
        turn.push(event);
    }
    assert_eq!(turn.finish().text, printed);
}

#[tokio::test]
async fn leaving_the_loop_early_closes_the_connection() {
    let chunks: Vec<&str> = std::iter::repeat_n(TEXT_1, 200).collect();
    let mock = Mock::start(move |_, _| chat_reply(20, &chunks)).await;
    let mut stream = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    let mut seen = 0;
    while let Some(event) = stream.next().await {
        event.expect("decoded");
        seen += 1;
        if seen == 3 {
            break;
        }
    }
    drop(stream);
    // The mock notices the closed socket instead of writing all 200 chunks.
    for _ in 0..100 {
        if mock.aborted_streams() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        mock.aborted_streams(),
        1,
        "the server saw the client close the stream"
    );
    assert_eq!(mock.finished_streams(), 0);
}

#[tokio::test]
async fn an_error_status_before_the_stream_is_an_api_error() {
    let mock = Mock::start(|_, _| {
        Reply::error(
            402,
            "payment_required_error",
            "platform_balance_exhausted",
            "Пополните счёт",
        )
    })
    .await;
    let error = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect_err("402");
    assert_eq!(error.status(), Some(402));
    assert_eq!(error.kind(), Some(ApiErrorKind::PaymentRequired));
    assert_eq!(
        error.as_api().and_then(|api| api.code.as_deref()),
        Some("platform_balance_exhausted")
    );
    assert_eq!(mock.count(), 1);
}

#[tokio::test]
async fn opening_a_stream_is_retried_with_the_same_idempotency_key() {
    // A persistent failure uses the whole retry budget and is then returned.
    let mock = Mock::start(|_, _| {
        Reply::error(503, "unavailable_error", "busy", "busy").with_header("retry-after-ms", "1")
    })
    .await;
    let error = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect_err("503");
    assert_eq!(error.status(), Some(503));
    assert_eq!(mock.count(), 3);

    // A transient failure is retried before the first event, then the stream works.
    let mock = Mock::start(|_, index| match index {
        0 => Reply::error(503, "unavailable_error", "busy", "busy")
            .with_header("retry-after-ms", "1"),
        1 => Reply::Drop,
        _ => chat_reply(1, &[TEXT_1, DONE]),
    })
    .await;
    let turn = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect("turn");
    assert_eq!(turn.text, "НДС ");
    let requests = mock.requests();
    assert_eq!(requests.len(), 3);
    let key = requests[0].header("idempotency-key").expect("a key");
    assert!(requests
        .iter()
        .all(|r| r.header("idempotency-key") == Some(key)));

    // The caller's own key is used as given.
    let keyed = mock
        .client()
        .with_options(RequestOptions::new().idempotency_key("turn-1"));
    keyed
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .close();
    assert_eq!(mock.last().header("idempotency-key"), Some("turn-1"));
}

#[tokio::test]
async fn an_error_event_inside_the_stream_is_reported() {
    let error_event = "id: 2\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"limit_error\",\
        \"code\":\"budget\",\"message\":\"Дневной лимит исчерпан.\",\"limit_kind\":\"day\"},\"request_id\":\"req_evt\"}\n\n";
    let done = "id: 3\nevent: done\ndata: {\"type\":\"done\",\"stop_reason\":\"budget\"}\n\n";
    let mock = Mock::start(move |_, _| chat_reply(1, &[TEXT_1, error_event, done])).await;
    let client = mock.client();

    // Read by hand, the event is an ordinary typed event.
    let mut stream = client.ask_stream("Какой НДС?").await.expect("stream");
    let mut kinds = Vec::new();
    while let Some(event) = stream.next().await {
        kinds.push(event.expect("decoded").tag().map(str::to_owned));
    }
    assert_eq!(
        kinds,
        [
            Some("text".into()),
            Some("error".into()),
            Some("done".into())
        ]
    );

    // Folded, it becomes the error of the turn.
    let error = client
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect_err("the turn failed");
    let Error::Stream(api) = &error else {
        panic!("expected a stream error, got {error:?}");
    };
    assert_eq!(api.kind, ApiErrorKind::Limit);
    assert_eq!(api.code.as_deref(), Some("budget"));
    assert_eq!(api.message, "Дневной лимит исчерпан.");
    assert_eq!(api.request_id.as_deref(), Some("req_evt"));
    assert_eq!(
        api.extra.get("limit_kind").and_then(|v| v.as_str()),
        Some("day")
    );
    assert_eq!(error.status(), Some(200));
}

#[tokio::test]
async fn a_feed_error_event_ends_the_stream_with_its_code() {
    // The contract: a feed failure is `event: error` with
    // {error: "feed_failed", message}, then the end of the stream; reconnect.
    const ITEM: &str = "event: feed\ndata: {\"id\":\"f1\",\"cursor\":\"c1\",\"section\":\"auto\",\
        \"section_title\":\"s\",\"at\":\"2026-11-09T10:00:00Z\",\"title\":\"t\",\"actions\":[]}\n\n";
    const FAILURE: &str =
        "event: error\ndata: {\"error\":\"feed_failed\",\"message\":\"Лента недоступна\"}\n\n";
    let mock = Mock::start(|_, _| {
        Reply::sse(1, &[ITEM, FAILURE, ITEM]).with_header("x-request-id", "req_feed")
    })
    .await;
    let client = mock.client();
    let mut stream = client
        .feed()
        .list_events(&FeedListEventsParams::new().once(true))
        .await
        .expect("stream");
    let first = stream.next().await.expect("an item").expect("decoded");
    assert_eq!(first.id, "f1");
    let error = stream
        .next()
        .await
        .expect("the error")
        .expect_err("a stream error");
    let Error::Stream(api) = &error else {
        panic!("expected a stream error, got {error:?}");
    };
    assert_eq!(api.code.as_deref(), Some("feed_failed"));
    assert_eq!(api.message, "Лента недоступна");
    assert_eq!(api.request_id.as_deref(), Some("req_feed"));
    assert!(
        stream.next().await.is_none(),
        "the stream ends after its error"
    );
}

#[tokio::test]
async fn unknown_events_and_fields_do_not_break_the_stream() {
    let hologram = "event: hologram\ndata: {\"type\":\"hologram\",\"payload\":{\"x\":1}}\n\n";
    let future_done =
        "event: done\ndata: {\"type\":\"done\",\"stop_reason\":\"end_turn\",\"future\":[1,2]}\n\n";
    let mock = Mock::start(move |_, _| chat_reply(1, &[hologram, TEXT_1, future_done])).await;
    let mut stream = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    let first = stream.next().await.expect("an event").expect("decoded");
    let ChatEvent::Unknown(raw) = &first else {
        panic!("expected an unknown event, got {first:?}");
    };
    assert_eq!(
        raw["payload"]["x"], 1,
        "the raw JSON of an unknown event is kept"
    );
    assert_eq!(first.tag(), Some("hologram"));
    let turn = stream.accumulate().await.expect("turn");
    assert_eq!(turn.text, "НДС ");
    assert_eq!(turn.stop_reason.as_deref(), Some("end_turn"));
}

#[tokio::test]
async fn malformed_event_data_is_a_decode_error_and_the_stream_continues() {
    let broken = "event: text\ndata: {\"type\":\"text\",\"delta\":\n\n";
    let mock = Mock::start(move |_, _| chat_reply(1, &[broken, TEXT_1, DONE])).await;
    let mut stream = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    let first = stream.next().await.expect("an item");
    assert!(
        matches!(
            first,
            Err(Error::Decode {
                status: Some(200),
                ..
            })
        ),
        "{first:?}"
    );
    let second = stream.next().await.expect("an item").expect("decoded");
    assert!(matches!(second, ChatEvent::Text(_)));
}

#[tokio::test]
async fn a_stream_cut_mid_event_ends_without_inventing_events() {
    // The connection closes in the middle of the second event.
    let half = "id: 2\nevent: text\ndata: {\"type\":\"text\",\"del";
    let mock = Mock::start(move |_, _| chat_reply(1, &[TEXT_1, half])).await;
    let client = mock.client();
    let error = client
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect_err("no `done` arrived: a partial text is not a finished turn");
    match &error {
        Error::Decode {
            message,
            status: Some(200),
            ..
        } => assert!(message.contains("ended before completion"), "{message}"),
        other => panic!("{other:?}"),
    }

    // Reading the events one by one simply ends; the accumulator shows that
    // the turn is incomplete.
    let mut stream = client.ask_stream("Какой НДС?").await.expect("stream");
    let mut turn = ChatAccumulator::new(stream.response());
    while let Some(event) = stream.next().await {
        turn.push(event.expect("event"));
    }
    let turn = turn.finish();
    assert_eq!(turn.events.len(), 1);
    assert!(!turn.done);
    assert_eq!(turn.stop_reason, None);
}

#[tokio::test]
async fn a_success_that_is_not_an_event_stream_is_a_decode_error() {
    // A JSON answer (or a proxy page) with status 200 to a streaming call.
    for content_type in ["application/json", "text/html; charset=utf-8"] {
        let mock = Mock::start(move |_, _| Reply::Full {
            status: 200,
            headers: vec![
                ("content-type".into(), content_type.into()),
                ("x-request-id".into(), "req_json".into()),
            ],
            body: br#"{"answer":"42","dialog_id":"d"}"#.to_vec(),
        })
        .await;
        let error = mock
            .client()
            .ask_stream("Какой НДС?")
            .await
            .expect_err("not a stream");
        match &error {
            Error::Decode {
                message,
                status: Some(200),
                request_id: Some(id),
                body,
                ..
            } => {
                assert!(message.contains("text/event-stream"), "{message}");
                assert_eq!(id, "req_json");
                assert!(body.contains("answer"), "{body}");
            }
            other => panic!("{content_type}: {other:?}"),
        }
        assert_eq!(mock.count(), 1, "a decode error is not retried");
    }

    // No Content-Type at all is not a stream either.
    let mock = Mock::start(|_, _| Reply::Full {
        status: 200,
        headers: Vec::new(),
        body: b"data: {}\n\n".to_vec(),
    })
    .await;
    let error = mock.client().ask_stream("?").await.expect_err("no type");
    assert!(matches!(error, Error::Decode { .. }), "{error:?}");

    // Parameters and letter case of the media type do not matter.
    let mock = Mock::start(|_, _| Reply::Stream {
        status: 200,
        headers: vec![(
            "content-type".into(),
            "Text/Event-Stream; charset=utf-8".into(),
        )],
        chunks: vec![(1, DONE.as_bytes().to_vec())],
    })
    .await;
    let turn = mock
        .client()
        .ask_stream("?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect("turn");
    assert!(turn.done);
}

#[tokio::test]
async fn accumulate_skips_an_event_it_cannot_decode() {
    // The second event is not JSON: the turn goes on, as in the other SDKs.
    let broken = "id: 2\nevent: text\ndata: {\"type\":\"text\",\"delta\":\n\n";
    let mock = Mock::start(move |_, _| chat_reply(1, &[TEXT_1, broken, DONE])).await;
    let turn = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect("the turn survives one bad event");
    assert!(turn.done);
    assert_eq!(turn.skipped_events, 1);
    assert_eq!(turn.events.len(), 2);
}

#[tokio::test]
async fn a_slow_consumer_does_not_trip_the_idle_timeout() {
    // The server is never silent for more than 50 ms; the caller takes
    // 400 ms per event with an idle timeout of 250 ms.
    let mock = Mock::start(move |_, _| chat_reply(50, &[TEXT_1; 12])).await;
    let options = RequestOptions::new().stream_idle_timeout(Duration::from_millis(250));
    let mut stream = mock
        .client()
        .with_options(options)
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    let mut events = 0;
    while let Some(event) = stream.next().await {
        event.expect("the time spent by the caller is not silence of the server");
        events += 1;
        if events == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    assert_eq!(events, 4);
}

#[tokio::test]
async fn an_idle_stream_times_out() {
    // One event, then silence with the connection kept open.
    let mock = Mock::start(|_, _| Reply::Stream {
        status: 200,
        headers: vec![("content-type".into(), "text/event-stream".into())],
        chunks: vec![
            (1, TEXT_1.as_bytes().to_vec()),
            (30_000, DONE.as_bytes().to_vec()),
        ],
    })
    .await;
    let client = mock
        .builder()
        .stream_idle_timeout(Duration::from_millis(300))
        .build()
        .expect("client");
    let mut stream = client.ask_stream("Какой НДС?").await.expect("stream");
    stream.next().await.expect("an event").expect("decoded");
    let started = Instant::now();
    let stalled = stream.next().await.expect("an item");
    assert!(
        matches!(&stalled, Err(error) if error.is_timeout()),
        "{stalled:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(
        stream.next().await.is_none(),
        "the stream ends after the timeout"
    );

    // The idle limit is not a total limit: a slow but live stream runs longer.
    let mock = Mock::start(|_, _| chat_reply(150, &[TEXT_1, TEXT_1, TEXT_1, TEXT_1, DONE])).await;
    let client = mock
        .builder()
        .stream_idle_timeout(Duration::from_millis(400))
        .timeout(Duration::from_millis(400))
        .build()
        .expect("client");
    let turn = client
        .ask_stream("Какой НДС?")
        .await
        .expect("stream")
        .accumulate()
        .await
        .expect("a stream longer than the request timeout");
    assert_eq!(turn.events.len(), 5);
}

#[tokio::test]
async fn a_very_long_line_is_read_whole() {
    // One event whose data line is 8 MB long, delivered in 64 KB pieces.
    let filler = "ж".repeat(4 * 1024 * 1024);
    let event = format!("event: text\ndata: {{\"type\":\"text\",\"delta\":\"{filler}\"}}\n\n");
    let bytes = event.into_bytes();
    let chunks: Vec<(u64, Vec<u8>)> = bytes
        .chunks(65_531)
        .map(|piece| (0, piece.to_vec()))
        .collect();
    let expected = filler.len();
    let mock = Mock::start(move |_, _| Reply::Stream {
        status: 200,
        headers: vec![("content-type".into(), "text/event-stream".into())],
        chunks: chunks.clone(),
    })
    .await;
    let mut stream = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    let event = stream.next().await.expect("an event").expect("decoded");
    let ChatEvent::Text(text) = event else {
        panic!("expected a text event");
    };
    assert_eq!(text.delta.len(), expected);
    assert!(
        text.delta.chars().all(|c| c == 'ж'),
        "multi-byte characters survive chunk borders"
    );
}

#[tokio::test]
async fn get_streams_and_raw_events_work() {
    let mock = Mock::start(|_, _| chat_reply(1, &[TEXT_1, PING, DONE])).await;
    let client = mock.client();

    let stream = client.batches().list_events("job/1").await.expect("stream");
    let request = mock.last();
    assert_eq!(request.method, "GET");
    assert_eq!(request.target, "/v1/companies/co_1/batch/job%2F1/events");
    assert_eq!(request.header("accept"), Some("text/event-stream"));
    assert!(request.body.is_empty());

    // The raw view keeps heart-beats and event names.
    let mut raw = stream.into_raw();
    let mut names = Vec::new();
    while let Some(event) = raw.next().await {
        names.push(event.expect("an event").event);
    }
    assert_eq!(names, ["text", "ping", "done"]);
}

#[tokio::test]
async fn dropping_an_unread_stream_does_not_hang() {
    let mock = Mock::start(|_, _| chat_reply(50, &[TEXT_1; 100])).await;
    let stream = mock
        .client()
        .ask_stream("Какой НДС?")
        .await
        .expect("stream");
    stream.close();
    for _ in 0..100 {
        if mock.aborted_streams() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(mock.aborted_streams(), 1);
}
