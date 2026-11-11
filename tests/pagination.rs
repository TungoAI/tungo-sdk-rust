//! Pages, auto-pagination and the wait helpers.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Mock, Reply};
use tungo_sdk::resources::ChatListParams;
use tungo_sdk::{Error, StreamExt, TryStreamExt, WaitOptions};

fn dialog(id: &str) -> String {
    format!(
        r#"{{"id":"{id}","message_count":2,"running":false,"title":"Диалог {id}","updated_at":"2026-11-09T15:00:00.000Z"}}"#
    )
}

fn page(ids: &[&str], next: Option<&str>) -> Reply {
    let items: Vec<String> = ids.iter().map(|id| dialog(id)).collect();
    let cursor = next.map_or("null".to_owned(), |cursor| format!("\"{cursor}\""));
    Reply::json(
        200,
        &format!(r#"{{"data":[{}],"next_cursor":{cursor}}}"#, items.join(",")),
    )
}

/// Three pages: d1 d2 | d3 d4 | d5.
fn three_pages() -> impl Fn(&common::Recorded, usize) -> Reply + Send + Sync + 'static {
    |request, _| match request.query() {
        "limit=2" => page(&["d1", "d2"], Some("c 2")),
        "limit=2&cursor=c%202" => page(&["d3", "d4"], Some("c3")),
        "limit=2&cursor=c3" => page(&["d5"], None),
        other => Reply::error(400, "invalid_request_error", "bad_query", other),
    }
}

fn two() -> ChatListParams {
    ChatListParams::new().limit(2)
}

#[tokio::test]
async fn pages_are_walked_by_hand() {
    let mock = Mock::start(three_pages()).await;
    let client = mock.client();

    let first = client.chat().list(&two()).await.expect("page 1");
    assert_eq!(first.data.len(), 2);
    assert_eq!(first.data[0].id, "d1");
    assert!(first.has_next());
    assert_eq!(first.next_cursor(), Some("c 2"));
    assert_eq!(first.meta().status(), 200);

    let second = first
        .next_page()
        .await
        .expect("page 2")
        .expect("there is a page 2");
    assert_eq!(second.data[1].id, "d4");
    let third = second
        .next_page()
        .await
        .expect("page 3")
        .expect("there is a page 3");
    assert!(!third.has_next());
    assert!(third.next_page().await.expect("no error").is_none());
    assert_eq!(
        mock.count(),
        3,
        "asking past the last page makes no request"
    );
    assert_eq!(third.into_items().len(), 1);
}

#[tokio::test]
async fn auto_pagination_yields_every_item_once() {
    let mock = Mock::start(three_pages()).await;
    let client = mock.client();
    let ids: Vec<String> = client
        .chat()
        .list_all(&two())
        .map_ok(|dialog| dialog.id)
        .try_collect()
        .await
        .expect("all items");
    assert_eq!(ids, ["d1", "d2", "d3", "d4", "d5"]);
    assert_eq!(mock.count(), 3);
}

#[tokio::test]
async fn auto_pagination_is_lazy() {
    let mock = Mock::start(three_pages()).await;
    let client = mock.client();
    let mut dialogs = client.chat().list_all(&two());
    assert_eq!(
        mock.count(),
        0,
        "nothing is requested until the stream is polled"
    );
    dialogs.try_next().await.expect("item").expect("d1");
    dialogs.try_next().await.expect("item").expect("d2");
    assert_eq!(
        mock.count(),
        1,
        "the second page is not fetched before it is needed"
    );
    drop(dialogs);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(mock.count(), 1, "stopping early stops the requests");
}

#[tokio::test]
async fn a_cursor_cycle_stops_the_walk() {
    // A, B, A, B, ...: comparing with the previous cursor alone would loop forever.
    let cycle = |_: &common::Recorded, index: usize| {
        let id = format!("d{index}");
        page(&[id.as_str()], Some(if index % 2 == 0 { "A" } else { "B" }))
    };
    let mock = Mock::start(cycle).await;
    let client = mock.client();
    let mut dialogs = client.chat().list_all(&Default::default());
    let mut seen = 0;
    let error = loop {
        match dialogs.next().await {
            Some(Ok(_)) => seen += 1,
            Some(Err(error)) => break error,
            None => panic!("the walk ended without reporting the loop"),
        }
        assert!(seen < 20, "the walk does not stop");
    };
    assert!(matches!(error, Error::Pagination(_)), "{error:?}");
    assert_eq!(
        seen, 3,
        "the items of every fetched page are delivered first"
    );
    assert_eq!(mock.count(), 3);

    // The same guard when the pages are fetched one by one.
    let mock = Mock::start(cycle).await;
    let client = mock.client();
    let first = client
        .chat()
        .list(&Default::default())
        .await
        .expect("page 1");
    let second = first.next_page().await.expect("page 2").expect("a page");
    let third = second.next_page().await.expect("page 3").expect("a page");
    let error = third.next_page().await.expect_err("the cursor A again");
    assert!(matches!(error, Error::Pagination(_)), "{error:?}");
    assert_eq!(mock.count(), 3);
}

#[tokio::test]
async fn a_repeated_cursor_stops_the_walk() {
    let mock = Mock::start(|_, _| page(&["d1"], Some("same"))).await;
    let client = mock.client();
    let mut dialogs = client.chat().list_all(&Default::default());
    let mut seen = 0;
    let error = loop {
        match dialogs.next().await {
            Some(Ok(_)) => seen += 1,
            Some(Err(error)) => break error,
            None => panic!("the walk ended without reporting the loop"),
        }
    };
    assert!(matches!(error, Error::Pagination(_)), "{error:?}");
    assert_eq!(seen, 2);
    assert_eq!(mock.count(), 2, "the loop is cut after the first repeat");
    assert!(
        dialogs.next().await.is_none(),
        "the stream is finished after the error"
    );
}

#[tokio::test]
async fn an_error_in_the_middle_of_a_walk_is_returned() {
    let mock = Mock::start(|request, _| match request.query() {
        "" => page(&["d1"], Some("c2")),
        _ => Reply::error(403, "permission_error", "missing_permission", "Нет права"),
    })
    .await;
    let client = mock.client();
    let mut dialogs = client.chat().list_all(&Default::default());
    assert_eq!(
        dialogs.try_next().await.expect("item").expect("d1").id,
        "d1"
    );
    let error = dialogs.try_next().await.expect_err("403");
    assert_eq!(error.status(), Some(403));
    assert!(dialogs.next().await.is_none());
}

#[tokio::test]
async fn an_empty_cursor_is_the_last_page() {
    let mock = Mock::start(|_, _| page(&["d1"], Some(""))).await;
    let client = mock.client();
    let first = client.chat().list(&Default::default()).await.expect("page");
    assert!(!first.has_next());
    let all: Vec<_> = client
        .chat()
        .list_all(&Default::default())
        .try_collect()
        .await
        .expect("items");
    assert_eq!(all.len(), 1);
}

// ------------------------------------------------------------ wait helpers

fn run(state: &str) -> String {
    format!(
        r#"{{"answer":null,"attempt":1,"auto_retries":0,"cancel_requested":false,"created_at":"2026-11-09T15:00:00.000Z",
        "current_step":null,"error":null,"error_code":null,"event":null,"event_id":null,"event_type":null,
        "external_id":null,"finished_at":null,"id":"run_1","output":null,"output_error":null,"output_note":null,
        "proposals":[],"requested_by":null,"retry_at":null,"retryable":false,"scenario_id":"sc_1","slot_at":null,
        "started_at":null,"state":"{state}","steps":[],"stop_reason":null,"tools":[],"trial":false,"trigger":"manual"}}"#
    )
}

fn fast() -> WaitOptions {
    WaitOptions::new()
        .timeout(Duration::from_secs(5))
        .poll_interval(Duration::from_millis(10), Duration::from_millis(20))
}

#[tokio::test]
async fn wait_for_run_polls_until_a_final_state() {
    let mock = Mock::start(|_, index| match index {
        0 => Reply::json(200, &run("queued")),
        1 => Reply::json(200, &run("running")),
        _ => Reply::json(200, &run("done")),
    })
    .await;
    let finished = mock
        .client()
        .wait_for_run("run_1", &fast())
        .await
        .expect("run");
    assert_eq!(finished.state.as_str(), "done");
    assert_eq!(mock.count(), 3);
    assert_eq!(
        mock.last().target,
        "/v1/companies/co_1/automations/runs/run_1"
    );
}

#[tokio::test]
async fn wait_for_run_times_out_and_keeps_polling_unknown_states() {
    let mock = Mock::start(|_, _| Reply::json(200, &run("running"))).await;
    let options = WaitOptions::new()
        .timeout(Duration::from_millis(150))
        .poll_interval(Duration::from_millis(20), Duration::from_millis(20));
    let started = Instant::now();
    let error = mock
        .client()
        .wait_for_run("run_1", &options)
        .await
        .expect_err("timeout");
    assert!(error.is_timeout(), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(mock.count() >= 2);

    // A state this SDK does not know is not final: the run is polled again.
    let mock = Mock::start(|_, index| match index {
        0 | 1 => Reply::json(200, &run("teleported")),
        _ => Reply::json(200, &run("done")),
    })
    .await;
    let finished = mock
        .client()
        .wait_for_run("run_1", &fast())
        .await
        .expect("the run finishes after the unknown state");
    assert_eq!(finished.state.as_str(), "done");
    assert_eq!(mock.count(), 3);

    // An unknown state that never changes ends with the deadline.
    let mock = Mock::start(|_, _| Reply::json(200, &run("teleported"))).await;
    let error = mock
        .client()
        .wait_for_run("run_1", &options)
        .await
        .expect_err("timeout");
    assert!(error.is_timeout(), "{error:?}");

    // An answer without a state, or with an empty one, is not a run.
    for body in [run("").replace(r#""state":"","#, ""), run("")] {
        let mock = Mock::start(move |_, _| Reply::json(200, &body)).await;
        let error = mock
            .client()
            .wait_for_run("run_1", &fast())
            .await
            .expect_err("no state");
        assert!(matches!(error, Error::Decode { .. }), "{error:?}");
        assert_eq!(mock.count(), 1);
    }
}

#[tokio::test]
async fn wait_for_batch_does_not_take_an_empty_object_for_a_finished_job() {
    // `{}` has no running flag: it is not a job that has finished.
    let mock = Mock::always(200, "{}").await;
    let error = mock
        .client()
        .wait_for_batch("bat_1", &fast())
        .await
        .expect_err("not a batch job");
    assert!(matches!(error, Error::Decode { .. }), "{error:?}");
    assert_eq!(mock.count(), 1);

    assert_eq!(
        WaitOptions::default().timeout,
        Duration::from_secs(600),
        "the default deadline is ten minutes, as in the other SDKs"
    );
}

#[tokio::test]
async fn run_scenario_and_wait_starts_then_polls() {
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&polls);
    let mock = Mock::start(move |request, _| {
        if request.method == "POST" {
            return Reply::json(200, &run("queued"));
        }
        match seen.fetch_add(1, Ordering::SeqCst) {
            0 => Reply::json(200, &run("running")),
            _ => Reply::json(200, &run("error")),
        }
    })
    .await;
    let finished = mock
        .client()
        .run_scenario_and_wait("sc_1", None, &fast())
        .await
        .expect("run");
    assert_eq!(
        finished.state.as_str(),
        "error",
        "a failed run is returned, not hidden"
    );
    let requests = mock.requests();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].target,
        "/v1/companies/co_1/automations/scenarios/sc_1/run"
    );
    assert!(requests[0].body.is_empty());
    assert!(requests[0].header("idempotency-key").is_some());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn batches_are_uploaded_and_awaited() {
    let job = |running: bool| {
        format!(
            r#"{{"events":0,"id":"job_1","kind":"bank_statement","running":{running},
            "started_at":"2026-11-09T15:00:00.000Z","title":"Выписка"}}"#
        )
    };
    let mock = Mock::start(
        move |request, index| match (request.method.as_str(), index) {
            ("POST", _) => Reply::json(200, &job(true)),
            (_, 1) => Reply::json(200, &job(true)),
            _ => Reply::json(200, &job(false)),
        },
    )
    .await;
    let client = mock.client();
    let started = client
        .upload_bank_statement("выписка.txt", &[0, 255, 1, 2])
        .await
        .expect("job");
    assert!(started.running);
    let upload = mock.last();
    assert_eq!(upload.target, "/v1/companies/co_1/batch/bank-statement");
    assert_eq!(
        upload.json(),
        serde_json::json!({"file": "AP8BAg==", "file_name": "выписка.txt"})
    );

    let finished = client
        .wait_for_batch(&started.id, &fast())
        .await
        .expect("job");
    assert!(!finished.running);
    assert_eq!(mock.count(), 3);
}

#[tokio::test]
async fn ask_sends_one_message_and_returns_the_turn() {
    let turn = r#"{"answer":"НДС — 5 400 ₸.","citations":[],"dialog_id":"dlg_1","errors":[],
        "pending_question":null,"proposals":[],"questions":[],"stop_reason":"end_turn","turn_no":1}"#;
    let mock = Mock::always(200, turn).await;
    let answer = mock.client().ask("Какой НДС?").await.expect("turn");
    assert_eq!(answer.answer, "НДС — 5 400 ₸.");
    assert_eq!(answer.dialog_id, "dlg_1");
    assert_eq!(
        mock.last().json(),
        serde_json::json!({"message": "Какой НДС?", "stream": false})
    );
}
