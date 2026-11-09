use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tungo_sdk::{
    accumulate_chat_stream, is_valid_bin_iin, parse_api_error, parse_sse, sign_hook_payload,
    verify_hook_signature, verify_webhook, AskInput, JsonValue, TungoClient, TungoErrorKind,
    TungoMoney,
};

#[test]
fn test_conformance_vectors() {
    let raw = std::fs::read_to_string("conformance/vectors.json")
        .expect("read conformance/vectors.json");
    let v = JsonValue::parse(&raw).expect("parse vectors.json");

    // 1. Svix Webhook Verification
    let svix = v.get("webhook_verification").unwrap();
    let secret = svix.get("secret").unwrap().as_str().unwrap();
    let msg_id = svix.get("msg_id").unwrap().as_str().unwrap();
    let ts = svix.get("timestamp").unwrap().as_i64().unwrap();
    let payload = svix.get("body").unwrap().as_str().unwrap();
    let dual_sig = svix.get("dual_signature").unwrap().as_str().unwrap();
    let mut headers = BTreeMap::new();
    headers.insert("svix-id".to_string(), msg_id.to_string());
    headers.insert("svix-timestamp".to_string(), ts.to_string());
    headers.insert("svix-signature".to_string(), dual_sig.to_string());
    let env = verify_webhook(payload, &headers, secret, Some(ts), 300).expect("verify_webhook");
    assert_eq!(env.event, "proposal.written");
    assert_eq!(env.company_id, "co_01h455vb4pex5vsknk084sn02q");
    assert!(verify_webhook(&(payload.to_string() + "x"), &headers, secret, Some(ts), 300).is_err());

    // 2. Incoming Hook HMAC-SHA256 (with and without prefix)
    let hook = v.get("incoming_hook").unwrap();
    let h_sec = hook.get("secret").unwrap().as_str().unwrap();
    let h_pay = hook.get("body").unwrap().as_str().unwrap();
    let h_sig = hook.get("prefixed_signature").unwrap().as_str().unwrap();
    let h_hex = hook.get("hex_signature").unwrap().as_str().unwrap();
    assert_eq!(sign_hook_payload(h_pay, h_sec), h_sig);
    assert!(verify_hook_signature(h_pay, h_sig, h_sec));
    assert!(verify_hook_signature(h_pay, h_hex, h_sec));

    // 3. Money (Tiyn <-> Tenge + Format) & Kazakhstan BIN/IIN
    for m in v.get("money").unwrap().as_array().unwrap() {
        let tenge = m.get("tenge").unwrap().as_f64().unwrap();
        let tiyn = m.get("tiyn").unwrap().as_i64().unwrap();
        let fmt_kzt = m.get("formatted").unwrap().as_str().unwrap();
        assert_eq!(TungoMoney::to_tiyn(tenge), tiyn);
        assert!((TungoMoney::to_tenge(tiyn) - tenge).abs() < 1e-9);
        assert_eq!(TungoMoney::format_tiyn(tiyn, "KZT"), fmt_kzt);
    }
    for b in v.get("bin_iin").unwrap().as_array().unwrap() {
        let val = b.get("value").unwrap().as_str().unwrap();
        let valid = b.get("valid").unwrap().as_bool().unwrap();
        assert_eq!(is_valid_bin_iin(val), valid, "BIN/IIN {}", val);
    }

    // 4. Structured Errors + Hint
    for e in v.get("errors").unwrap().as_array().unwrap() {
        let status = e.get("status").unwrap().as_i64().unwrap() as u16;
        let body = e.get("body").unwrap().as_str().unwrap();
        let exp_code = e.get("error_code").unwrap().as_str().unwrap();
        let exp_hint = e.get("hint").unwrap().as_str().unwrap();
        let exp_req_id = e.get("request_id").unwrap().as_str().unwrap();
        let parsed = parse_api_error(status, body, &BTreeMap::new());
        assert_eq!(parsed.code, exp_code);
        assert_eq!(parsed.hint.as_deref(), Some(exp_hint));
        assert_eq!(parsed.request_id.as_deref(), Some(exp_req_id));
        if status == 429 {
            assert_eq!(parsed.kind, TungoErrorKind::RateLimit);
        }
        if status == 402 {
            assert_eq!(parsed.kind, TungoErrorKind::PaymentRequired);
        }
    }

    // 5. SSE Parser & AccumulateChatStream
    let sse = v.get("sse").unwrap();
    let events = parse_sse(sse.get("raw_stream").unwrap().as_str().unwrap());
    let expected = sse.get("events").unwrap().as_array().unwrap();
    assert_eq!(events.len(), expected.len());
    for (i, exp) in expected.iter().enumerate() {
        assert_eq!(events[i].event, exp.get("event").unwrap().as_str().unwrap());
    }
    let acc = accumulate_chat_stream(&events);
    assert_eq!(acc.text, sse.get("expected_text").unwrap().as_str().unwrap());
    assert_eq!(acc.dialog_id, "dlg_01");
    assert_eq!(acc.cost_tiyn, 185);
}

#[test]
fn test_client_ask_retry_and_pagination() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_srv = attempts.clone();

    let srv = thread::spawn(move || {
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&stream);
            let mut req_line = String::new();
            reader.read_line(&mut req_line).unwrap();
            let mut content_len = 0usize;
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h == "\r\n" || h.is_empty() {
                    break;
                }
                if let Some(v) = h.strip_prefix("Content-Length:") {
                    content_len = v.trim().parse().unwrap_or(0);
                }
            }
            if content_len > 0 {
                let mut buf = vec![0u8; content_len];
                reader.read_exact(&mut buf).unwrap();
            }

            let path = req_line.split_whitespace().nth(1).unwrap_or("");
            if path.starts_with("/v1/companies/co_demo/chat") {
                let n = attempts_srv.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    let body = r#"{"error":{"code":"rate_limit_exceeded","message":"Slow down"}}"#;
                    let resp = format!(
                        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0.01\r\nx-ratelimit-limit: 600\r\nx-ratelimit-remaining: 598\r\nx-ratelimit-reset: 1762167600\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(resp.as_bytes()).unwrap();
                } else {
                    let body = r#"{"dialog_id":"conv_1","turn_no":1,"answer":"Сальдо: 45 000,00 ₸"}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nx-ratelimit-limit: 600\r\nx-ratelimit-remaining: 597\r\nx-ratelimit-reset: 1762167600\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(resp.as_bytes()).unwrap();
                }
            } else if path.contains("cursor=cur_2") {
                let body = r#"{"data":[{"id":"prop_3"}],"next_cursor":null}"#;
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
                stream.write_all(resp.as_bytes()).unwrap();
            } else {
                let body = r#"{"data":[{"id":"prop_1"},{"id":"prop_2"}],"next_cursor":"cur_2"}"#;
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
                stream.write_all(resp.as_bytes()).unwrap();
            }
        }
    });

    let client = TungoClient::builder()
        .api_key("tg_live_test")
        .base_url(format!("http://{}", addr))
        .company_id("co_demo")
        .max_retries(2)
        .build();

    let ans = client
        .ask(AskInput {
            prompt: "Какое сальдо?".to_string(),
            company_id: None,
            conversation_id: None,
        })
        .expect("ask succeeds after retry");
    assert_eq!(ans.reply, "Сальдо: 45 000,00 ₸");
    assert_eq!(ans.conversation_id, "conv_1");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(client.last_rate_limit().unwrap().remaining, 597);

    let all = client.proposals().list_all(None).expect("list_all proposals");
    assert_eq!(all.len(), 3);
    srv.join().unwrap();
}
