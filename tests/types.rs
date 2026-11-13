//! The generated models: forward compatibility, null versus absent, unions,
//! and webhook verification through the public entry points.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tungo_sdk::types::{
    AutomationRunDetailOutputValue, AutomationRunDetailState, AutomationSchedule,
    AutomationScheduleDaily, AutomationStateInput, BatchApproveRequest, ChatDialogSummary,
    ChatEvent, ChatMessageRequest, Company, WebhookEndpointPatch, WebhookEvent,
    WEBHOOK_EVENT_TYPES,
};
use tungo_sdk::webhooks::WebhookVerificationError;
use tungo_sdk::{HeaderMap, Tungo, WebhookVerifier};

#[test]
fn enums_keep_values_this_sdk_does_not_know() {
    let known: AutomationRunDetailState = serde_json::from_value(json!("done")).expect("known");
    assert_eq!(known, AutomationRunDetailState::Done);

    let unknown: AutomationRunDetailState =
        serde_json::from_value(json!("teleported")).expect("an unknown value still decodes");
    assert_eq!(
        unknown,
        AutomationRunDetailState::Unknown("teleported".to_owned())
    );
    assert_eq!(unknown.as_str(), "teleported");
    assert_eq!(unknown.to_string(), "teleported");
    assert_eq!(
        serde_json::to_value(&unknown).expect("round trip"),
        json!("teleported")
    );

    assert_eq!(
        AutomationRunDetailState::from("queued"),
        AutomationRunDetailState::Queued
    );
    assert_eq!(
        "running".parse::<AutomationRunDetailState>(),
        Ok(AutomationRunDetailState::Running)
    );
}

#[test]
fn objects_ignore_unknown_fields_and_require_the_required_ones() {
    let company = json!({
        "id": "co_1", "workspace_id": "ws_1", "name": "ТОО Ромашка", "bin": null,
        "legal_form": "spaceship", "status": "active", "role": "owner", "role_name": "Владелец",
        "permissions": ["chat.use", "teleport.use"], "created_at": "2026-11-09T15:00:00.000Z",
        "added_in_a_newer_api": {"x": 1}
    });
    let decoded: Company = serde_json::from_value(company.clone()).expect("company");
    assert_eq!(decoded.bin, None);
    assert_eq!(
        decoded.legal_form.as_ref().map(|f| f.as_str()),
        Some("spaceship"),
        "an unknown enum value is kept"
    );
    // A company whose legal form is not set yet has a null legal_form.
    let mut unset = company.clone();
    unset["legal_form"] = serde_json::Value::Null;
    let decoded_unset: Company =
        serde_json::from_value(unset).expect("company without a legal form");
    assert_eq!(decoded_unset.legal_form, None);
    assert_eq!(decoded.permissions.len(), 2);
    assert_eq!(decoded.permissions[1].as_str(), "teleport.use");

    // A field the contract requires must be there: without it the object is
    // not a company (no empty name, no zero). A wrong type is an error too.
    for required in ["name", "status", "id"] {
        let mut partial = company.clone();
        partial.as_object_mut().expect("object").remove(required);
        let error = serde_json::from_value::<Company>(partial).expect_err("a required field");
        let wanted = format!("missing field `{required}`");
        assert!(error.to_string().contains(&wanted), "{error}");
    }

    let mut broken = company;
    broken["name"] = json!(42);
    let error = serde_json::from_value::<Company>(broken).expect_err("a wrong type is an error");
    assert!(error.to_string().contains("string"), "{error}");
}

#[test]
fn numbers_keep_their_precision() {
    let summary: ChatDialogSummary = serde_json::from_value(json!({
        "id": "d1", "message_count": 3, "running": false, "title": "t", "title_source": "ai",
        "updated_at": "2026-11-09"
    }))
    .expect("summary");
    assert_eq!(summary.message_count, 3.0);

    // int64 money survives beyond 2^53.
    let event: ChatEvent = serde_json::from_str(
        r#"{"type":"done","stop_reason":"end_turn","cost_tiyn":9007199254740993}"#,
    )
    .expect("event");
    let ChatEvent::Done(done) = event else {
        panic!("expected a done event");
    };
    assert_eq!(done.cost_tiyn, Some(9_007_199_254_740_993));
}

#[test]
fn request_bodies_tell_null_from_absent() {
    let empty = WebhookEndpointPatch::default();
    assert_eq!(serde_json::to_value(&empty).expect("json"), json!({}));

    let clear = WebhookEndpointPatch {
        channels: Some(None),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&clear).expect("json"),
        json!({"channels": null})
    );

    let set = WebhookEndpointPatch {
        channels: Some(Some(vec!["a".to_owned()])),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&set).expect("json"),
        json!({"channels": ["a"]})
    );

    // And back: null and absent decode to different values.
    let null: WebhookEndpointPatch =
        serde_json::from_value(json!({"channels": null})).expect("null");
    assert_eq!(null.channels, Some(None));
    let absent: WebhookEndpointPatch = serde_json::from_value(json!({})).expect("absent");
    assert_eq!(absent.channels, None);

    let state = AutomationStateInput {
        paused: false,
        reason: Some(None),
    };
    assert_eq!(
        serde_json::to_value(&state).expect("json"),
        json!({"paused": false, "reason": null})
    );

    let message = ChatMessageRequest {
        message: Some("hi".to_owned()),
        dialog_id: Some(None),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&message).expect("json"),
        json!({"message": "hi", "dialog_id": null})
    );
}

#[test]
fn reserved_words_are_renamed_only_in_rust() {
    let request = BatchApproveRequest {
        async_: Some(true),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&request).expect("json"),
        json!({"async": true})
    );
}

#[test]
fn tagged_unions_pick_the_variant_by_the_tag() {
    let daily: AutomationSchedule =
        serde_json::from_value(json!({"kind": "daily", "at": "09:00"})).expect("daily");
    assert!(matches!(&daily, AutomationSchedule::Daily(schedule) if schedule.at == "09:00"));
    assert_eq!(daily.tag(), Some("daily"));
    assert_eq!(
        serde_json::to_value(&daily).expect("json"),
        json!({"kind": "daily", "at": "09:00"})
    );

    // A variant from a newer API is kept as raw JSON and round-trips.
    let raw = json!({"kind": "lunar", "phase": "full"});
    let lunar: AutomationSchedule = serde_json::from_value(raw.clone()).expect("unknown variant");
    assert!(matches!(&lunar, AutomationSchedule::Unknown(value) if value["phase"] == "full"));
    assert_eq!(lunar.tag(), Some("lunar"));
    assert_eq!(serde_json::to_value(&lunar).expect("json"), raw);

    // No tag at all, or a body that is not an object, does not fail either.
    let untagged: AutomationSchedule =
        serde_json::from_value(json!({"at": "09:00"})).expect("no tag");
    assert!(matches!(untagged, AutomationSchedule::Unknown(_)));
    assert_eq!(untagged.tag(), None);

    // A known tag with a property of the wrong type is an error, not a silent Unknown.
    let broken = serde_json::from_value::<AutomationSchedule>(json!({"kind": "daily", "at": 9}));
    assert!(broken.is_err(), "{broken:?}");

    // A known tag without a property its variant requires is kept as raw
    // JSON: the typed variant is never filled with an empty value.
    let partial: AutomationSchedule =
        serde_json::from_value(json!({"kind": "daily"})).expect("kept as raw JSON");
    assert!(
        matches!(partial, AutomationSchedule::Unknown(_)),
        "{partial:?}"
    );
    assert_eq!(partial.tag(), Some("daily"));

    // The fixed tag value is filled in by `Default`.
    let built = AutomationSchedule::Daily(AutomationScheduleDaily {
        at: "10:30".to_owned(),
        ..Default::default()
    });
    assert_eq!(
        serde_json::to_value(&built).expect("json"),
        json!({"kind": "daily", "at": "10:30"})
    );
}

#[test]
fn primitive_unions_decode_each_shape() {
    let decode = |value: Value| -> AutomationRunDetailOutputValue {
        serde_json::from_value(value).expect("a value")
    };
    assert_eq!(
        decode(json!(true)),
        AutomationRunDetailOutputValue::Boolean(true)
    );
    assert_eq!(
        decode(json!(12.5)),
        AutomationRunDetailOutputValue::Number(12.5)
    );
    assert_eq!(
        decode(json!("x")),
        AutomationRunDetailOutputValue::String("x".to_owned())
    );
    assert!(matches!(
        decode(json!([1, 2])),
        AutomationRunDetailOutputValue::Other(Value::Array(_))
    ));
    assert_eq!(
        serde_json::to_value(decode(json!("x"))).expect("json"),
        json!("x")
    );
}

// ---------------------------------------------------------------- webhooks

const SECRET: &str = "whsec_MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
const BODY: &str = "{\"id\":\"evt_01h455vb4pex5vsknk084sn02q\",\"type\":\"proposal.written\",\"created_at\":\"2026-11-09T15:00:00.000Z\",\"workspace_id\":\"ws_01h455vb4pex5vsknk084sn02q\",\"company_id\":\"co_01h455vb4pex5vsknk084sn02q\",\"data\":{\"proposal_id\":\"P-000042\",\"kind\":\"payment_out\",\"status\":\"written\",\"amount_tiyn\":1250000,\"onec_document_number\":\"00ЦБ-000042\"}}";
const SIGNATURE: &str = "v1,ZRCt3+b69jHpabRApIIfKTd6llLmiFecNltLMFlNk/c=";
const SENT_AT: u64 = 1_793_974_568;

fn sent_at() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(SENT_AT)
}

#[test]
fn webhooks_verify_from_every_header_container() {
    let verifier = WebhookVerifier::new(SECRET)
        .expect("verifier")
        .now(sent_at());
    let timestamp = SENT_AT.to_string();

    let pairs = [
        ("Svix-Id", "msg_2xyzExample123"),
        ("Svix-Timestamp", timestamp.as_str()),
        ("Svix-Signature", SIGNATURE),
    ];
    let event = verifier.verify(BODY, &pairs).expect("an array of pairs");
    let WebhookEvent::ProposalWritten(written) = &event else {
        panic!("expected proposal.written, got {event:?}");
    };
    assert_eq!(written.data.proposal_id, "P-000042");
    assert_eq!(written.data.kind, "payment_out");
    assert_eq!(written.data.amount_tiyn, Some(1_250_000));

    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(
            name.parse::<tungo_sdk::reqwest::header::HeaderName>()
                .expect("name"),
            value.parse().expect("value"),
        );
    }
    verifier.verify(BODY.as_bytes(), &map).expect("a HeaderMap");

    let hash: std::collections::HashMap<String, String> = pairs
        .iter()
        .map(|(name, value)| (name.to_uppercase(), (*value).to_owned()))
        .collect();
    verifier
        .verify(BODY, &hash)
        .expect("a HashMap with upper-case names");

    let vector: Vec<(String, String)> = pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    verifier.verify(BODY, &vector).expect("a Vec of pairs");
    verifier
        .verify(BODY, vector.as_slice())
        .expect("a slice of pairs");
}

#[test]
fn webhook_failures_are_distinguishable() {
    let timestamp = SENT_AT.to_string();
    let headers = [
        ("svix-id", "msg_2xyzExample123"),
        ("svix-timestamp", timestamp.as_str()),
        ("svix-signature", SIGNATURE),
    ];
    let verifier = WebhookVerifier::new(SECRET).expect("verifier");

    // The real clock is far from the timestamp of the fixture.
    let stale = verifier.verify(BODY, &headers).expect_err("stale");
    assert!(
        matches!(
            stale,
            WebhookVerificationError::TimestampTooOld | WebhookVerificationError::TimestampTooNew
        ),
        "{stale:?}"
    );

    let fixed = verifier.clone().now(sent_at());
    let tampered = fixed.verify(BODY.replace("P-000042", "P-000043"), &headers);
    assert_eq!(
        tampered.expect_err("tampered"),
        WebhookVerificationError::SignatureMismatch
    );

    let missing = fixed
        .verify(BODY, &headers[..2])
        .expect_err("no signature header");
    assert!(
        matches!(missing, WebhookVerificationError::MissingHeader(_)),
        "{missing:?}"
    );

    assert_eq!(
        WebhookVerifier::new("").expect_err("empty secret"),
        WebhookVerificationError::InvalidSecret
    );
    let zero = fixed
        .clone()
        .tolerance(Duration::ZERO)
        .verify(BODY, &headers);
    assert_eq!(
        zero.expect_err("zero tolerance"),
        WebhookVerificationError::InvalidTolerance
    );

    // A verified body that is not an event is reported as such.
    let sdk_error: tungo_sdk::Error = WebhookVerificationError::SignatureMismatch.into();
    assert!(matches!(
        sdk_error,
        tungo_sdk::Error::WebhookVerification(_)
    ));
}

#[test]
fn the_client_verifies_with_its_configured_secret() {
    let timestamp = SENT_AT.to_string();
    let headers = [
        ("svix-id", "msg_2xyzExample123"),
        ("svix-timestamp", timestamp.as_str()),
        ("svix-signature", SIGNATURE),
    ];
    let without = Tungo::builder().ignore_env().build().expect("client");
    assert_eq!(
        without
            .verify_webhook(BODY, &headers)
            .expect_err("no secret"),
        WebhookVerificationError::MissingSecret
    );

    let with = Tungo::builder()
        .ignore_env()
        .webhook_secret(SECRET)
        .build()
        .expect("client");
    // The secret is used: the only failure left is the age of the fixture.
    let error = with.verify_webhook(BODY, &headers).expect_err("stale");
    assert!(
        matches!(
            error,
            WebhookVerificationError::TimestampTooOld | WebhookVerificationError::TimestampTooNew
        ),
        "{error:?}"
    );
}

#[test]
fn the_list_of_webhook_event_types_matches_the_union() {
    assert!(WEBHOOK_EVENT_TYPES.contains(&"proposal.written"));
    assert!(WEBHOOK_EVENT_TYPES.contains(&"ping"));
    for name in WEBHOOK_EVENT_TYPES {
        // Every listed type is a known variant: a body with this tag and an
        // `id` of the wrong type is rejected by the variant's model. An
        // unknown tag would be kept as raw JSON instead.
        let decoded = serde_json::from_value::<WebhookEvent>(json!({"type": name, "id": 5}));
        assert!(decoded.is_err(), "{name} is not a variant: {decoded:?}");
    }
    let future: WebhookEvent =
        serde_json::from_value(json!({"type": "moon.landed", "id": "evt_1"}))
            .expect("unknown type");
    assert!(matches!(future, WebhookEvent::Unknown(_)));
    assert_eq!(future.tag(), Some("moon.landed"));
}

// ------------------------------------------------------------------- money

#[test]
fn money_is_exact() {
    use tungo_sdk::money::{format_tiyn, to_tiyn, to_tiyn_f64, MoneyError};
    assert_eq!(to_tiyn("45000.50"), Ok(4_500_050));
    assert_eq!(
        to_tiyn("1.005"),
        Ok(101),
        "half away from zero, with no float in between"
    );
    assert_eq!(to_tiyn_f64(1.005), Ok(101));
    assert_eq!(format_tiyn(4_500_050), "45 000,50 ₸");
    assert!(matches!(to_tiyn("12,5"), Err(MoneyError::Invalid(_))));
    assert!(matches!(to_tiyn_f64(f64::NAN), Err(MoneyError::NotFinite)));
    let sdk_error: tungo_sdk::Error = to_tiyn("x").expect_err("invalid").into();
    assert!(matches!(sdk_error, tungo_sdk::Error::Money(_)));
}

#[cfg(feature = "rust_decimal")]
#[test]
fn money_converts_through_rust_decimal() {
    use rust_decimal::Decimal;
    use tungo_sdk::money::{to_decimal, to_tiyn_decimal};
    let amount: Decimal = "45000.505".parse().expect("decimal");
    assert_eq!(to_tiyn_decimal(amount), Ok(4_500_051));
    assert_eq!(to_decimal(4_500_050).to_string(), "45000.50");
    assert_eq!(to_tiyn_decimal(to_decimal(i64::MIN)), Ok(i64::MIN));
    assert!(to_tiyn_decimal(Decimal::MAX).is_err());
}

// ---------------------------------------------------------------- credentials

#[test]
fn debug_output_of_models_hides_credentials() {
    use tungo_sdk::types::{ApiKeyRolled, AutomationHookCreate, WebhookEndpointCreated};

    let body = json!({
        "id": "ep_1", "secret": "whsec_TOPSECRET", "url": "https://example.com/hook",
        "description": "", "enabled": true, "channels": null, "event_types": null,
        "created_at": "2026-11-09T15:00:00.000Z", "updated_at": "2026-11-09T15:00:00.000Z",
    });
    let created: WebhookEndpointCreated = serde_json::from_value(body).expect("decode");
    let text = format!("{created:?} {created:#?}");
    assert!(!text.contains("TOPSECRET"), "{text}");
    assert!(text.contains("[REDACTED]"), "{text}");
    assert!(text.contains("ep_1"), "{text}");
    // The value itself stays readable.
    assert_eq!(created.secret.as_deref(), Some("whsec_TOPSECRET"));

    let body = json!({
        "key": "tg_TOPSECRET", "id": "key_1", "name": "CRM", "prefix": "tg_live_ab",
        "status": "active", "workspace_id": "ws_1", "all_companies": false,
        "company_ids": ["co_1"], "no_companies": false, "permissions": ["company.read"],
        "created_at": "2026-11-09T15:00:00.000Z", "created_by": "usr_1",
        "expires_at": "2027-02-07T15:00:00.000Z",
    });
    let rolled: ApiKeyRolled = serde_json::from_value(body).expect("decode");
    assert!(!format!("{rolled:?}").contains("TOPSECRET"));
    assert_eq!(rolled.key.as_deref(), Some("tg_TOPSECRET"));

    let hook = AutomationHookCreate {
        signature_secret: Some(Some("TOPSECRET-TOPSECRET".to_owned())),
        ..Default::default()
    };
    assert!(!format!("{hook:?}").contains("TOPSECRET"));
}

// ------------------------------------------------------- strict decoding

#[test]
fn an_empty_object_is_not_a_model() {
    use tungo_sdk::types::{AutomationRunDetail, BatchJob, ChatTurnResult, PlatformAccount};

    // `{}` used to decode into any model: a batch job that "is not running",
    // a balance of zero. Now a required field must be present.
    let error = serde_json::from_str::<BatchJob>("{}").expect_err("not a batch job");
    assert!(error.to_string().contains("missing field"), "{error}");
    assert!(serde_json::from_str::<ChatTurnResult>("{}").is_err());
    assert!(serde_json::from_str::<AutomationRunDetail>("{}").is_err());
    assert!(serde_json::from_str::<PlatformAccount>("{}").is_err());

    // The running flag alone is what `wait_for_batch` looks at: without it
    // the job does not decode, whatever else is there.
    let job = json!({
        "id": "bat_1", "kind": "bank_statement", "title": "t", "events": 3,
        "started_at": "2026-11-09T10:00:00Z", "running": false,
    });
    let decoded: BatchJob = serde_json::from_value(job.clone()).expect("a complete job");
    assert!(!decoded.running);
    assert_eq!(decoded.finished_at, None, "an optional field may be absent");
    let mut flagless = job;
    flagless.as_object_mut().expect("object").remove("running");
    let error = serde_json::from_value::<BatchJob>(flagless).expect_err("no running flag");
    assert!(
        error.to_string().contains("missing field `running`"),
        "{error}"
    );
}

#[test]
fn the_error_models_alone_decode_leniently() {
    use tungo_sdk::types::FieldError;

    // An error answer is reported with whatever the server managed to send.
    let field: FieldError = serde_json::from_value(json!({"path": "name"})).expect("lenient");
    assert_eq!(field.path, "name");
    assert_eq!(field.message, "");
}

#[test]
fn a_union_value_that_breaks_its_contract_is_kept_as_raw_json() {
    // A known tag with a required property missing: the typed variant
    // cannot hold it, the stream must not break, nothing is zero-filled.
    let raw = json!({"type": "text"});
    let event: ChatEvent = serde_json::from_value(raw.clone()).expect("not an error");
    assert_eq!(event, ChatEvent::Unknown(raw.clone()));
    assert_eq!(event.tag(), Some("text"));
    assert_eq!(serde_json::to_value(&event).expect("encode"), raw);

    // Nested models are covered too.
    let nested = json!({"type": "choice", "choice_id": "ch_1", "questions": [{"header": "h"}]});
    let event: ChatEvent = serde_json::from_value(nested.clone()).expect("not an error");
    assert_eq!(event, ChatEvent::Unknown(nested));

    // A property of the wrong type is still an error.
    assert!(serde_json::from_value::<ChatEvent>(json!({"type": "text", "delta": 5})).is_err());

    let good: ChatEvent =
        serde_json::from_value(json!({"type": "text", "delta": "a"})).expect("ok");
    assert!(matches!(good, ChatEvent::Text(_)));
}

#[test]
fn an_unset_enumeration_cannot_be_sent() {
    use tungo_sdk::types::CompanyStatus;

    // `Default` is the empty `Unknown`: "not chosen yet". It must not go out
    // as an empty string in a required property.
    let unset = CompanyStatus::default();
    let error = serde_json::to_value(&unset).expect_err("not set");
    assert!(error.to_string().contains("is not set"), "{error}");
    assert_eq!(
        serde_json::to_value(CompanyStatus::from("active")).expect("encode"),
        json!("active")
    );
    assert_eq!(
        serde_json::to_value(CompanyStatus::from("from_the_future")).expect("encode"),
        json!("from_the_future")
    );
}
