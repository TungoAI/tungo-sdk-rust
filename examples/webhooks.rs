use tungo_sdk::types::WebhookEvent;
use tungo_sdk::{HookPayload, Tungo};

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    // The webhook secret is read from TUNGO_WEBHOOK_SECRET.
    let client = Tungo::from_env()?;

    // In your HTTP handler: the raw body and the request headers. Any
    // `http::HeaderMap`, string map or slice of pairs works as headers.
    let body = br#"{"id":"evt_1","type":"ping","created_at":"2026-11-09T15:00:00Z"}"#;
    let headers = [
        ("webhook-id", "msg_2xyzExample123"),
        ("webhook-timestamp", "1793974568"),
        (
            "webhook-signature",
            "v1,4H/XE/Z5TVdWNr7mzvFh+DuOghc+OuBDkZ1b82Fwhww=",
        ),
    ];
    match client.verify_webhook(body, &headers) {
        Ok(WebhookEvent::ProposalWritten(event)) => println!("written: {:?}", event.data),
        Ok(event) => println!("event {:?}", event.tag()),
        Err(error) => eprintln!("rejected ({}): {error}", error.reason()),
    }

    // Send an event to an incoming hook, signed with the hook's secret.
    let payload = HookPayload::bytes(
        r#"{"id":"order_1532","amount_tiyn":4500000}"#,
        "application/json",
    )
    .secret("hksec_...")
    .event_id("order_1532");
    let accepted = client.send_hook("hk_token", payload).await?;
    println!("runs started: {:?}", accepted.run_ids);
    Ok(())
}
