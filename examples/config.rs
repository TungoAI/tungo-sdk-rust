use std::time::Duration;

use tungo_sdk::{RequestOptions, Tungo};

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    let client = Tungo::builder()
        .api_key("tg_...")
        .base_url("https://api.tungo.kz")
        .webhook_secret("whsec_...")
        .company_id("co_...")
        .workspace_id("ws_...")
        .timeout(Duration::from_secs(30))
        .max_retries(3)
        .default_header("x-client-name", "billing-service")
        // Your own reqwest settings (proxy, TLS, connection pool). The SDK
        // finishes the builder and turns redirects off, so the API key is
        // never forwarded to another host.
        .http_client_builder(tungo_sdk::reqwest::Client::builder())
        .build()?;

    // Per-request options: `with_options` returns a client that applies them
    // to every call made through it and shares the connection pool.
    let options = RequestOptions::new()
        .timeout(Duration::from_secs(5))
        .max_retries(0)
        .company_id("co_other")
        .header("x-trace-id", "trace-1")?;
    let company = client.with_options(options).companies().get().await?;

    // An idempotency key belongs to one request: the client returned by
    // `with_options` sends it once, with an operation that accepts it.
    let keyed = client.with_options(RequestOptions::new().idempotency_key("order-1532"));
    keyed.proposals().approve("P-000042", None).await?;

    // Every response carries its status, headers, request id and rate limit.
    println!("{} (HTTP {})", company.name, company.status());
    println!("request id: {:?}", company.request_id());
    println!("rate limit: {:?}", company.rate_limit());

    // Cancellation: drop the future, for example with a deadline of your own.
    let call = client.companies().list();
    if tokio::time::timeout(Duration::from_secs(1), call)
        .await
        .is_err()
    {
        println!("cancelled");
    }
    Ok(())
}
