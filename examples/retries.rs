use std::time::Duration;

use tungo_sdk::{RequestOptions, Tungo};

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    // For every call of this client: 5 retries, 20 s per attempt.
    let client = Tungo::builder()
        .max_retries(5)
        .timeout(Duration::from_secs(20))
        .build()?;

    // For one call: no retries, 2 s, and an idempotency key of your own.
    let options = RequestOptions::new()
        .max_retries(0)
        .timeout(Duration::from_secs(2))
        .idempotency_key("approve-P-000042");
    let decision = client
        .with_options(options)
        .proposals()
        .approve("P-000042", None)
        .await?;
    println!("{:?}", decision.into_inner());
    Ok(())
}
