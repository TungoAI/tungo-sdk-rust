use tungo_sdk::{ApiErrorKind, Error, Tungo};

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    let client = Tungo::from_env()?;

    match client.proposals().get("P-000042").await {
        Ok(proposal) => println!("{:?}", proposal.into_inner()),
        Err(Error::Api(error)) => {
            // status, type, code, message, request id, field errors, retry-after,
            // extra properties, headers and the raw body.
            eprintln!("{} {} {:?}", error.status, error.error_type, error.code);
            eprintln!("{} (request id {:?})", error.message, error.request_id);
            for field in &error.fields {
                eprintln!("  {}: {}", field.path, field.message);
            }
            match error.kind {
                ApiErrorKind::NotFound => eprintln!("no such proposal"),
                ApiErrorKind::Limit => eprintln!("retry after {:?}", error.retry_after),
                ApiErrorKind::Permission => {
                    eprintln!("missing {:?}", error.extra.get("permission"))
                }
                _ => {}
            }
        }
        Err(Error::Timeout { message, .. }) => eprintln!("timed out: {message}"),
        Err(Error::Connection { message, .. }) => eprintln!("network problem: {message}"),
        Err(Error::Decode { message, .. }) => eprintln!("unexpected response: {message}"),
        // Raised before any request: no API key, an empty identifier, ...
        Err(error @ (Error::Config(_) | Error::Validation(_))) => eprintln!("{error}"),
        Err(other) => return Err(other),
    }
    Ok(())
}
