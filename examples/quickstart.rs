use tungo_sdk::Tungo;

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    // The API key is read from TUNGO_API_KEY when it is not passed explicitly.
    let client = Tungo::builder()
        .company_id("co_01h455vb4pex5vsknk084sn02q")
        .build()?;

    let turn = client.ask("What is the VAT for October?").await?;
    println!("{}", turn.answer);
    println!("request id: {:?}", turn.request_id());
    Ok(())
}
