use tungo_sdk::types::{ChatEvent, ChatMessageRequest};
use tungo_sdk::{StreamExt, Tungo};

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    let client = Tungo::from_env()?;
    let request = ChatMessageRequest {
        message: Some("What is the VAT for October?".to_owned()),
        ..Default::default()
    };

    // Events arrive as the server sends them.
    let mut stream = client.chat().create_stream(&request).await?;
    println!(
        "dialog {:?}, turn {:?}",
        stream.dialog_id(),
        stream.turn_no()
    );
    while let Some(event) = stream.next().await {
        match event? {
            ChatEvent::Text(text) => print!("{}", text.delta),
            ChatEvent::Tool(tool) => eprintln!("[{} {}]", tool.name, tool.status),
            ChatEvent::Error(error) => eprintln!("error: {}", error.error.message),
            ChatEvent::Done(done) => eprintln!("\nstop reason: {:?}", done.stop_reason),
            _ => {}
        }
    }

    // Or let the SDK fold the stream into the final answer. An `error` event
    // is returned as `Error::Stream`, and a stream that ends before its
    // `done` event as `Error::Decode`: a partial text is never the answer.
    let turn = client
        .ask_stream("And for November?")
        .await?
        .accumulate()
        .await?;
    println!("{} (cost: {:?} tiyn)", turn.text, turn.cost_tiyn);
    Ok(())
}
