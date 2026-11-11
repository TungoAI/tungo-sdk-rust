use tungo_sdk::resources::ProposalsListParams;
use tungo_sdk::{TryStreamExt, Tungo};

#[tokio::main]
async fn main() -> tungo_sdk::Result<()> {
    let client = Tungo::from_env()?;
    let params = ProposalsListParams::new().status("previewed").limit(50);

    // One page: the page model plus `has_next` and `next_page`.
    let page = client.proposals().list(&params).await?;
    println!("{} of {} proposals", page.data.len(), page.total);
    if let Some(next) = page.next_page().await? {
        println!("the next page has {} more", next.data.len());
    }

    // Every item: pages are requested lazily, with the same filters.
    let mut proposals = client.proposals().list_all(&params);
    while let Some(proposal) = proposals.try_next().await? {
        println!("{}", proposal.id);
    }
    Ok(())
}
