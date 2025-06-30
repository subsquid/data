#![allow(unused)]
mod cassandra;
mod chain;
mod chain_sender;
mod ingest;
mod block;
mod util;


use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await?;

    println!("connected");
    Ok(())
}