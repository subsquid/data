use crate::block::BlockArc;
use crate::cassandra::CassandraStorage;
use crate::data_source::create_data_source;
use crate::ingest::Ingest;
use anyhow::Context;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_primitives::BlockNumber;
use std::sync::Arc;
use tracing::debug;
use url::Url;


#[derive(clap::Args)]
pub struct Args {
    #[arg(required = true, short = 'c', long, value_name = "HOST[:PORT]")]
    pub cassandra_node: Vec<String>,

    #[arg(required = true, short = 'k', long, value_name = "NAME")]
    pub cassandra_keyspace: String,

    #[arg(required = true, short = 's', long, value_name = "URL")]
    pub data_source: Vec<Url>,

    #[arg(long, value_name = "NUMBER", default_value = "0")]
    pub first_block: BlockNumber,

    #[arg(long, value_name = "HASH")]
    pub parent_block_hash: Option<String>
}


pub fn run(args: Args) -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_async(args))
}


async fn run_async(args: Args) -> anyhow::Result<()> {
    let cassandra_session = {
        use scylla::client::session_builder::SessionBuilder;

        let session = SessionBuilder::new()
            .known_nodes(args.cassandra_node.iter())
            .build()
            .await
            .context("cassandra connection failed")?;

        Arc::new(session)
    };

    let storage = CassandraStorage::new(
        cassandra_session,
        &args.cassandra_keyspace
    ).await?;

    debug!("cassandra storage initialized");

    let data_source = create_data_source(
        args.data_source.into_iter().map(ReqwestDataClient::from_url).collect()
    );

    let (_, handle) = Ingest::new(storage, data_source)
        .set_first_block(args.first_block)
        .set_parent_block_hash(args.parent_block_hash)
        .start()
        .await?;

    handle.await
}