use crate::block::BlockArc;
use crate::cassandra::CassandraStorage;
use crate::chain_watch::ChainSender;
use crate::ingest::{create_data_source, grow_chain, write_chain};
use anyhow::{bail, Context};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_primitives::BlockNumber;
use std::sync::Arc;
use tokio::select;
use url::Url;


#[derive(clap::Args)]
pub struct Args {
    #[arg(required = true, short = 'c', long, value_name = "HOST[:PORT]")]
    cassandra_node: Vec<String>,

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
        .block_on(ingest(args))
}


async fn ingest(args: Args) -> anyhow::Result<()> {
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

    let data_source = create_data_source(
        args.data_source.into_iter().map(ReqwestDataClient::from_url).collect()
    );
    
    let chain_sender = ChainSender::<BlockArc>::new();

    let mut write_task = tokio::spawn(
        write_chain(storage.clone(), chain_sender.clone())
    );
    
    let mut ingest_task = tokio::spawn(
        grow_chain(
            storage.clone(), 
            args.first_block,
            args.parent_block_hash,
            chain_sender,
            data_source
        )
    );
    
    let res = select! {
        res = &mut write_task => {
            task_termination_error("write", res)
        },
        res = &mut ingest_task => {
            task_termination_error("ingest", res)
        }
    };
    
    write_task.abort();
    ingest_task.abort();
    
    res
}


fn task_termination_error(
    task_name: &str, 
    res: Result<anyhow::Result<()>, tokio::task::JoinError>
) -> anyhow::Result<()> 
{
    match res {
        Ok(Ok(_)) => bail!("{} task unexpectedly terminated", task_name),
        Ok(Err(err)) => Err(err.context(format!("{} task failed", task_name))),
        Err(join_error) => bail!("{} task terminated: {}", task_name, join_error)
    }    
}