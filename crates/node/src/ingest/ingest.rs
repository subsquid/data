use crate::ingest::ingest_generic::{IngestGeneric, IngestMessage};
use crate::types::DatasetKind;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::Url;
use sqd_data_client::ReqwestDataClient;
use sqd_primitives::BlockNumber;


pub type DataSource = (reqwest::Client, Url);


pub fn ingest<'a, 'b>(
    message_sender: tokio::sync::mpsc::Sender<IngestMessage>,
    sources: Vec<DataSource>,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    prev_block_hash: Option<&'a str>
) -> BoxFuture<'b, anyhow::Result<()>> 
{
    match dataset_kind {
        DatasetKind::Evm => unimplemented!(),
        DatasetKind::Solana => {
            let data_client = ReqwestDataClient::new(sources[0].0.clone(), sources[0].1.clone());
            let builder = sqd_data::solana::tables::SolanaChunkBuilder::new();
            let ingest = IngestGeneric::new(
                data_client,
                builder,
                first_block,
                prev_block_hash,
                message_sender
            );
            ingest.run().boxed()
        }
    }
}