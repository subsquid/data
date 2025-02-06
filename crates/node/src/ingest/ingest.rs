use crate::ingest::ingest_generic::{IngestGeneric, IngestMessage};
use crate::types::DatasetKind;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::Url;
use serde::de::DeserializeOwned;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_data_source::StandardDataSource;
use sqd_primitives::BlockNumber;


pub type DataSource = (reqwest::Client, Url);


pub fn ingest<'a, 'b>(
    message_sender: tokio::sync::mpsc::Sender<IngestMessage>,
    sources: Vec<DataSource>,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    parent_block_hash: Option<&'a str>
) -> BoxFuture<'b, anyhow::Result<()>> 
{
    let data_clients = sources.into_iter().map(|(http, url)| {
        ReqwestDataClient::new(http, url)    
    }).collect();
    
    match dataset_kind {
        DatasetKind::Evm => unimplemented!(),
        DatasetKind::Solana => {
            let data_source = StandardDataSource::new(data_clients, from_json_bytes);
            let builder = sqd_data::solana::tables::SolanaChunkBuilder::new();
            let ingest = IngestGeneric::new(
                data_source,
                builder,
                first_block,
                parent_block_hash.map(|s| s.to_string()),
                message_sender
            );
            ingest.run().boxed()
        }
    }
}


fn from_json_bytes<T: DeserializeOwned>(bytes: Bytes) -> anyhow::Result<T> {
    serde_json::from_slice(&bytes).map_err(|err| err.into())
}