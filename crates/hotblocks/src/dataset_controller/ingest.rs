use crate::dataset_controller::ingest_generic::{IngestGeneric, IngestMessage};
use crate::types::DatasetKind;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_data_source::{DataSource, StandardDataSource};
use sqd_primitives::BlockNumber;


pub fn ingest<'a, 'b>(
    message_sender: tokio::sync::mpsc::Sender<IngestMessage>,
    sources: Vec<ReqwestDataClient>,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    parent_block_hash: Option<&'a str>
) -> BoxFuture<'b, anyhow::Result<()>> 
{
    macro_rules! run {
        ($builder:expr) => {{
            let mut data_source = StandardDataSource::new(sources, from_json_bytes);
            data_source.set_position(first_block, parent_block_hash);
            IngestGeneric::new(
                data_source,
                $builder,
                message_sender
            ).run().boxed()
        }};
    }

    match dataset_kind {
        DatasetKind::Evm => {
            run!(sqd_data::evm::tables::EvmChunkBuilder::new())
        },
        DatasetKind::Solana => {
            run!(sqd_data::solana::tables::SolanaChunkBuilder::new())
        },
        DatasetKind::Bitcoin => {
            run!(sqd_data::bitcoin::tables::BitcoinChunkBuilder::new())
        },
        DatasetKind::HyperliquidFills => {
            run!(sqd_data::hyperliquid_fills::tables::HyperliquidFillsChunkBuilder::new())
        }
        DatasetKind::HyperliquidReplicaCmds => {
            run!(sqd_data::hyperliquid_replica_cmds::tables::HyperliquidReplicaCmdsChunkBuilder::new())
        }
    }
}


fn from_json_bytes<T: DeserializeOwned>(bytes: Bytes) -> anyhow::Result<T> {
    serde_json::from_slice(&bytes).map_err(|err| err.into())
}