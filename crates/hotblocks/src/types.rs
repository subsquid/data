use serde::{Deserialize, Serialize};
use sqd_dataset::DatasetDescriptionRef;
use sqd_query::{BlockNumber, Query};
use sqd_storage::db::Database;
use std::sync::Arc;


pub type DBRef = Arc<Database>;


#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKind {
    #[serde(rename = "evm")]
    Evm,
    #[serde(rename = "solana")]
    Solana,
    #[serde(rename = "hyperliquid-fills")]
    HyperliquidFills,
    #[serde(rename = "hyperliquid-replica-cmds")]
    HyperliquidReplicaCmds,
}


impl DatasetKind {
    pub fn storage_kind(&self) -> sqd_storage::db::DatasetKind {
        sqd_storage::db::DatasetKind::from_str(self.as_str())
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DatasetKind::Evm => "evm",
            DatasetKind::Solana => "solana",
            DatasetKind::HyperliquidFills => "hyperliquid-fills",
            DatasetKind::HyperliquidReplicaCmds => "hyperliquid-replica-cmds",
        }
    }
    
    pub fn dataset_description(&self) -> DatasetDescriptionRef {
        match self {
            DatasetKind::Evm => sqd_data::evm::tables::EvmChunkBuilder::dataset_description(),
            DatasetKind::Solana => sqd_data::solana::tables::SolanaChunkBuilder::dataset_description(),
            DatasetKind::HyperliquidFills => sqd_data::hyperliquid_fills::tables::HyperliquidFillsChunkBuilder::dataset_description(),
            DatasetKind::HyperliquidReplicaCmds => sqd_data::hyperliquid_replica_cmds::tables::HyperliquidReplicaCmdsChunkBuilder::dataset_description(),
        }
    }
    
    pub fn from_query(query: &Query) -> Self {
        match query {
            Query::Eth(_) => Self::Evm,
            Query::Solana(_) => Self::Solana,
            Query::HyperliquidFills(_) => Self::HyperliquidFills,
            Query::HyperliquidReplicaCmds(_) => Self::HyperliquidReplicaCmds,
            _ => unimplemented!()
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionStrategy {
    FromBlock {
        number: BlockNumber,
        parent_hash: Option<String>
    },
    Head(u64),
    None
}