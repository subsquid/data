use serde::{Deserialize, Serialize};
use sqd_dataset::DatasetDescriptionRef;
use sqd_query::{BlockNumber, Query};
use sqd_storage::db::Database;
use std::sync::Arc;


pub use sqd_storage::db::DatabaseSettings;
pub type Name = &'static str;
pub type DBRef = Arc<Database>;


#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKind {
    #[serde(rename = "evm")]
    Evm,
    #[serde(rename = "solana")]
    Solana
}


impl DatasetKind {
    pub fn storage_kind(&self) -> sqd_storage::db::DatasetKind {
        sqd_storage::db::DatasetKind::from_str(self.as_str())
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DatasetKind::Evm => "evm",
            DatasetKind::Solana => "solana"
        }
    }
    
    pub fn dataset_description(&self) -> DatasetDescriptionRef {
        match self {
            DatasetKind::Evm => sqd_data::evm::tables::EVMChunkBuilder::dataset_description(),
            DatasetKind::Solana => sqd_data::solana::tables::SolanaChunkBuilder::dataset_description()
        }
    }
    
    pub fn from_query(query: &Query) -> Self {
        match query {
            Query::Eth(_) => Self::Evm,
            Query::Solana(_) => Self::Solana,
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