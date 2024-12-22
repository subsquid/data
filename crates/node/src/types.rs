use std::sync::Arc;
use sqd_storage::db::Database;


pub type Name = &'static str;
pub type DBRef = Arc<Database>;


#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DatasetKind {
    Evm,
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
}