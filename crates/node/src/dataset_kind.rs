use serde::{Deserialize, Serialize};


#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKind {
    #[serde(rename = "eth")]
    Eth,
    #[serde(rename = "solana")]
    Solana
}


impl DatasetKind {
    pub fn storage_kind(&self) -> sqd_storage::db::DatasetKind {
        sqd_storage::db::DatasetKind::try_from(self.as_str()).unwrap()
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DatasetKind::Eth => "eth",
            DatasetKind::Solana => "solana"
        }
    }
}