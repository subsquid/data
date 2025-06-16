use serde::Deserialize;
use sqd_primitives::{BlockNumber, ItemIndex};


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_index: ItemIndex,
    pub user: String,
    pub actions: Vec<serde_json::Value>,
    pub raw_tx_hash: Option<String>,
    pub error: Option<String>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub height: BlockNumber,
    pub hash: String,
    pub parent_hash: String,
    pub proposer: String,
    pub block_time: i64,
}


#[derive(Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
}


impl sqd_primitives::Block for Block {
    fn number(&self) -> BlockNumber {
        self.header.height
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.header.height.saturating_sub(1)
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }

    fn timestamp(&self) -> Option<i64> {
        Some(self.header.block_time * 1000)
    }
}
