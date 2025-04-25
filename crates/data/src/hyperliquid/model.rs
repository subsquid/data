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
        unimplemented!()
    }

    fn parent_hash(&self) -> &str {
        unimplemented!()
    }
}
