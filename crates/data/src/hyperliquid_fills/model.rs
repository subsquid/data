use serde::Deserialize;
use sqd_primitives::{BlockNumber, ItemIndex};


pub type Bytes = String;


#[derive(Deserialize)]
pub enum Side {
    A,
    B
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    pub fill_index: ItemIndex,
    pub user: Bytes,
    pub coin: String,
    pub px: f64,
    pub sz: f64,
    pub side: Side,
    pub time: i64,
    pub start_position: f64,
    pub dir: String,
    pub closed_pnl: f64,
    pub hash: Bytes,
    pub oid: u64,
    pub crossed: bool,
    pub fee: f64,
    pub builder_fee: Option<f64>,
    pub tid: u64,
    pub cloid: Option<Bytes>,
    pub fee_token: String,
    pub builder: Option<Bytes>,
    pub twap_id: Option<u64>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: Bytes,
    pub parent_hash: Bytes,
    pub timestamp: i64,
}


#[derive(Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub fills: Vec<Fill>,
}


impl sqd_primitives::Block for Block {
    fn number(&self) -> BlockNumber {
        self.header.number
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.header.number.saturating_sub(1)
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }

    fn timestamp(&self) -> Option<i64> {
        Some(self.header.timestamp)
    }
}
