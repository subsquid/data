use serde::{Deserialize, Serialize};
use sqd_primitives::{BlockNumber, ItemIndex};


#[derive(Deserialize, Serialize)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u64,
}


#[derive(Deserialize, Serialize)]
pub struct ActionData {
    pub r#type: String,
    #[serde(flatten)]
    pub data: serde_json::Value, // type-specific data
}


#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Err,
}


#[derive(Deserialize, Serialize)]
pub struct HardforkInfo {
    pub version: u64,
    pub round: u64,
}


#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Action {
    pub action_index: ItemIndex,
    pub signature: Signature,
    pub action: ActionData,
    pub nonce: u64,
    pub vault_address: Option<String>,
    pub user: Option<String>,
    pub status: Status,
    pub response: serde_json::Value,
}


#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub height: BlockNumber,
    pub hash: String,
    pub parent_hash: String,
    pub round: u64,
    pub parent_round: u64,
    pub proposer: String,
    pub timestamp: i64,
    pub hardfork: HardforkInfo,
}


#[derive(Deserialize, Serialize)]
pub struct Block {
    pub header: BlockHeader,
    pub actions: Vec<Action>,
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
        Some(self.header.timestamp)
    }
}
