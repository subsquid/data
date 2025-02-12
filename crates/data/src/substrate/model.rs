use crate::types::{Bytes, JsonValue};
use serde::{Deserialize, Serialize};
use sqd_primitives::{BlockNumber, ItemIndex};


#[derive(Deserialize, Serialize)]
pub struct Digest {
    pub logs: Vec<Bytes>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub hash: Bytes,
    pub parent_hash: Bytes,
    pub height: BlockNumber,
    pub state_root: Bytes,
    pub extrinsics_root: Bytes,
    pub digest: Digest,
    pub spec_name: String,
    pub spec_version: u32,
    pub impl_name: String,
    pub impl_version: u32,
    pub timestamp: Option<i64>,
    pub validator: Option<Bytes>,
}


#[derive(Deserialize)]
pub struct Extrinsic {
    pub index: ItemIndex,
    pub version: u32,
    pub signature: Option<JsonValue>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub fee: Option<u128>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub tip: Option<u128>,
    pub error: Option<JsonValue>,
    pub success: bool,
    pub hash: Bytes,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Call {
    pub extrinsic_index: ItemIndex,
    pub address: Vec<ItemIndex>,
    pub name: String,
    pub args: JsonValue,
    pub origin: Option<JsonValue>,
    pub error: Option<JsonValue>,
    pub success: bool,
    pub _ethereum_transact_to: Option<Bytes>,
    pub _ethereum_transact_sighash: Option<Bytes>,
}


#[derive(Deserialize, Serialize)]
pub enum Phase {
    Initialization,
    ApplyExtrinsic,
    Finalization,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub index: ItemIndex,
    pub name: String,
    pub args: JsonValue,
    pub phase: Phase,
    pub extrinsic_index: ItemIndex,
    pub call_address: Option<Vec<ItemIndex>>,
    pub topics: Vec<Bytes>,
    pub _evm_log_address: Option<Bytes>,
    pub _evm_log_topics: Option<Vec<Bytes>>,
    pub _contract_address: Option<Bytes>,
    pub _gear_program_id: Option<Bytes>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
    pub extrinsics: Vec<Extrinsic>,
    pub calls: Vec<Call>,
    pub events: Vec<Event>,
}


impl sqd_primitives::Block for Block {
    fn number(&self) -> BlockNumber {
        self.header.height
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.number().saturating_sub(1)
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }
}
