use crate::types::{HexBytes, JsonValue};
use serde::Deserialize;
use sqd_primitives::{BlockNumber, ItemIndex};


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub height: BlockNumber,
    pub hash: HexBytes,
    pub parent_hash: HexBytes,
    pub tx_trie_root: HexBytes,
    pub version: Option<i32>,
    pub timestamp: i64,
    pub witness_address: HexBytes,
    pub witness_signature: Option<HexBytes>,
}


#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionResult {
    pub contract_ret: Option<String>, // eg. "SUCCESS", "REVERT", etc.
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_index: ItemIndex,
    pub hash: HexBytes,
    pub ret: Option<Vec<TransactionResult>>,
    pub signature: Option<Vec<HexBytes>>,
    #[serde(rename = "type")]
    pub r#type: String,
    pub parameter: JsonValue,
    pub permission_id: Option<i32>,
    pub ref_block_bytes: Option<HexBytes>,
    pub ref_block_hash: Option<HexBytes>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub fee_limit: Option<u64>,
    pub expiration: Option<i64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub timestamp: Option<i64>,
    pub raw_data_hex: HexBytes,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub fee: Option<u64>,
    pub contract_result: Option<HexBytes>,
    pub contract_address: Option<HexBytes>,
    pub res_message: Option<HexBytes>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub withdraw_amount: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub unfreeze_amount: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub withdraw_expire_amount: Option<u64>,
    pub cancel_unfreeze_v2_amount: Option<JsonValue>,
    pub result: Option<String>, // Result from receipt, eg. "SUCCESS", "REVERT", etc.
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub energy_fee: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub energy_usage: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub energy_usage_total: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub net_usage: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub net_fee: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub origin_energy_usage: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub energy_penalty_total: Option<u64>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub transaction_index: ItemIndex,
    pub log_index: ItemIndex,
    pub address: HexBytes,
    pub data: Option<HexBytes>,
    pub topics: Option<Vec<HexBytes>>,
}


#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallValueInfo {
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub call_value: Option<u64>,
    pub token_id: Option<String>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalTransaction {
    pub transaction_index: ItemIndex,
    pub internal_transaction_index: ItemIndex,
    pub hash: HexBytes,
    pub caller_address: HexBytes,
    pub transfer_to_address: Option<HexBytes>,
    pub call_value_info: Vec<CallValueInfo>,
    pub note: HexBytes,
    pub rejected: Option<bool>,
    pub extra: Option<HexBytes>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub logs: Vec<Log>,
    pub internal_transactions: Vec<InternalTransaction>,
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
        // Tron timestamps are already in milliseconds
        Some(self.header.timestamp)
    }
}
