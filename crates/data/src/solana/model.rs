use serde::{Deserialize, Serialize};

use sqd_primitives::{BlockNumber, ItemIndex};

use crate::types::{Base58Bytes, JsonValue};


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub hash: Base58Bytes,
    pub height: BlockNumber,
    pub slot: BlockNumber,
    pub parent_slot: BlockNumber,
    pub parent_hash: Base58Bytes,
    pub timestamp: i64,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressTableLookup {
    pub account_key: Base58Bytes,
    pub readonly_indexes: Vec<u8>,
    pub writable_indexes: Vec<u8>,
}


#[derive(Deserialize)]
pub struct LoadedAddresses {
    pub readonly: Vec<Base58Bytes>,
    pub writable: Vec<Base58Bytes>,
}


#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionVersion {
    Legacy,
    #[serde(untagged)]
    Other(u8),
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_index: ItemIndex,
    pub version: TransactionVersion,
    pub account_keys: Vec<Base58Bytes>,
    pub address_table_lookups: Vec<AddressTableLookup>,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub num_required_signatures: u8,
    pub recent_blockhash: Base58Bytes,
    pub signatures: Vec<Base58Bytes>,
    pub err: Option<JsonValue>,
    #[serde(deserialize_with="crate::types::decode_string_option")]
    pub compute_units_consumed: Option<u64>,
    #[serde(deserialize_with="crate::types::decode_string")]
    pub fee: u64,
    pub loaded_addresses: LoadedAddresses,
    pub has_dropped_log_messages: bool,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instruction {
    pub transaction_index: ItemIndex,
    pub instruction_address: Vec<ItemIndex>,
    pub program_id: Base58Bytes,
    pub accounts: Vec<Base58Bytes>,
    pub data: Base58Bytes,
    #[serde(deserialize_with="crate::types::decode_string_option")]
    pub compute_units_consumed: Option<u64>,
    pub error: Option<JsonValue>,
    pub is_committed: bool,
    pub has_dropped_log_messages: bool,
}


#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogMessageKind {
    Log,
    Data,
    Other,
}


impl LogMessageKind {
    pub fn to_str(&self) -> &'static str {
        match self {
            LogMessageKind::Log => "log",
            LogMessageKind::Data => "data",
            LogMessageKind::Other => "other"
        }
    }
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogMessage {
    pub transaction_index: ItemIndex,
    pub log_index: ItemIndex,
    pub instruction_address: Vec<ItemIndex>,
    pub program_id: Base58Bytes,
    pub kind: LogMessageKind,
    pub message: String,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub transaction_index: ItemIndex,
    pub account: Base58Bytes,
    #[serde(deserialize_with="crate::types::decode_string")]
    pub pre: u64,
    #[serde(deserialize_with="crate::types::decode_string")]
    pub post: u64,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub transaction_index: ItemIndex,
    pub account: Base58Bytes,
    pub pre_mint: Option<Base58Bytes>,
    pub post_mint: Option<Base58Bytes>,
    pub pre_decimals: Option<u16>,
    pub post_decimals: Option<u16>,
    pub pre_program_id: Option<Base58Bytes>,
    pub post_program_id: Option<Base58Bytes>,
    pub pre_owner: Option<Base58Bytes>,
    pub post_owner: Option<Base58Bytes>,
    #[serde(deserialize_with="crate::types::decode_string_option")]
    pub pre_amount: Option<u64>,
    #[serde(deserialize_with="crate::types::decode_string_option")]
    pub post_amount: Option<u64>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reward {
    pub pubkey: Base58Bytes,
    #[serde(deserialize_with="crate::types::decode_string")]
    pub lamports: i64,
    #[serde(deserialize_with="crate::types::decode_string")]
    pub post_balance: u64,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub instructions: Vec<Instruction>,
    pub logs: Vec<LogMessage>,
    pub balances: Vec<Balance>,
    pub token_balances: Vec<TokenBalance>,
    pub rewards: Vec<Reward>,
}