use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sqd_primitives::{BlockNumber, ItemIndex};
use crate::types::{Base58Bytes, JSON, StringEncoded};


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub hash: Base58Bytes,
    pub height: BlockNumber,
    pub slot: BlockNumber,
    pub parent_slot: BlockNumber,
    pub parent_hash: Base58Bytes,
    pub timestamp: i64,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressTableLookup {
    pub account_key: Base58Bytes,
    pub readonly_indexes: Vec<u8>,
    pub writable_indexes: Vec<u8>,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
pub struct LoadedAddresses {
    pub readonly: Vec<Base58Bytes>,
    pub writable: Vec<Base58Bytes>,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionVersion {
    Legacy,
    #[serde(untagged)]
    Other(u8),
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub transaction_index: ItemIndex,
    pub version: TransactionVersion,
    pub account_keys: Vec<Base58Bytes>,
    pub address_table_lookups: Vec<AddressTableLookup>,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub num_required_signatures: u8,
    pub recent_blockhash: Base58Bytes,
    pub signatures: Vec<Base58Bytes>,
    pub err: Option<JSON>,
    pub compute_units_consumed: Option<StringEncoded<u64>>,
    pub fee: StringEncoded<u64>,
    pub loaded_addresses: LoadedAddresses,
    pub has_dropped_log_messages: bool,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instruction {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub transaction_index: ItemIndex,
    pub instruction_address: Vec<ItemIndex>,
    pub program_id: Base58Bytes,
    pub accounts: Vec<Base58Bytes>,
    pub data: Base58Bytes,
    pub compute_units_consumed: Option<StringEncoded<u64>>,
    pub error: Option<JSON>,
    pub is_committed: bool,
    pub has_dropped_log_messages: bool,
}


#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogMessageKind {
    Log,
    Data,
    Other,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogMessage {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub transaction_index: ItemIndex,
    pub log_index: ItemIndex,
    pub instruction_address: Vec<ItemIndex>,
    pub program_id: Base58Bytes,
    pub kind: LogMessageKind,
    pub message: String,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub transaction_index: ItemIndex,
    pub account: Base58Bytes,
    pub pre: StringEncoded<u64>,
    pub post: StringEncoded<u64>,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    #[serde(default)]
    pub block_number: BlockNumber,
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
    pub pre_amount: Option<StringEncoded<u64>>,
    pub post_amount: Option<StringEncoded<u64>>,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reward {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub pubkey: Base58Bytes,
    pub lamports: StringEncoded<i64>,
    pub post_balance: StringEncoded<u64>,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}


#[derive(Deserialize, BorshSerialize, BorshDeserialize)]
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