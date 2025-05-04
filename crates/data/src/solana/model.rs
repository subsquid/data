use crate::types::{Base58Bytes, JsonValue};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use sqd_primitives::{BlockNumber, ItemIndex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};


pub type AccountIndex = u32;


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: Base58Bytes,
    pub parent_number: BlockNumber,
    pub parent_hash: Base58Bytes,
    pub height: BlockNumber,
    pub timestamp: i64,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressTableLookup {
    pub account_key: AccountIndex,
    pub readonly_indexes: Vec<u8>,
    pub writable_indexes: Vec<u8>,
}


#[derive(Deserialize)]
pub struct LoadedAddresses {
    pub readonly: Vec<AccountIndex>,
    pub writable: Vec<AccountIndex>,
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
    pub account_keys: Vec<AccountIndex>,
    pub address_table_lookups: Vec<AddressTableLookup>,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub num_required_signatures: u8,
    pub recent_blockhash: Base58Bytes,
    pub signatures: Vec<Base58Bytes>,
    pub err: Option<JsonValue>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string")]
    pub fee: u64,
    pub loaded_addresses: LoadedAddresses,
    pub has_dropped_log_messages: bool,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Instruction {
    pub transaction_index: ItemIndex,
    pub instruction_address: Vec<ItemIndex>,
    pub program_id: AccountIndex,
    pub accounts: Vec<AccountIndex>,
    pub data: Base58Bytes,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(default)]
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
    pub program_id: AccountIndex,
    pub kind: LogMessageKind,
    pub message: String,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub transaction_index: ItemIndex,
    pub account: AccountIndex,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string")]
    pub pre: u64,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string")]
    pub post: u64,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub transaction_index: ItemIndex,
    pub account: AccountIndex,
    #[serde(default)]
    pub pre_mint: Option<AccountIndex>,
    #[serde(default)]
    pub post_mint: Option<AccountIndex>,
    #[serde(default)]
    pub pre_decimals: Option<u16>,
    #[serde(default)]
    pub post_decimals: Option<u16>,
    #[serde(default)]
    pub pre_program_id: Option<AccountIndex>,
    #[serde(default)]
    pub post_program_id: Option<AccountIndex>,
    #[serde(default)]
    pub pre_owner: Option<AccountIndex>,
    #[serde(default)]
    pub post_owner: Option<AccountIndex>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub pre_amount: Option<u64>,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string_option", default)]
    pub post_amount: Option<u64>,
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reward {
    pub pubkey: AccountIndex,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string")]
    pub lamports: i64,
    #[serde(deserialize_with="sqd_data_core::serde::decode_string")]
    pub post_balance: u64,
    #[serde(default)]
    pub reward_type: Option<String>,
    #[serde(default)]
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
    pub accounts: Vec<Base58Bytes>
}


impl Block {
    pub fn get_account(&self, idx: AccountIndex) -> anyhow::Result<&str> {
        self.accounts.get(idx as usize).map(|s| s.as_str()).ok_or_else(|| {
            anyhow!(
                "invalid account reference {} in block {}#{}", 
                idx, 
                self.header.number, 
                self.header.hash
            )
        })
    }
}


impl sqd_primitives::Block for Block {
    fn number(&self) -> BlockNumber {
        self.header.number
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.header.parent_number
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }

    fn timestamp(&self) -> Option<SystemTime> {
        let ts = self.header.timestamp;
        if ts < 0 {
            UNIX_EPOCH.checked_sub(Duration::from_secs(ts.abs() as u64))
        } else {
            UNIX_EPOCH.checked_add(Duration::from_secs(ts as u64))
        }
    }
}