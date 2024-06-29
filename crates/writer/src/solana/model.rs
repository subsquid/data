use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode, impl_borrow_decode};
use bincode::enc::Encoder;
use bincode::de::Decoder;
use bincode::error::{EncodeError, DecodeError};

use crate::primitives::{BlockNumber, ItemIndex};


pub type Base58Bytes = String;
pub type JsBigInt = String; // FIXME: implement custom SerDe


#[derive(Serialize, Deserialize)]
pub struct JSON(serde_json::Value);


impl Encode for JSON {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let value = serde_json::to_string(&self.0).unwrap();
        Encode::encode(&value, encoder)?;
        Ok(())
    }
}


impl Decode for JSON {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let value: String = Decode::decode(decoder)?;
        let json = JSON(serde_json::from_str(&value).unwrap());
        Ok(json)
    }
}


impl_borrow_decode!(JSON);


#[derive(Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub hash: Base58Bytes,
    pub height: BlockNumber,
    pub slot: BlockNumber,
    pub parent_slot: BlockNumber,
    pub parent_hash: Base58Bytes,
    pub timestamp: i64,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
pub struct AddressTableLookup {
    pub account_key: Base58Bytes,
    pub readonly_indexes: Vec<u8>,
    pub writable_indexes: Vec<u8>,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
pub struct LoadedAddresses {
    pub readonly: Vec<Base58Bytes>,
    pub writable: Vec<Base58Bytes>,
}


#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionVersion {
    Legacy,
    #[serde(untagged)]
    Other(u8),
}


impl Encode for TransactionVersion {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let value: i16 = match self {
            TransactionVersion::Legacy => -1,
            TransactionVersion::Other(num) => (*num).into(),
        };
        Encode::encode(&value, encoder)?;
        Ok(())
    }
}


impl Decode for TransactionVersion {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let value: i16 = Decode::decode(decoder)?;
        let version = match value {
            -1 => TransactionVersion::Legacy,
            _ => TransactionVersion::Other(value.try_into().unwrap())
        };
        Ok(version)
    }
}


impl_borrow_decode!(TransactionVersion);


#[derive(Serialize, Deserialize, Encode, Decode)]
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
    pub compute_units_consumed: Option<JsBigInt>,
    pub fee: JsBigInt,
    pub loaded_addresses: LoadedAddresses,
    pub has_dropped_log_messages: bool,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
pub struct Instruction {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub transaction_index: ItemIndex,
    pub instruction_address: Vec<ItemIndex>,
    pub program_id: Base58Bytes,
    pub accounts: Vec<Base58Bytes>,
    pub data: Base58Bytes,
    pub compute_units_consumed: Option<JsBigInt>,
    pub error: Option<String>,
    pub is_committed: bool,
    pub has_dropped_log_messages: bool,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "lowercase")]
pub enum LogMessageKind {
    Log,
    Data,
    Other,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
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


#[derive(Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub transaction_index: ItemIndex,
    pub account: Base58Bytes,
    pub pre: JsBigInt,
    pub post: JsBigInt,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
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
    pub pre_amount: Option<JsBigInt>,
    pub post_amount: Option<JsBigInt>,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "camelCase")]
pub struct Reward {
    #[serde(default)]
    pub block_number: BlockNumber,
    pub pubkey: Base58Bytes,
    pub lamports: JsBigInt,
    pub post_balance: JsBigInt,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}


#[derive(Serialize, Deserialize, Encode, Decode)]
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