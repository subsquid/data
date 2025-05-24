use crate::types::HexBytes;
use serde::Deserialize;
use sqd_primitives::{BlockNumber, ItemIndex};


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: HexBytes,
    pub parent_hash: HexBytes,
    pub timestamp: i64,
    pub transactions_root: HexBytes,
    pub receipts_root: HexBytes,
    pub state_root: HexBytes,
    pub logs_bloom: HexBytes,
    pub sha3_uncles: HexBytes,
    pub extra_data: HexBytes,
    pub miner: HexBytes,
    pub nonce: Option<HexBytes>,
    pub mix_hash: Option<HexBytes>,
    pub size: u64,
    pub gas_limit: HexBytes,
    pub gas_used: HexBytes,
    pub difficulty: Option<HexBytes>,
    pub total_difficulty: Option<HexBytes>,
    pub base_fee_per_gas: Option<HexBytes>,
    pub blob_gas_used: Option<HexBytes>,
    pub excess_blob_gas: Option<HexBytes>,
    pub l1_block_number: Option<BlockNumber>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EIP7702Authorization {
    pub chain_id: u64,
    pub address: HexBytes,
    pub nonce: u64,
    pub y_parity: u8,
    pub r: HexBytes,
    pub s: HexBytes,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_index: ItemIndex,
    pub hash: HexBytes,
    pub nonce: u64,
    pub from: HexBytes,
    pub to: Option<HexBytes>,
    pub input: HexBytes,
    pub value: HexBytes,
    #[serde(rename = "type")]
    pub r#type: Option<u64>,
    pub gas: HexBytes,
    pub gas_price: Option<HexBytes>,
    pub max_fee_per_gas: Option<HexBytes>,
    pub max_priority_fee_per_gas: Option<HexBytes>,
    pub v: Option<HexBytes>,
    pub r: Option<HexBytes>,
    pub s: Option<HexBytes>,
    pub y_parity: Option<u8>,
    pub chain_id: Option<u64>,
    pub max_fee_per_blob_gas: Option<HexBytes>,

    pub blob_versioned_hashes: Option<Vec<HexBytes>>,
    pub authorization_list: Option<Vec<EIP7702Authorization>>,

    pub contract_address: Option<HexBytes>,
    pub cumulative_gas_used: HexBytes,
    pub effective_gas_price: HexBytes,
    pub gas_used: HexBytes,
    pub status: Option<u8>,

    pub l1_base_fee_scalar: Option<u64>,
    pub l1_blob_base_fee: Option<HexBytes>,
    pub l1_blob_base_fee_scalar: Option<u64>,
    pub l1_fee: Option<HexBytes>,
    pub l1_fee_scalar: Option<u64>,
    pub l1_gas_price: Option<HexBytes>,
    pub l1_gas_used: Option<HexBytes>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub log_index: ItemIndex,
    pub transaction_index: ItemIndex,
    pub transaction_hash: HexBytes,
    pub address: HexBytes,
    pub data: HexBytes,
    pub topics: Vec<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionCreate {
    pub from: HexBytes,
    pub value: Option<HexBytes>,
    pub gas: HexBytes,
    pub init: HexBytes,
    pub creation_method: Option<String>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionCall {
    pub from: HexBytes,
    pub to: HexBytes,
    pub value: Option<HexBytes>,
    pub gas: HexBytes,
    pub input: HexBytes,
    pub call_type: String,
    pub r#type: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionReward {
    pub author: HexBytes,
    pub value: u64,
    pub reward_type: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceActionSelfDestruct {
    pub address: HexBytes,
    pub refund_address: HexBytes,
    pub balance: u64
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResultCreate {
    pub gas_used: HexBytes,
    pub code: HexBytes,
    pub address: HexBytes,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResultCall {
    pub gas_used: HexBytes,
    pub output: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub transaction_index: u32,
    pub trace_address: Vec<u32>,
    pub subtraces: u32,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    #[serde(flatten)]
    pub op: TraceOp
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum TraceOp {
    #[serde(rename = "create")]
    Create {
        action: TraceActionCreate,
        result: Option<TraceResultCreate>
    },
    #[serde(rename = "call")]
    Call {
        action: TraceActionCall,
        result: Option<TraceResultCall>
    },
    #[serde(rename = "selfdestruct")]
    SelfDestruct {
        action: TraceActionSelfDestruct
    },
    #[serde(rename = "reward")]
    Reward {
        action: TraceActionReward
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDiff {
    pub transaction_index: ItemIndex,
    pub address: HexBytes,
    pub key: String,
    pub kind: String,
    pub prev: Option<HexBytes>,
    pub next: Option<HexBytes>
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub logs: Option<Vec<Log>>,
    pub traces: Option<Vec<Trace>>,
    pub state_diffs: Option<Vec<StateDiff>>,
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
}