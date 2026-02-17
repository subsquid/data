use crate::types::HexBytes;
use serde::Deserialize;
use sqd_primitives::BlockNumber;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: HexBytes,
    pub parent_hash: HexBytes,
    pub timestamp: u32,
    pub median_time: u32,
    pub version: u32,
    pub merkle_root: HexBytes,
    pub nonce: u32,
    pub target: HexBytes,
    pub bits: HexBytes,
    pub difficulty: f64,
    pub chain_work: HexBytes,
    pub stripped_size: u64,
    pub size: u64,
    pub weight: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptSig {
    pub hex: HexBytes,
    pub asm: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptPubKey {
    pub hex: HexBytes,
    pub asm: Option<String>,
    pub desc: Option<String>,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub address: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Prevout {
    pub generated: bool,
    pub height: BlockNumber,
    pub value: f64,
    pub script_pub_key: ScriptPubKey,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionInputTx {
    pub txid: HexBytes,
    pub vout: u32,
    pub script_sig: ScriptSig,
    pub sequence: u32,
    pub tx_in_witness: Option<Vec<HexBytes>>,
    pub prevout: Option<Prevout>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionInputCoinbase {
    pub coinbase: HexBytes,
    pub sequence: u32,
    pub tx_in_witness: Option<Vec<HexBytes>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum TransactionInput {
    #[serde(rename = "tx")]
    Tx(TransactionInputTx),
    #[serde(rename = "coinbase")]
    Coinbase(TransactionInputCoinbase),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionOutput {
    pub value: f64,
    pub n: u32,
    pub script_pub_key: ScriptPubKey,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub hex: HexBytes,
    pub txid: HexBytes,
    pub hash: HexBytes,
    pub size: u64,
    pub vsize: u64,
    pub weight: u64,
    pub version: u32,
    pub locktime: u32,
    pub vin: Vec<TransactionInput>,
    pub vout: Vec<TransactionOutput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
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
        Some(self.header.timestamp as i64 * 1000)
    }
}
