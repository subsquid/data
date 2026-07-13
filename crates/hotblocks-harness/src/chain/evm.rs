//! `evm`: 14 required header fields, one required item collection and three optional ones whose
//! presence is the block's *data-availability mask*.
//!
//! Two traps this kind sets, both of which produce failures that look like service bugs:
//! its `timestamp` is in **seconds** (the service scales it), and a transaction's `nonce` is a
//! plain number while every other numeric-looking field is a hex string.

use serde_json::{Value, json};

use super::Chain;
use crate::types::{Block, BlockNumber, block_hash};

pub struct Evm;

const MINER: &str = "0x1111111111111111111111111111111111111111";
const SENDER: &str = "0x2222222222222222222222222222222222222222";
const CONTRACT: &str = "0x3333333333333333333333333333333333333333";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
/// The canonical keccak of an empty uncle list.
const EMPTY_UNCLES: &str = "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347";

/// Every other block carries a transaction, so a window mixes blocks that emit nothing at all
/// with blocks that emit a transaction and up to two logs.
fn tx_count(b: &Block) -> u32 {
    (b.number % 2) as u32
}

fn log_count(b: &Block) -> u32 {
    if tx_count(b) == 0 { 0 } else { (b.number % 3) as u32 }
}

/// Distinct from any block hash: block numbers never reach this domain.
fn tx_hash(b: &Block, index: u32) -> String {
    block_hash(
        b.number.wrapping_mul(1_000_003).wrapping_add(u64::from(index)),
        b.fork_id
    )
}

impl Evm {
    fn header(b: &Block) -> Value {
        json!({
            "number": b.number,
            "hash": b.hash,
            "parentHash": b.parent_hash,
            // Seconds. `Block::timestamp()` multiplies by 1000 on the way in, and the query
            // renders the stored column back as seconds — so this is what the oracle predicts.
            "timestamp": b.timestamp_ms / 1000
        })
    }

    fn source_header(b: &Block) -> Value {
        let mut header = Self::header(b);
        let o = header.as_object_mut().expect("header is an object");
        for (key, value) in [
            ("transactionsRoot", json!(block_hash(b.number, b.fork_id ^ 0x11))),
            ("receiptsRoot", json!(block_hash(b.number, b.fork_id ^ 0x22))),
            ("stateRoot", json!(block_hash(b.number, b.fork_id ^ 0x33))),
            ("logsBloom", json!("0x00")),
            ("sha3Uncles", json!(EMPTY_UNCLES)),
            ("extraData", json!("0x")),
            ("miner", json!(MINER)),
            ("size", json!(1_000 + b.number % 100)),
            ("gasLimit", json!("0x1c9c380")),
            ("gasUsed", json!("0x5208"))
        ] {
            o.insert(key.into(), value);
        }
        header
    }

    fn projected_tx(b: &Block, index: u32) -> Value {
        json!({
            "transactionIndex": index,
            "hash": tx_hash(b, index),
            "from": SENDER,
            // A plain number here, unlike every other numeric field of a transaction.
            "nonce": b.number
        })
    }

    fn source_tx(b: &Block, index: u32) -> Value {
        let mut tx = Self::projected_tx(b, index);
        let o = tx.as_object_mut().expect("transaction is an object");
        for (key, value) in [
            ("gas", json!("0x5208")),
            ("cumulativeGasUsed", json!("0x5208")),
            ("gasUsed", json!("0x5208")),
            ("logsBloom", json!("0x00"))
        ] {
            o.insert(key.into(), value);
        }
        tx
    }

    fn projected_log(b: &Block, index: u32) -> Value {
        json!({
            "logIndex": index,
            "transactionIndex": 0,
            "address": CONTRACT,
            "topics": [TRANSFER_TOPIC, block_hash(b.number, b.fork_id ^ (0x40 + index))],
            "data": "0x"
        })
    }

    fn source_log(b: &Block, index: u32) -> Value {
        let mut log = Self::projected_log(b, index);
        log.as_object_mut()
            .expect("log is an object")
            .insert("transactionHash".into(), json!(tx_hash(b, 0)));
        log
    }
}

impl Chain for Evm {
    fn config_kind(&self) -> &'static str {
        "evm"
    }

    fn storage_kind(&self) -> &'static str {
        "evm"
    }

    fn dialect(&self) -> &'static str {
        "evm"
    }

    fn source_block(&self, b: &Block) -> Value {
        json!({
            "header": Self::source_header(b),
            "transactions": (0..tx_count(b)).map(|i| Self::source_tx(b, i)).collect::<Vec<_>>(),
            // Presence, not content, is what the data-availability mask records: a block with
            // `logs: []` and one with no `logs` key carry *different* masks, and the service
            // cuts a chunk wherever the mask changes. Every block here declares all three, so
            // the mask is constant and chunk boundaries stay driven by the batch, not the shape.
            "logs": (0..log_count(b)).map(|i| Self::source_log(b, i)).collect::<Vec<_>>(),
            "traces": [],
            "stateDiffs": []
        })
    }

    fn scan_query(&self, from: BlockNumber, to: Option<BlockNumber>, expected_parent: Option<&str>) -> Value {
        super::scan_query(
            self.dialect(),
            from,
            to,
            expected_parent,
            json!({
                "block": {"number": true, "hash": true, "parentHash": true, "timestamp": true},
                "transaction": {"transactionIndex": true, "hash": true, "from": true, "nonce": true},
                "log": {"logIndex": true, "transactionIndex": true, "address": true, "topics": true, "data": true}
            }),
            vec![("transactions", json!([{}])), ("logs", json!([{}]))]
        )
    }

    fn expected_emission(&self, b: &Block) -> Value {
        super::emission(
            Self::header(b),
            vec![
                (
                    "transactions",
                    (0..tx_count(b)).map(|i| Self::projected_tx(b, i)).collect()
                ),
                ("logs", (0..log_count(b)).map(|i| Self::projected_log(b, i)).collect()),
            ]
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: BlockNumber) -> Block {
        Block {
            number,
            hash: block_hash(number, 0),
            parent_number: number - 1,
            parent_hash: block_hash(number - 1, 0),
            timestamp_ms: 1_760_000_000_000 + number as i64 * 1000,
            fork_id: 0
        }
    }

    #[test]
    fn the_source_serves_seconds_and_the_oracle_predicts_seconds() {
        let b = block(1_001);
        let source = Evm.source_block(&b);
        let expected = Evm.expected_emission(&b);
        assert_eq!(source["header"]["timestamp"], expected["header"]["timestamp"]);
        assert_eq!(source["header"]["timestamp"].as_i64().unwrap(), b.timestamp_ms / 1000);
    }

    #[test]
    fn the_emission_oracle_is_the_source_payload_projected() {
        let b = block(1_001); // odd → 1 transaction; 1001 % 3 == 2 → 2 logs
        let source = Evm.source_block(&b);
        let expected = Evm.expected_emission(&b);

        assert_eq!(expected["transactions"].as_array().unwrap().len(), 1);
        assert_eq!(expected["logs"].as_array().unwrap().len(), 2);

        for collection in ["transactions", "logs"] {
            for (src, exp) in source[collection]
                .as_array()
                .unwrap()
                .iter()
                .zip(expected[collection].as_array().unwrap())
            {
                for (key, value) in exp.as_object().unwrap() {
                    assert_eq!(&src[key], value, "{collection}.{key} must come from the served payload");
                }
            }
        }
    }

    #[test]
    fn a_block_with_no_items_emits_a_bare_header() {
        let b = block(1_000); // even → no transactions, and therefore no logs
        let emitted = Evm.expected_emission(&b);
        assert_eq!(emitted.as_object().unwrap().len(), 1);
        assert!(emitted.get("transactions").is_none());
    }

    /// The mask is derived from *presence*, so it must not drift block to block.
    #[test]
    fn every_block_declares_the_same_optional_collections() {
        for number in 1_000..1_010 {
            let source = Evm.source_block(&block(number));
            for collection in ["logs", "traces", "stateDiffs"] {
                assert!(source[collection].is_array(), "block {number} dropped {collection}");
            }
        }
    }
}
