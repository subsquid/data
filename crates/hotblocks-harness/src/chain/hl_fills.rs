//! `hyperliquid-fills`: one header, one flat item collection, no nesting.

use serde_json::{Value, json};

use super::Chain;
use crate::types::{Block, BlockNumber};

pub struct HlFills;

const USERS: [&str; 2] = [
    "0xcf0a36dec06e90263288100c11cf69828338e826",
    "0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00"
];
const COINS: [&str; 2] = ["BTC", "ETH"];

/// `number % 3` fills, so a window mixes empty and non-empty blocks — the filter-sparse shape
/// CT-5 (GAP-8) will lean on.
fn fill_count(b: &Block) -> u32 {
    (b.number % 3) as u32
}

fn fill_seed(b: &Block, i: u32) -> u64 {
    b.number
        .wrapping_mul(7)
        .wrapping_add(u64::from(i))
        .wrapping_add(u64::from(b.fork_id).wrapping_mul(1_000_003))
}

impl HlFills {
    /// The fields a scan projects — the same set the emission oracle predicts.
    fn projected_fill(b: &Block, i: u32) -> Value {
        let s = fill_seed(b, i);
        json!({
            "fillIndex": i,
            "user": USERS[(s % 2) as usize],
            "coin": COINS[(s % 2) as usize],
            // Quarters are exact in binary floating point: source → parquet → response
            // round-trips compare by equality, no epsilon.
            "px": 100.0 + (s % 8) as f64 * 0.25,
            "sz": 0.25 * ((s % 4) + 1) as f64,
            "side": if s % 2 == 0 { "B" } else { "A" }
        })
    }

    fn source_fill(b: &Block, i: u32) -> Value {
        let s = fill_seed(b, i);
        let mut fill = Self::projected_fill(b, i);
        let o = fill.as_object_mut().expect("fill is an object");
        o.insert("time".into(), json!(b.timestamp_ms));
        o.insert("startPosition".into(), json!(0.0));
        o.insert("dir".into(), json!(if s % 2 == 0 { "Open Long" } else { "Open Short" }));
        o.insert("closedPnl".into(), json!(0.0));
        o.insert("hash".into(), json!(format!("{}{:02x}", &b.hash[..64], i % 256)));
        o.insert("oid".into(), json!(s));
        o.insert("crossed".into(), json!(s % 2 == 0));
        o.insert("fee".into(), json!(0.25));
        o.insert("tid".into(), json!(s.wrapping_mul(31)));
        o.insert("feeToken".into(), json!("USDC"));
        fill
    }

    fn header(b: &Block) -> Value {
        json!({
            "number": b.number,
            "hash": b.hash,
            "parentHash": b.parent_hash,
            // Milliseconds at the source for this kind; evm/bitcoin serve seconds.
            "timestamp": b.timestamp_ms
        })
    }
}

impl Chain for HlFills {
    fn config_kind(&self) -> &'static str {
        "hyperliquid-fills"
    }

    fn storage_kind(&self) -> &'static str {
        "hl-fills"
    }

    fn dialect(&self) -> &'static str {
        "hyperliquidFills"
    }

    fn source_block(&self, b: &Block) -> Value {
        json!({
            "header": Self::header(b),
            "fills": (0..fill_count(b)).map(|i| Self::source_fill(b, i)).collect::<Vec<_>>()
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
                "fill": {"fillIndex": true, "user": true, "coin": true, "px": true, "sz": true, "side": true}
            }),
            // No predicates — selects every fill, so the scan checks payload provenance (INV-7).
            vec![("fills", json!([{}]))]
        )
    }

    fn expected_emission(&self, b: &Block) -> Value {
        let fills = (0..fill_count(b)).map(|i| Self::projected_fill(b, i)).collect();
        super::emission(Self::header(b), vec![("fills", fills)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::block_hash;

    fn block(number: BlockNumber, fork_id: u32) -> Block {
        Block {
            number,
            hash: block_hash(number, fork_id),
            parent_number: number - 1,
            parent_hash: block_hash(number - 1, fork_id),
            timestamp_ms: 1_700_000_000_000 + number as i64 * 1000,
            fork_id
        }
    }

    #[test]
    fn the_emission_oracle_is_the_source_payload_projected() {
        let b = block(101, 0); // 101 % 3 == 2 fills
        let source = HlFills.source_block(&b);
        let expected = HlFills.expected_emission(&b);

        let source_fills = source["fills"].as_array().unwrap();
        let expected_fills = expected["fills"].as_array().unwrap();
        assert_eq!(source_fills.len(), 2);
        assert_eq!(expected_fills.len(), 2);

        for (src, exp) in source_fills.iter().zip(expected_fills) {
            for (k, v) in exp.as_object().unwrap() {
                assert_eq!(&src[k], v, "projected field {k} must come from the served payload");
            }
        }
        assert_eq!(source["header"], expected["header"]);
    }

    #[test]
    fn a_fork_changes_the_payload_at_the_same_height() {
        let a = HlFills.source_block(&block(101, 0));
        let b = HlFills.source_block(&block(101, 1));
        assert_ne!(
            a, b,
            "a replaced block must not be payload-identical to the one it replaced"
        );
    }

    #[test]
    fn empty_blocks_emit_a_bare_header() {
        let b = block(102, 0); // 102 % 3 == 0
        assert_eq!(HlFills.expected_emission(&b).get("fills"), None);
        assert_eq!(HlFills.source_block(&b)["fills"].as_array().unwrap().len(), 0);
    }
}
