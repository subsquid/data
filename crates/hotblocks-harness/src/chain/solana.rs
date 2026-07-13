//! `solana`: the kind that makes the sparse-numbering case real. Blocks are numbered by
//! time-based **slots**, so `number - parent_number` is not always 1, and the header carries
//! `parentNumber` explicitly.
//!
//! Traps: every one of the eight collections must be present (an empty array, never an absent
//! key); a reward's `lamports` is a JSON *string*; and its `pubkey` is an *index* into the
//! block's `accounts`, which the service resolves — so the emission carries the account, not the
//! index.

use serde_json::{Value, json};

use super::Chain;
use crate::types::{Block, BlockNumber};

pub struct Solana;

const ACCOUNTS: [&str; 2] = [
    "11111111111111111111111111111111",
    "Vote111111111111111111111111111111111111111"
];

/// `number % 3` rewards, so a window mixes empty and non-empty blocks.
fn reward_count(b: &Block) -> u32 {
    (b.number % 3) as u32
}

fn lamports(b: &Block, index: u32) -> i64 {
    (b.number as i64).wrapping_mul(7) + i64::from(index) + i64::from(b.fork_id) * 1_000_003
}

impl Solana {
    fn header(b: &Block) -> Value {
        json!({
            "number": b.number,
            "hash": b.hash,
            "parentNumber": b.parent_number,
            "parentHash": b.parent_hash,
            // Block height is informational here: nothing in the write path reads it, and the
            // harness does not model it apart from the slot.
            "height": b.number,
            // Seconds, like evm.
            "timestamp": b.timestamp_ms / 1000
        })
    }

    /// What the service emits: `pubkey` resolved from the index to the account itself.
    fn projected_reward(b: &Block, index: u32) -> Value {
        json!({
            "pubkey": ACCOUNTS[(index % 2) as usize],
            "lamports": lamports(b, index).to_string(),
            "postBalance": (lamports(b, index) + 1_000).to_string()
        })
    }

    fn source_reward(b: &Block, index: u32) -> Value {
        json!({
            "pubkey": index % 2,
            "lamports": lamports(b, index).to_string(),
            "postBalance": (lamports(b, index) + 1_000).to_string()
        })
    }
}

impl Chain for Solana {
    fn config_kind(&self) -> &'static str {
        "solana"
    }

    fn storage_kind(&self) -> &'static str {
        "solana"
    }

    fn dialect(&self) -> &'static str {
        "solana"
    }

    fn source_block(&self, b: &Block) -> Value {
        json!({
            "header": Self::header(b),
            "transactions": [],
            "instructions": [],
            "logs": [],
            "balances": [],
            "tokenBalances": [],
            "rewards": (0..reward_count(b)).map(|i| Self::source_reward(b, i)).collect::<Vec<_>>(),
            "accounts": ACCOUNTS
        })
    }

    fn scan_query(&self, from: BlockNumber, to: Option<BlockNumber>, expected_parent: Option<&str>) -> Value {
        super::scan_query(
            self.dialect(),
            from,
            to,
            expected_parent,
            json!({
                "block": {
                    "number": true, "hash": true, "parentNumber": true,
                    "parentHash": true, "height": true, "timestamp": true
                },
                "reward": {"pubkey": true, "lamports": true, "postBalance": true}
            }),
            vec![("rewards", json!([{}]))]
        )
    }

    fn expected_emission(&self, b: &Block) -> Value {
        let rewards = (0..reward_count(b)).map(|i| Self::projected_reward(b, i)).collect();
        super::emission(Self::header(b), vec![("rewards", rewards)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::block_hash;

    /// Slot 1_003, whose parent is slot 1_000 — three slots produced nothing.
    fn sparse_block(number: BlockNumber, parent_number: BlockNumber) -> Block {
        Block {
            number,
            hash: block_hash(number, 0),
            parent_number,
            parent_hash: block_hash(parent_number, 0),
            timestamp_ms: 1_760_000_000_000 + number as i64 * 1000,
            fork_id: 0
        }
    }

    #[test]
    fn the_header_carries_the_parent_slot_across_a_hole() {
        let b = sparse_block(1_003, 1_000);
        let header = &Solana.source_block(&b)["header"];
        assert_eq!(header["number"], 1_003);
        assert_eq!(header["parentNumber"], 1_000, "the hole is explicit in the payload");
        assert_eq!(Solana.expected_emission(&b)["header"], *header);
    }

    #[test]
    fn every_collection_is_present_even_when_empty() {
        let source = Solana.source_block(&sparse_block(1_002, 1_000)); // 1002 % 3 == 0 rewards
        for collection in [
            "transactions",
            "instructions",
            "logs",
            "balances",
            "tokenBalances",
            "rewards",
            "accounts"
        ] {
            assert!(source[collection].is_array(), "{collection} must be present");
        }
        assert!(
            Solana
                .expected_emission(&sparse_block(1_002, 1_000))
                .get("rewards")
                .is_none()
        );
    }

    /// The source sends an index; the service stores the account it resolves to.
    #[test]
    fn a_reward_pubkey_is_an_index_at_the_source_and_an_account_in_the_emission() {
        let b = sparse_block(1_004, 1_003); // 1004 % 3 == 2 rewards
        let source = Solana.source_block(&b);
        let emitted = Solana.expected_emission(&b);

        let source_rewards = source["rewards"].as_array().unwrap();
        let emitted_rewards = emitted["rewards"].as_array().unwrap();
        assert_eq!(source_rewards.len(), 2);

        for (src, exp) in source_rewards.iter().zip(emitted_rewards) {
            let index = src["pubkey"].as_u64().unwrap() as usize;
            assert_eq!(exp["pubkey"], ACCOUNTS[index], "the index must resolve to its account");
            assert_eq!(src["lamports"], exp["lamports"]);
            assert!(src["lamports"].is_string(), "lamports crosses the wire as a string");
        }
    }
}
