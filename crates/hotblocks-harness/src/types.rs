//! Primitives shared by the simulator, the model and the driver (spec 02).

use serde::{Deserialize, Serialize};

pub type BlockNumber = u64;

/// DEF-3 — a block reference `⟨number, hash⟩`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub number: BlockNumber,
    pub hash: String
}

impl BlockRef {
    pub fn new(number: BlockNumber, hash: impl Into<String>) -> Self {
        Self {
            number,
            hash: hash.into()
        }
    }
}

impl std::fmt::Display for BlockRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.number, &self.hash[..self.hash.len().min(10)])
    }
}

/// DEF-4. Only the structural part is materialized: the payload is a pure function of
/// `(number, fork_id)`, so the simulator and the model derive identical items independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub number: BlockNumber,
    pub hash: String,
    /// The number of the parent block, which is *not* always `number - 1`: Solana numbers
    /// blocks by time-based slots and a slot that produced nothing leaves a hole. This is the
    /// pointer that makes the window one chain; the numbering does not.
    pub parent_number: BlockNumber,
    pub parent_hash: String,
    pub timestamp_ms: i64,
    /// The branch it was minted on: a fork mints a fresh id, so replaced blocks differ in hash.
    pub fork_id: u32
}

impl Block {
    pub fn as_ref(&self) -> BlockRef {
        BlockRef::new(self.number, self.hash.clone())
    }
}

/// DEF-7. `hash = None` is the spec's `⊥`: linkage below the window is unverifiable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Anchor {
    pub number: BlockNumber,
    pub hash: Option<String>
}

impl Anchor {
    pub fn new(number: BlockNumber, hash: Option<String>) -> Self {
        Self { number, hash }
    }
}

/// Deterministic hash of `(number, fork_id)` — hashes are opaque strings compared by equality (DEF-2).
pub fn block_hash(number: BlockNumber, fork_id: u32) -> String {
    let mut s =
        splitmix64(number.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ u64::from(fork_id).wrapping_mul(0xD1B5_4A32_D192_ED03));
    let mut out = String::with_capacity(66);
    out.push_str("0x");
    for _ in 0..4 {
        s = splitmix64(s);
        out.push_str(&format!("{s:016x}"));
    }
    out
}

/// Deterministic PRNG for randomized scripts.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        splitmix64(self.0)
    }

    /// Uniform-ish in `[0, n)`; `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

fn splitmix64(x: u64) -> u64 {
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_deterministic_and_branch_dependent() {
        assert_eq!(block_hash(100, 0), block_hash(100, 0));
        assert_ne!(block_hash(100, 0), block_hash(100, 1));
        assert_ne!(block_hash(100, 0), block_hash(101, 0));
        assert_eq!(block_hash(100, 0).len(), 66);
    }
}
