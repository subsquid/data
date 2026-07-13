//! The reference model — the oracle (spec 12 §2). One instance per dataset, fed the same script
//! the simulator executes. Transitions mirror the normative pseudocode one-for-one;
//! well-formedness (INV-1..6) is asserted after each.
//!
//! It is deliberately block-exact. Where the implementation may be coarser (chunk-granular
//! retention, RS-3's "keep *at least* k blocks"), the slack belongs in the comparator, not here.

use anyhow::{Result, bail, ensure};
use serde_json::Value;

use crate::{
    chain::Chain,
    types::{Anchor, Block, BlockNumber, BlockRef}
};

/// Outcome of a FINALIZE transition (WP §2.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Finalize {
    Applied,
    /// Stale or not-yet-applicable report; state unchanged (WP-7).
    Ignored,
    /// Finality contradicting stored data — must be alarmed, never accepted (WP-8, GAP-4/5).
    IntegrityFault
}

/// Outcome of resolving a `ForkSignal` (WP-6/WP-6b).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkResolution {
    Resume {
        from: BlockNumber,
        parent_hash: Option<String>
    },
    /// The divergence is at or below the anchor — a destructive re-bootstrap, alarmed
    /// and observable like every RESET (WP-6b, OB-9).
    Reset { anchor: Anchor },
    /// The divergence is below the finalized prefix — unapplicable (FM-SRC-5).
    IntegrityFault
}

/// What a query must produce against this state (04 §3/§7). The coverage end `L` is the one free
/// variable left to the implementation (12 §2), so `Ok` reports only the bound it may not exceed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Predicted {
    Ok { hi: BlockNumber },
    NoData,
    RangeUnavailable,
    Conflict { hints: Vec<BlockRef> }
}

/// DEF-6 — the abstract dataset state.
#[derive(Clone, Debug)]
pub struct Model {
    pub seg: Vec<Block>,
    pub anchor: Anchor,
    pub fin: Option<BlockRef>,
    pub ver: u64
}

impl Model {
    pub fn new(anchor: Anchor) -> Self {
        Self {
            seg: Vec::new(),
            anchor,
            fin: None,
            ver: 0
        }
    }

    pub fn first(&self) -> Option<BlockNumber> {
        self.seg.first().map(|b| b.number)
    }

    pub fn head(&self) -> Option<BlockRef> {
        self.seg.last().map(Block::as_ref)
    }

    pub fn next(&self) -> BlockNumber {
        self.seg.last().map_or(self.anchor.number + 1, |b| b.number + 1)
    }

    pub fn span(&self) -> usize {
        self.seg.len()
    }

    /// DEF-16 — the chain hash at position `n`: the highest stored block at or below it, else
    /// the anchor (whose hash may be `⊥`). `None` below the anchor — the position is outside
    /// what this window knows.
    pub fn hash_at(&self, n: BlockNumber) -> Option<&str> {
        let i = self.seg.partition_point(|b| b.number <= n);
        if i > 0 {
            return Some(&self.seg[i - 1].hash);
        }
        if n >= self.anchor.number {
            return self.anchor.hash.as_deref();
        }
        None
    }

    /// The stored block numbered exactly `n` — `⊥` at a hole (12 §2).
    fn block_at(&self, n: BlockNumber) -> Option<&Block> {
        self.seg
            .binary_search_by_key(&n, |b| b.number)
            .ok()
            .map(|i| &self.seg[i])
    }

    fn stored_or_anchor(&self, r: &BlockRef) -> bool {
        self.hash_at(r.number) == Some(r.hash.as_str())
    }

    /// INV-1..6 — must hold in every observable state.
    fn wf(&self) {
        for w in self.seg.windows(2) {
            // Numbering may be sparse (Solana slots), so contiguity of *numbers* is not the
            // invariant; being parent and child is.
            assert!(w[1].number > w[0].number, "INV-1: segment is not ascending");
            assert_eq!(
                w[1].parent_number, w[0].number,
                "INV-1: a block is missing between {} and {}",
                w[0].number, w[1].number
            );
            assert_eq!(w[1].parent_hash, w[0].hash, "INV-2: segment is not linked");
        }
        if let Some(first) = self.seg.first() {
            assert!(
                self.anchor.number < first.number,
                "INV-3: the anchor does not sit below the window"
            );
            if let Some(h) = &self.anchor.hash {
                assert_eq!(&first.parent_hash, h, "INV-3: window does not link to its anchor");
            }
        }
        if let Some(fin) = &self.fin {
            let first = self.first().expect("INV-5: finalized watermark without a window");
            let head = self.head().expect("INV-5: finalized watermark without a window").number;
            assert!(
                first <= fin.number && fin.number <= head,
                "INV-5: finalized watermark {} outside [{first}, {head}]",
                fin.number
            );
            assert_eq!(
                self.block_at(fin.number).map(|b| b.hash.as_str()),
                Some(fin.hash.as_str()),
                "INV-6: finalized watermark does not name the stored block"
            );
        }
    }

    /// WP §2.2 — append a linked run of blocks at the tail, optionally advancing `fin`.
    pub fn extend(&mut self, blocks: &[Block], fin: Option<&BlockRef>) -> Result<()> {
        ensure!(!blocks.is_empty(), "EXTEND with an empty run");
        valid_run(blocks)?;
        // `next()` is a *request position*: with sparse numbering the run may start above it.
        ensure!(
            blocks[0].number >= self.next(),
            "EXTEND at {} but the dataset expects {} or above",
            blocks[0].number,
            self.next()
        );
        if let Some(head) = self.head() {
            ensure!(
                blocks[0].parent_number == head.number,
                "EXTEND skips a block: {} is not the child of the head {head}",
                blocks[0].number
            );
            ensure!(
                blocks[0].parent_hash == head.hash,
                "EXTEND breaks linkage: block {} claims parent {} but the chain ends at {head}",
                blocks[0].number,
                blocks[0].parent_hash
            );
        } else if let Some(anchor) = &self.anchor.hash {
            ensure!(
                &blocks[0].parent_hash == anchor,
                "EXTEND breaks linkage: block {} does not link to the anchor",
                blocks[0].number
            );
        }
        self.seg.extend_from_slice(blocks);
        self.ver += 1;
        self.apply_finality(fin)?;
        self.wf();
        Ok(())
    }

    /// WP §2.3 — atomically substitute the suffix `>= from` (a fork).
    pub fn replace(&mut self, from: BlockNumber, blocks: &[Block], fin: Option<&BlockRef>) -> Result<()> {
        ensure!(!self.seg.is_empty(), "REPLACE on an empty window");
        ensure!(!blocks.is_empty(), "REPLACE with an empty run");
        valid_run(blocks)?;
        ensure!(
            blocks[0].number >= from,
            "REPLACE run starts below the fork point {from}"
        );
        let first = self.first().expect("seg is non-empty");
        // INV-14: a fork may not reach below the window ...
        ensure!(
            from >= first,
            "INV-14: REPLACE at {from} is below the window floor {first}"
        );
        // ... nor below the finalized prefix (INV-13).
        if let Some(fin) = &self.fin {
            ensure!(
                from > fin.number,
                "INV-13: REPLACE at {from} is at or below the finalized head {}",
                fin.number
            );
        }
        let keep = self.seg.partition_point(|b| b.number < from);
        {
            // The run attaches to the last surviving block, or to the anchor if none survives.
            match keep.checked_sub(1).map(|i| &self.seg[i]) {
                Some(base) => ensure!(
                    blocks[0].parent_number == base.number && blocks[0].parent_hash == base.hash,
                    "REPLACE breaks linkage at {from}: block {} is not the child of {}",
                    blocks[0].number,
                    base.number
                ),
                None => {
                    if let Some(anchor) = &self.anchor.hash {
                        ensure!(
                            &blocks[0].parent_hash == anchor,
                            "REPLACE breaks linkage at {from}: block {} does not link to the anchor",
                            blocks[0].number
                        );
                    }
                }
            }
        }
        self.seg.truncate(keep);
        self.seg.extend_from_slice(blocks);
        self.ver += 1;
        self.apply_finality(fin)?;
        self.wf();
        Ok(())
    }

    /// WP §2.4 — advance `fin`, monotone and clamped to the stored chain.
    pub fn finalize(&mut self, r: &BlockRef) -> Finalize {
        let outcome = self.finalize_inner(r);
        if outcome == Finalize::Applied {
            self.ver += 1;
            self.wf();
        }
        outcome
    }

    fn finalize_inner(&mut self, r: &BlockRef) -> Finalize {
        let Some(head) = self.head() else {
            return Finalize::Ignored;
        };
        let first = self.first().expect("seg is non-empty");
        let e = r.number.min(head.number);
        if e < first {
            return Finalize::Ignored;
        }
        if let Some(fin) = &self.fin
            && e < fin.number
        {
            return Finalize::Ignored;
        }
        let at_e = if e == r.number {
            // The report names an exact height: a hole there contradicts the stored chain the
            // same way a hash mismatch does (WP §2.4), and a different stored hash is WP-8.
            let Some(stored) = self.block_at(e) else {
                return Finalize::IntegrityFault;
            };
            if stored.hash != r.hash {
                return Finalize::IntegrityFault;
            }
            stored.hash.clone()
        } else {
            // Clamped: `e = head.number`, and the head is a stored block.
            head.hash
        };
        if let Some(fin) = &self.fin
            && e == fin.number
            && at_e != fin.hash
        {
            return Finalize::IntegrityFault;
        }
        let new = BlockRef::new(e, at_e);
        if self.fin.as_ref() == Some(&new) {
            return Finalize::Ignored;
        }
        self.fin = Some(new);
        Finalize::Applied
    }

    /// A commit may compose EXTEND/REPLACE with FINALIZE (02 §6) — one version bump, not two.
    fn apply_finality(&mut self, fin: Option<&BlockRef>) -> Result<()> {
        let Some(r) = fin else { return Ok(()) };
        match self.finalize_inner(r) {
            Finalize::Applied | Finalize::Ignored => Ok(()),
            Finalize::IntegrityFault => bail!("INTEGRITY_FAULT: finality report {r} contradicts stored data")
        }
    }

    /// WP §2.5 — trim the prefix below `from`, moving the anchor up.
    pub fn retain(&mut self, from: BlockNumber, hash: Option<&str>) {
        let floor = self.first().unwrap_or(self.anchor.number + 1);
        if from == floor || (self.seg.is_empty() && from < floor) {
            // No-op — unless the hash contradicts the anchor, which is a WP-9 self-heal.
            if let (Some(h), Some(anchor)) = (hash, self.anchor.hash.as_deref())
                && from == floor
                && h != anchor
            {
                self.reset(Anchor::new(from - 1, Some(h.to_string())));
            }
            return;
        }
        if from < floor {
            // WP §2.5: a downward bound is a destructive re-bootstrap — history below the
            // window cannot be re-acquired in place, so the window is discarded and
            // re-ingested from `from` upward. Alarmed like every RESET (OB-9), never a
            // silent widening.
            self.reset(Anchor::new(from - 1, hash.map(str::to_string)));
            return;
        }
        if !self.seg.is_empty() && from <= self.next() {
            // The window's new anchor is the last block below `from` — with sparse numbering
            // that is not necessarily the block numbered `from - 1`.
            let drop = self.seg.partition_point(|b| b.number < from);
            let new_anchor = self.seg[drop - 1].as_ref();
            if let Some(h) = hash
                && h != new_anchor.hash
            {
                // The instruction names a block we do not have — re-anchor (WP-9).
                self.reset(Anchor::new(from - 1, Some(h.to_string())));
                return;
            }
            self.seg.drain(..drop);
            self.anchor = Anchor::new(new_anchor.number, Some(new_anchor.hash));
            // RS-2: the finalized watermark cannot survive below the window.
            if let Some(fin) = &self.fin
                && fin.number < from
            {
                self.fin = None;
            }
            self.ver += 1;
            self.wf();
        } else {
            self.reset(Anchor::new(from - 1, hash.map(str::to_string)));
        }
    }

    /// WP §2.6 — clear the window and re-anchor. Must be observable (OB-9).
    pub fn reset(&mut self, anchor: Anchor) {
        self.seg.clear();
        self.anchor = anchor;
        self.fin = None;
        self.ver += 1;
        self.wf();
    }

    /// WP-6 — where a `ForkSignal` puts the write position.
    pub fn resolve_fork(&self, hints: &[BlockRef]) -> Result<ForkResolution> {
        ensure!(!hints.is_empty(), "ForkSignal with no hints");
        ensure!(
            hints.windows(2).all(|w| w[0].number < w[1].number),
            "ForkSignal hints are not ascending"
        );
        let mut hints = hints;
        if let Some(fin) = &self.fin {
            let keep = hints.iter().position(|h| h.number >= fin.number);
            let Some(keep) = keep else {
                return Ok(ForkResolution::IntegrityFault);
            };
            hints = &hints[keep..];
            if hints[0].number == fin.number && hints[0].hash != fin.hash {
                return Ok(ForkResolution::IntegrityFault);
            }
        }
        // WP-6b (direct evidence): a hint at the anchor position contradicting a known anchor
        // hash puts the divergence at or below the anchor — RESET, never a replacement probe.
        if let Some(anchor_hash) = &self.anchor.hash
            && let Some(h) = hints.iter().find(|h| h.number == self.anchor.number)
            && &h.hash != anchor_hash
        {
            return Ok(ForkResolution::Reset {
                anchor: Anchor::new(self.anchor.number, Some(h.hash.clone()))
            });
        }
        let anchored = hints.iter().filter(|h| self.stored_or_anchor(h)).next_back();
        Ok(match anchored {
            Some(m) => ForkResolution::Resume {
                from: m.number + 1,
                parent_hash: Some(m.hash.clone())
            },
            // No hint matches the stored chain. Under finality only the volatile suffix may
            // be replaced (WP-6): the finalized block is common to both chains unless the
            // source contradicts finality — repeated rejection here is FM-SRC-5, never RESET.
            None => match &self.fin {
                Some(fin) => ForkResolution::Resume {
                    from: fin.number + 1,
                    parent_hash: Some(fin.hash.clone())
                },
                // Full-window replacement *probe*; repeated WP-2 rejection at `first(D)`
                // escalates to RESET per WP-6b.
                None => ForkResolution::Resume {
                    from: self.first().unwrap_or(self.anchor.number + 1),
                    parent_hash: self.anchor.hash.clone()
                }
            }
        })
    }

    /// 04 §3/§7 — the outcome class a query must produce against this state.
    pub fn predict_query(
        &self,
        from: BlockNumber,
        to: Option<BlockNumber>,
        finalized_only: bool,
        expected_parent: Option<&str>
    ) -> Predicted {
        let tip = if finalized_only { self.fin.clone() } else { self.head() };
        if let Some(first) = self.first()
            && from < first
        {
            return Predicted::RangeUnavailable;
        }
        // RP-5b: the assertion names the watermark block's exact position — a definite fork
        // on any chain, answered before the NO_DATA / long-poll path.
        if let (Some(expected), Some(t)) = (expected_parent, &tip)
            && from == t.number + 1
            && expected != t.hash
        {
            return Predicted::Conflict {
                hints: self.conflict_hints(from - 1)
            };
        }
        let Some(hi) = tip.map(|t| t.number) else {
            return Predicted::NoData;
        };
        if from > hi {
            return Predicted::NoData;
        }
        if let Some(expected) = expected_parent
            && let Some(stored) = self.hash_at(from - 1)
            && stored != expected
        {
            return Predicted::Conflict {
                hints: self.conflict_hints(from - 1)
            };
        }
        Predicted::Ok {
            hi: to.map_or(hi, |to| to.min(hi))
        }
    }

    /// RP-11 — the ancestors a CONFLICT payload must carry, ending at `at`.
    fn conflict_hints(&self, at: BlockNumber) -> Vec<BlockRef> {
        let hi = self.seg.partition_point(|b| b.number <= at);
        let lo = hi.saturating_sub(crate::P_CONFLICT_WINDOW as usize);
        self.seg[lo..hi].iter().map(Block::as_ref).collect()
    }

    /// INV-22 — what the SUT must emit for coverage `[from, l]` under `include_all`.
    /// The RP-9 coverage-end carrier and the boundary-marker exemption for *filtered*
    /// queries are a documented follow-up (README, "known open questions").
    pub fn emission(&self, from: BlockNumber, l: BlockNumber, chain: &dyn Chain) -> Vec<Value> {
        self.blocks_in(from, l)
            .iter()
            .map(|b| chain.expected_emission(b))
            .collect()
    }

    pub fn blocks_in(&self, from: BlockNumber, to: BlockNumber) -> &[Block] {
        if to < from {
            return &[];
        }
        let lo = self.seg.partition_point(|b| b.number < from);
        let hi = self.seg.partition_point(|b| b.number <= to);
        &self.seg[lo..hi]
    }
}

/// DEF-12 — a `Blocks` event must be an internally linked run of parents and children.
fn valid_run(blocks: &[Block]) -> Result<()> {
    for w in blocks.windows(2) {
        ensure!(w[1].number > w[0].number, "block run is not ascending");
        ensure!(
            w[1].parent_number == w[0].number,
            "block run has a hole: {} is not the parent of {}",
            w[0].number,
            w[1].number
        );
        ensure!(w[1].parent_hash == w[0].hash, "block run is not linked");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::block_hash;

    /// A linked run over the given block numbers, which need not be consecutive.
    fn chain_of(numbers: &[BlockNumber], fork_id: u32, anchor: (BlockNumber, &str)) -> Vec<Block> {
        let mut out = Vec::new();
        let (mut parent_number, mut parent_hash) = (anchor.0, anchor.1.to_string());
        for &number in numbers {
            let hash = block_hash(number, fork_id);
            out.push(Block {
                number,
                hash: hash.clone(),
                parent_number,
                parent_hash,
                timestamp_ms: 1_700_000_000_000 + number as i64 * 1000,
                fork_id
            });
            parent_number = number;
            parent_hash = hash;
        }
        out
    }

    /// A dense linked run on branch `fork_id`, starting at `from`.
    fn run(from: BlockNumber, len: u64, fork_id: u32, parent: &str) -> Vec<Block> {
        let numbers: Vec<_> = (from..from + len).collect();
        chain_of(&numbers, fork_id, (from - 1, parent))
    }

    fn model_at(start: BlockNumber, len: u64) -> (Model, Vec<Block>) {
        let anchor_hash = block_hash(start - 1, 0);
        let mut m = Model::new(Anchor::new(start - 1, Some(anchor_hash.clone())));
        let blocks = run(start, len, 0, &anchor_hash);
        m.extend(&blocks, None).unwrap();
        (m, blocks)
    }

    #[test]
    fn extend_appends_and_tracks_watermarks() {
        let (m, blocks) = model_at(100, 5);
        assert_eq!(m.first(), Some(100));
        assert_eq!(m.head(), Some(blocks[4].as_ref()));
        assert_eq!(m.next(), 105);
        assert_eq!(m.span(), 5);
        assert_eq!(m.fin, None);
    }

    #[test]
    fn extend_rejects_gaps_and_broken_linkage() {
        let (mut m, _) = model_at(100, 5);
        assert!(
            m.extend(&run(106, 1, 0, "0xdead"), None).is_err(),
            "gap must be rejected"
        );
        assert!(
            m.extend(&run(105, 1, 0, "0xdead"), None).is_err(),
            "broken linkage must be rejected"
        );
    }

    /// Solana numbers blocks by time-based slots: a slot that produced nothing leaves a hole,
    /// and the window is still one chain. Numbering carries no invariant — the parent does.
    #[test]
    fn sparse_numbering_is_still_one_chain() {
        let anchor = block_hash(99, 0);
        let mut m = Model::new(Anchor::new(99, Some(anchor.clone())));
        let blocks = chain_of(&[100, 103, 104, 109], 0, (99, &anchor));
        m.extend(&blocks, None).unwrap();

        assert_eq!(m.first(), Some(100));
        assert_eq!(m.head().unwrap().number, 109);
        assert_eq!(m.span(), 4);
        assert_eq!(m.hash_at(103), Some(blocks[1].hash.as_str()));
        assert_eq!(
            m.hash_at(101),
            Some(blocks[0].hash.as_str()),
            "a hole carries the preceding block's hash (DEF-16)"
        );
        assert_eq!(m.hash_at(99), Some(anchor.as_str()), "at the anchor position: its hash");
        assert_eq!(m.hash_at(42), None, "below the anchor the chain hash is undefined");
        assert_eq!(m.blocks_in(101, 105).len(), 2, "103 and 104");

        // A run whose first block is not the child of the head is a hole in the *chain* — that
        // is the violation, whatever the numbers say.
        let orphan = chain_of(&[112], 0, (110, &block_hash(110, 0)));
        assert!(m.extend(&orphan, None).is_err());

        // Trimming re-anchors on the last block below the cut, not on `from - 1`.
        m.retain(104, None);
        assert_eq!(m.first(), Some(104));
        assert_eq!(m.anchor, Anchor::new(103, Some(block_hash(103, 0))));

        // A fork point may fall in a hole; the run attaches to the last surviving block.
        let fork = chain_of(&[106, 107], 1, (104, &block_hash(104, 0)));
        m.replace(105, &fork, None).unwrap();
        assert_eq!(m.head().unwrap().number, 107);
        assert_eq!(m.span(), 3, "104, then the two forked blocks");
    }

    #[test]
    fn anchor_with_unknown_hash_accepts_any_first_block() {
        let mut m = Model::new(Anchor::new(99, None));
        assert!(m.extend(&run(100, 3, 0, "0xwhatever"), None).is_ok());
        assert_eq!(m.first(), Some(100));
    }

    #[test]
    fn finalize_is_monotone_clamped_and_hash_checked() {
        let (mut m, blocks) = model_at(100, 5);

        // Above the head: clamped to the head (WP §2.4).
        assert_eq!(m.finalize(&BlockRef::new(200, block_hash(200, 0))), Finalize::Applied);
        assert_eq!(m.fin, Some(blocks[4].as_ref()));

        // Stale report: ignored, not an error (WP-7).
        assert_eq!(m.finalize(&BlockRef::new(101, block_hash(101, 0))), Finalize::Ignored);
        assert_eq!(m.fin, Some(blocks[4].as_ref()));

        // A report naming a block we do not have is an integrity fault (WP-8) — this is the
        // check GAP-4 says the implementation skips below the head.
        let (mut m, _) = model_at(100, 5);
        assert_eq!(
            m.finalize(&BlockRef::new(102, block_hash(102, 7))),
            Finalize::IntegrityFault
        );
        assert_eq!(m.fin, None, "a faulted report must not mutate state");
    }

    #[test]
    fn finalize_on_a_hole_is_an_integrity_fault() {
        let anchor = block_hash(99, 0);
        let mut m = Model::new(Anchor::new(99, Some(anchor.clone())));
        m.extend(&chain_of(&[100, 103, 104], 0, (99, &anchor)), None).unwrap();
        // WP §2.4: a report naming a height that is a hole in the stored chain contradicts
        // the chain exactly like a hash mismatch.
        assert_eq!(
            m.finalize(&BlockRef::new(101, block_hash(101, 0))),
            Finalize::IntegrityFault
        );
        assert_eq!(m.fin, None);
    }

    #[test]
    fn replace_forks_above_finality_only() {
        let (mut m, blocks) = model_at(100, 5);
        assert_eq!(m.finalize(&BlockRef::new(102, block_hash(102, 0))), Finalize::Applied);

        // Below the finalized head — INV-13.
        let bad = run(102, 2, 1, &blocks[0].hash);
        assert!(m.replace(102, &bad, None).is_err());

        // Above it — accepted, and the head moves back by one.
        let good = run(103, 2, 1, &blocks[2].hash);
        m.replace(103, &good, None).unwrap();
        assert_eq!(m.span(), 5);
        assert_eq!(m.head().unwrap().number, 104);
        assert_eq!(m.head().unwrap().hash, block_hash(104, 1));
        assert_eq!(m.fin.as_ref().unwrap().number, 102, "finality survives a fork above it");
    }

    #[test]
    fn replace_below_the_window_is_rejected() {
        let (mut m, _) = model_at(100, 5);
        assert!(
            m.replace(99, &run(99, 2, 1, "0xzz"), None).is_err(),
            "INV-14 fork floor"
        );
    }

    #[test]
    fn retain_trims_moves_the_anchor_and_drops_stale_finality() {
        let (mut m, blocks) = model_at(100, 10);
        m.finalize(&BlockRef::new(101, block_hash(101, 0)));

        m.retain(105, None);

        assert_eq!(m.first(), Some(105));
        assert_eq!(m.anchor, Anchor::new(104, Some(blocks[4].hash.clone())), "INV-18");
        assert_eq!(m.fin, None, "RS-2: finality below the window is dropped");
        assert_eq!(m.head().unwrap().number, 109);

        // Trimming at the floor is a no-op.
        m.retain(105, None);
        assert_eq!(m.first(), Some(105));
    }

    #[test]
    fn retain_below_the_window_is_a_destructive_reset() {
        let (mut m, _) = model_at(100, 10);
        m.retain(105, None);
        // WP §2.5: lowering the bound cannot re-acquire history — the window is discarded
        // and re-ingested from the new bound (alarmed, OB-9); it never widens in place.
        m.retain(100, None);
        assert!(m.seg.is_empty());
        assert_eq!(m.anchor, Anchor::new(99, None));
        assert_eq!(m.next(), 100);
    }

    #[test]
    fn retain_above_the_head_resets() {
        let (mut m, _) = model_at(100, 5);
        m.retain(200, Some("0xnew"));
        assert!(m.seg.is_empty());
        assert_eq!(m.anchor, Anchor::new(199, Some("0xnew".to_string())));
        assert_eq!(m.next(), 200);
    }

    #[test]
    fn retain_with_a_conflicting_hash_self_heals() {
        let (mut m, _) = model_at(100, 10);
        m.retain(105, Some("0xnot-our-block"));
        assert!(m.seg.is_empty(), "WP-9: a contradicting instruction re-anchors");
        assert_eq!(m.anchor, Anchor::new(104, Some("0xnot-our-block".to_string())));
    }

    #[test]
    fn resolve_fork_picks_the_highest_known_hint() {
        let (m, blocks) = model_at(100, 5);
        let hints = vec![
            blocks[1].as_ref(),                     // known
            blocks[2].as_ref(),                     // known — the highest
            BlockRef::new(103, block_hash(103, 9)), // a block from another branch
            BlockRef::new(104, block_hash(104, 9)),
        ];
        assert_eq!(
            m.resolve_fork(&hints).unwrap(),
            ForkResolution::Resume {
                from: 103,
                parent_hash: Some(blocks[2].hash.clone())
            }
        );
    }

    #[test]
    fn resolve_fork_with_no_known_hint_falls_back_to_the_window_floor() {
        // `fin = ⊥`: a full-window replacement probe is the only fallback left (WP-6).
        let (m, _) = model_at(100, 5);
        let hints = vec![
            BlockRef::new(101, block_hash(101, 9)),
            BlockRef::new(102, block_hash(102, 9)),
        ];
        assert_eq!(
            m.resolve_fork(&hints).unwrap(),
            ForkResolution::Resume {
                from: 100,
                parent_hash: Some(block_hash(99, 0))
            }
        );
    }

    #[test]
    fn resolve_fork_with_no_known_hint_replaces_the_volatile_suffix_when_finalized() {
        let (mut m, blocks) = model_at(100, 5);
        m.finalize(&blocks[2].as_ref());
        let hints = vec![
            BlockRef::new(103, block_hash(103, 9)),
            BlockRef::new(104, block_hash(104, 9)),
        ];
        assert_eq!(
            m.resolve_fork(&hints).unwrap(),
            ForkResolution::Resume {
                from: 103,
                parent_hash: Some(blocks[2].hash.clone())
            },
            "WP-6: the fallback under finality is fin + 1, not the window floor"
        );
    }

    #[test]
    fn resolve_fork_contradicting_the_anchor_resets() {
        let (m, _) = model_at(100, 5);
        let hints = vec![
            BlockRef::new(99, block_hash(99, 9)),
            BlockRef::new(100, block_hash(100, 9)),
        ];
        assert_eq!(
            m.resolve_fork(&hints).unwrap(),
            ForkResolution::Reset {
                anchor: Anchor::new(99, Some(block_hash(99, 9)))
            },
            "WP-6b: divergence at or below the anchor is a re-bootstrap, not a probe"
        );
    }

    #[test]
    fn resolve_fork_below_finality_is_an_integrity_fault() {
        let (mut m, blocks) = model_at(100, 5);
        m.finalize(&blocks[3].as_ref());
        let hints = vec![
            BlockRef::new(101, block_hash(101, 9)),
            BlockRef::new(102, block_hash(102, 9)),
        ];
        assert_eq!(
            m.resolve_fork(&hints).unwrap(),
            ForkResolution::IntegrityFault,
            "FM-SRC-5"
        );
    }

    #[test]
    fn predict_query_covers_the_error_taxonomy() {
        let (mut m, blocks) = model_at(100, 5);
        m.finalize(&blocks[2].as_ref());

        assert_eq!(
            m.predict_query(99, None, false, None),
            Predicted::RangeUnavailable,
            "RP-4"
        );
        assert_eq!(m.predict_query(105, None, false, None), Predicted::NoData, "RP-5");
        assert_eq!(m.predict_query(100, None, false, None), Predicted::Ok { hi: 104 });
        assert_eq!(m.predict_query(100, Some(102), false, None), Predicted::Ok { hi: 102 });
        assert_eq!(
            m.predict_query(100, None, true, None),
            Predicted::Ok { hi: 102 },
            "RP-6"
        );
        assert_eq!(m.predict_query(103, None, true, None), Predicted::NoData, "RP-6");

        // Anchored: the parent of 103 is block 102.
        assert_eq!(
            m.predict_query(103, None, false, Some(&blocks[2].hash)),
            Predicted::Ok { hi: 104 }
        );
        let Predicted::Conflict { hints } = m.predict_query(103, None, false, Some("0xwrong")) else {
            panic!("RP-11: a mismatching expected_parent must conflict")
        };
        assert_eq!(hints.last().unwrap(), &blocks[2].as_ref(), "hints end at from - 1");
        assert!(
            hints.windows(2).all(|w| w[0].number < w[1].number),
            "hints are ascending"
        );
    }

    #[test]
    fn predict_query_conflicts_at_the_tip_before_no_data() {
        let (mut m, blocks) = model_at(100, 5); // head = 104

        // RP-5b: `from = head + 1` with a mismatching assertion is a definite fork — CONFLICT,
        // not a long-poll into NO_DATA.
        let Predicted::Conflict { hints } = m.predict_query(105, None, false, Some("0xwrong")) else {
            panic!("RP-5b: a mismatching assertion at head + 1 must conflict")
        };
        assert_eq!(hints.last().unwrap(), &blocks[4].as_ref(), "hints end at the tip");

        // One position further out the assertion names an un-ingested block: NO_DATA (RP-5).
        assert_eq!(m.predict_query(106, None, false, Some("0xwrong")), Predicted::NoData);

        // Finalized-only: the watermark block is `fin`.
        m.finalize(&blocks[2].as_ref());
        let Predicted::Conflict { .. } = m.predict_query(103, None, true, Some("0xwrong")) else {
            panic!("RP-5b applies to the finalized watermark too")
        };
        assert_eq!(
            m.predict_query(103, None, true, Some(&blocks[2].hash)),
            Predicted::NoData,
            "a matching assertion at fin + 1 long-polls as usual"
        );
    }

    #[test]
    fn predict_query_evaluates_anchors_across_holes() {
        // Blocks 100 and 105: an assertion for `from = 103` names `hash_at(102)`, which is
        // block 100 (DEF-16) — a hole never makes the assertion unevaluable (GAP-21 is the
        // implementation failing exactly this).
        let anchor = block_hash(99, 0);
        let mut m = Model::new(Anchor::new(99, Some(anchor.clone())));
        let blocks = chain_of(&[100, 105], 0, (99, &anchor));
        m.extend(&blocks, None).unwrap();

        assert_eq!(
            m.predict_query(103, None, false, Some(&blocks[0].hash)),
            Predicted::Ok { hi: 105 }
        );
        let Predicted::Conflict { hints } = m.predict_query(103, None, false, Some("0xwrong")) else {
            panic!("RP-11: the assertion is evaluated against the preceding block across a hole")
        };
        assert_eq!(
            hints.last().unwrap(),
            &blocks[0].as_ref(),
            "hints end at the nearest stored position below from - 1"
        );
    }
}
