//! CT-1 — stateful conformance on a happy-path script (spec 12 §3), run against every kind the
//! service serves.
//!
//! Verifies INV-1..3 (structural chain), INV-5/6 (watermark bounds), INV-7 (provenance),
//! INV-11 (append), INV-12 (finality monotone), INV-21/22 (response shape and completeness),
//! INV-23 (anchored ancestry), INV-25 (progress), INV-27 (range honesty), INV-30 (reporting),
//! RP-5 (bounded wait), RP-9/10 (coverage, continuation), OB-1 (chain gauges).

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use serde_json::Value;
use sqd_hotblocks_harness::{
    chain::{Chain, Evm, HlFills, Solana},
    driver::FollowStep,
    harness::{Harness, HarnessConfig},
    sim::Numbering
};

const START: u64 = 1_000;
const PATIENCE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn ct1_evm() -> Result<()> {
    ct1(Arc::new(Evm), Numbering::Dense).await
}

struct EvmChangingOptionalMask;

impl Chain for EvmChangingOptionalMask {
    fn config_kind(&self) -> &'static str {
        Evm.config_kind()
    }

    fn storage_kind(&self) -> &'static str {
        Evm.storage_kind()
    }

    fn dialect(&self) -> &'static str {
        Evm.dialect()
    }

    fn source_block(&self, block: &sqd_hotblocks_harness::types::Block) -> Value {
        let mut value = Evm.source_block(block);
        if block.number % 2 == 0 {
            value
                .as_object_mut()
                .expect("EVM source block is an object")
                .remove("traces");
        }
        value
    }

    fn scan_query(&self, from: u64, to: Option<u64>, expected_parent: Option<&str>) -> Value {
        Evm.scan_query(from, to, expected_parent)
    }

    fn expected_emission(&self, block: &sqd_hotblocks_harness::types::Block) -> Value {
        Evm.expected_emission(block)
    }
}

/// Every block flips between a full EVM data mask and one without traces. The ingest path
/// must flush at every transition without dropping or duplicating any blocks or required
/// tables.
#[tokio::test(flavor = "multi_thread")]
async fn ct1_evm_changing_optional_mask() -> Result<()> {
    ct1(Arc::new(EvmChangingOptionalMask), Numbering::Dense).await
}

/// Solana numbers blocks by time-based slots, and a slot that produced nothing leaves a hole.
/// The window is still one chain — carried by `parentNumber`, not by the numbering (INV-1/2).
/// The service links batches and chunks by hash and never by number, so it must not care.
#[tokio::test(flavor = "multi_thread")]
async fn ct1_solana() -> Result<()> {
    ct1(Arc::new(Solana), Numbering::Sparse).await
}

#[tokio::test(flavor = "multi_thread")]
async fn ct1_hyperliquid_fills() -> Result<()> {
    ct1(Arc::new(HlFills), Numbering::Dense).await
}

async fn ct1(chain: Arc<dyn Chain>, numbering: Numbering) -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), chain, START);
    cfg.numbering = numbering;
    let mut h = Harness::start(cfg).await?;

    // `Sut::drop` prints the service log on unwind, so a failure comes with its own account.
    if let Err(err) = run(&mut h).await {
        panic!("CT-1 ({}) failed: {err:?}", h.chain.config_kind());
    }
    Ok(())
}

async fn run(h: &mut Harness) -> Result<()> {
    // --- Act 1: cold ingest of a window, finality trailing the tip -------------------------
    h.produce(50)?;
    h.finalize_with_lag(5)?;

    h.settle().await?;
    h.assert_conforms().await?;

    assert_eq!(
        h.model.first(),
        Some(START),
        "the window starts where the config pinned it"
    );
    assert_eq!(h.model.span(), 50);
    assert!(h.model.fin.is_some(), "finality trails the tip");

    if h.numbering == Numbering::Sparse {
        let holes = h.model.seg.windows(2).filter(|w| w[1].number > w[0].number + 1).count();
        assert!(holes > 0, "the sparse script produced a chain with no holes in it");
    }

    // --- Act 2: an anchored client follows the tip ------------------------------------------
    let mut follower = h.follower();
    let tip = h.model.head().expect("a head").number;
    follower.follow_to(&h.client, &*h.chain, tip, PATIENCE).await?;
    assert_eq!(follower.local.len(), 50, "the follower backfilled the whole window");

    for _ in 0..4 {
        h.produce(5)?;
        h.finalize_with_lag(5)?;
        let target = h.model.head().expect("a head").number;
        follower.follow_to(&h.client, &*h.chain, target, PATIENCE).await?;
    }

    h.settle().await?;
    h.assert_conforms().await?;

    // The client's chain is the model's window, block for block (INV-23/25).
    assert_eq!(follower.local.len(), h.model.span());
    for (got, want) in follower.local.iter().zip(&h.model.seg) {
        assert_eq!(got.number, want.number);
        assert_eq!(
            got.hash, want.hash,
            "block {} came back on the wrong branch",
            got.number
        );
        assert_eq!(got.parent_hash, want.parent_hash);
    }
    assert_eq!(
        follower.rollbacks, 0,
        "a happy-path script must never force a client to roll back"
    );

    // Above the head: a bounded wait, then NO_DATA — a poll, not an error (RP-5).
    assert_eq!(follower.step(&h.client, &*h.chain).await?, FollowStep::NoData);

    let stats = h.sim.stats(&h.dataset);
    assert_eq!(
        stats.below_history, 0,
        "the service asked the source for blocks below its window"
    );
    assert_eq!(stats.fork_signals, 0, "a happy-path script produces no fork signals");
    assert!(
        stats.blocks_served >= 70,
        "the source served {} blocks",
        stats.blocks_served
    );

    Ok(())
}
