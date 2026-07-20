//! CT-4 — an equivocating source must not rewrite the accepted finalized prefix, and an honest
//! reorg above finality must recover rather than wedge.
//!
//! Covers INV-12/13/14/24, WP-6 and FM-SRC-5 through the public service binding. Conflict windows
//! deliberately omit the old finalized block, forcing fork resolution to resume at a stored chunk
//! boundary — both at the retained-window floor and from a chunk that straddles finality. The
//! whole-chunk rewrite that follows is verified on the write path: an equivocating source reproduces
//! a different hash at the finalized height and is refused; an honest source reproduces it and the
//! service converges on the reorged chain.

use std::{
    sync::Arc,
    time::{Duration, Instant}
};

use anyhow::{Context, Result, ensure};
use sqd_hotblocks_harness::{
    P_CONFLICT_WINDOW,
    chain::HlFills,
    harness::{Harness, HarnessConfig},
    types::BlockRef
};

const START: u64 = 1_000;
const DEEP_FORK_BLOCKS: u32 = (P_CONFLICT_WINDOW + 50) as u32;
const PREFIX_CHUNK_BLOCKS: u32 = 50;
const STRADDLING_CHUNK_BLOCKS: u32 = (P_CONFLICT_WINDOW + 50) as u32;
const FINALITY_LAG: u64 = P_CONFLICT_WINDOW + 20;
const REJECTION_TIMEOUT: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test(flavor = "multi_thread")]
async fn ct4_finality_equivocation_does_not_replace_finalized_prefix() -> Result<()> {
    let mut h = start_harness(false).await?;

    if let Err(err) = run_deep_fork(&mut h).await {
        panic!("CT-4 failed: {err:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ct4_straddling_chunk_rollback_respects_finalized_floor() -> Result<()> {
    let mut h = start_harness(true).await?;

    if let Err(err) = run_straddling_chunk_fork(&mut h).await {
        panic!("CT-4 straddling-chunk scenario failed: {err:?}");
    }
    Ok(())
}

/// The honest dual of the fault scenarios: a legitimate reorg above `fin` whose common ancestor
/// lies inside a finality-straddling chunk must RECOVER, not wedge. Fork resolution resumes at the
/// chunk boundary (below `fin`) and rewrites the whole chunk; the write path verifies the finalized
/// block is reproduced and accepts. Before the finalized-floor fix this clamped to `fin + 1`, a
/// mid-chunk position `insert_fork` rejected, freezing the dataset on a 60-second restart loop.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_honest_reorg_into_straddling_chunk_recovers() -> Result<()> {
    let mut h = start_harness(true).await?;

    if let Err(err) = run_honest_reorg_recovery(&mut h).await {
        panic!("honest-reorg recovery failed: {err:?}");
    }
    Ok(())
}

async fn start_harness(disable_compaction: bool) -> Result<Harness> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(HlFills), START);
    cfg.disable_compaction = disable_compaction;
    Harness::start(cfg).await
}

async fn run_deep_fork(h: &mut Harness) -> Result<()> {
    // Arrange: accept a chain whose finalized head is deeper than one conflict-hint window.
    h.produce(DEEP_FORK_BLOCKS)?;
    h.finalize_with_lag(FINALITY_LAG)?;
    h.settle().await?;
    h.assert_conforms().await?;

    assert_finality_equivocation_rejected(h, START, DEEP_FORK_BLOCKS).await
}

async fn run_straddling_chunk_fork(h: &mut Harness) -> Result<()> {
    // Arrange: commit two separate responses as [prefix] [finality-straddling chunk].
    h.produce(PREFIX_CHUNK_BLOCKS)?;
    h.settle().await?;
    h.assert_conforms().await?;

    let straddling_chunk_start = START
        .checked_add(u64::from(PREFIX_CHUNK_BLOCKS))
        .context("the second chunk start overflows")?;
    h.produce(STRADDLING_CHUNK_BLOCKS)?;
    h.finalize_with_lag(FINALITY_LAG)?;
    h.settle().await?;
    h.assert_conforms().await?;

    let head = h.model.head().context("the accepted model has no head")?;
    let fin = h
        .model
        .fin
        .as_ref()
        .context("the accepted model has no finalized head")?;
    ensure!(
        straddling_chunk_start < fin.number && fin.number < head.number,
        "finalized block {} is not strictly inside the second chunk [{straddling_chunk_start}, {}]",
        fin.number,
        head.number
    );

    assert_finality_equivocation_rejected(h, straddling_chunk_start, STRADDLING_CHUNK_BLOCKS).await
}

async fn run_honest_reorg_recovery(h: &mut Harness) -> Result<()> {
    // Arrange: the same [prefix] [finality-straddling] layout, with `fin` strictly inside chunk 2.
    h.produce(PREFIX_CHUNK_BLOCKS)?;
    h.settle().await?;
    let straddling_chunk_start = START
        .checked_add(u64::from(PREFIX_CHUNK_BLOCKS))
        .context("the second chunk start overflows")?;
    h.produce(STRADDLING_CHUNK_BLOCKS)?;
    h.finalize_with_lag(FINALITY_LAG)?;
    h.settle().await?;
    h.assert_conforms().await?;

    let head = h.model.head().context("the accepted model has no head")?;
    let fin = h
        .model
        .fin
        .clone()
        .context("the accepted model has no finalized head")?;
    ensure!(
        straddling_chunk_start < fin.number && fin.number < head.number,
        "finalized block {} is not strictly inside the second chunk [{straddling_chunk_start}, {}]",
        fin.number,
        head.number
    );

    // Act: an honest reorg of the tip, above `fin` but inside the straddling chunk. Its common
    // ancestor sits below `fin`, so resolution must resume at the chunk boundary and rewrite the
    // whole chunk — reproducing the finalized block unchanged.
    let reorg_from = fin.number + (head.number - fin.number) / 2;
    ensure!(
        fin.number < reorg_from && reorg_from <= head.number,
        "the reorg point {reorg_from} must lie strictly above finality and on the chain"
    );
    h.fork(reorg_from, STRADDLING_CHUNK_BLOCKS)?;

    // Assert: the service recovers onto the reorged chain and finality is preserved, not rewound.
    h.settle().await?;
    h.assert_conforms().await?;
    let recovered_fin = h
        .client
        .finalized_head()
        .await
        .context("failed to read FINALIZED-HEAD after recovery")?;
    ensure!(
        recovered_fin.as_ref() == Some(&fin),
        "finality moved during an honest recovery: expected {fin:?}, got {recovered_fin:?}"
    );
    Ok(())
}

async fn assert_finality_equivocation_rejected(h: &Harness, fork_from: u64, replacement_blocks: u32) -> Result<()> {
    let expected_head = h.model.head().context("the accepted model has no head")?;
    let expected_fin = h
        .model
        .fin
        .clone()
        .context("the accepted model has no finalized head")?;
    ensure!(
        expected_head.number.saturating_sub(expected_fin.number) > P_CONFLICT_WINDOW,
        "the first conflict window would include finalized block {}",
        expected_fin.number
    );
    let baseline_requests = h.sim.stats(&h.dataset).stream_requests;

    // Act: the source rewrites a suffix including `fin` and claims the new tip final.
    // This is a source fault, so the reference model intentionally remains on the accepted fork.
    h.sim
        .equivocate_finalized_prefix(&h.dataset, fork_from, replacement_blocks)?;
    let conflicting_source_head = h.sim.tip(&h.dataset).context("the faulty source has no head")?;
    assert_ne!(
        conflicting_source_head.hash, expected_head.hash,
        "the fault did not mint a distinct source branch"
    );

    // Assert: fork resolution resumes at a chunk boundary and the write path refuses the rewrite
    // (it reproduces a different hash at the finalized height), so every public watermark stays on
    // the last accepted state throughout.
    await_finality_fault_rejection(h, &expected_head, &expected_fin, baseline_requests).await?;
    h.assert_conforms().await?;
    Ok(())
}

async fn await_finality_fault_rejection(
    h: &Harness,
    expected_head: &BlockRef,
    expected_fin: &BlockRef,
    baseline_requests: u64
) -> Result<()> {
    // Hold the accepted watermarks still for the whole window. Adopting the equivocation would move
    // HEAD/FINALIZED-HEAD off the accepted chain within a poll or two; refusing it keeps them fixed.
    let deadline = Instant::now() + REJECTION_TIMEOUT;
    loop {
        let observed_head = h
            .client
            .head()
            .await
            .context("failed to read HEAD during fork recovery")?;
        let observed_fin = h
            .client
            .finalized_head()
            .await
            .context("failed to read FINALIZED-HEAD during fork recovery")?;
        ensure!(
            observed_head.as_ref() == Some(expected_head),
            "finality equivocation changed HEAD: expected {expected_head:?}, got {observed_head:?}"
        );
        ensure!(
            observed_fin.as_ref() == Some(expected_fin),
            "finality equivocation changed FINALIZED-HEAD: expected {expected_fin:?}, got {observed_fin:?}"
        );

        if Instant::now() > deadline {
            // Liveness: the SUT actually re-engaged the faulty source rather than idling.
            let stats = h.sim.stats(&h.dataset);
            ensure!(
                stats.stream_requests > baseline_requests,
                "the SUT never re-engaged the faulty source after the fault: {stats:?}"
            );
            return Ok(());
        }
        tokio::time::sleep(POLL).await;
    }
}
