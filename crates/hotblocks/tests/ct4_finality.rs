//! CT-4 — an equivocating source must not rewrite the accepted finalized prefix, and an honest
//! reorg above finality must recover rather than wedge.
//!
//! Covers INV-12/13/14/24, WP-6 and FM-SRC-5 through the public binding. Conflict windows
//! deliberately omit the old finalized block, forcing fork resolution to resume at a stored chunk
//! boundary; the whole-chunk rewrite that follows is verified on the write path.

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

/// The honest dual: a legitimate reorg above `fin` whose common ancestor lies inside a
/// finality-straddling chunk must recover, not wedge. Before the fix this clamped to `fin + 1`, a
/// mid-chunk position `insert_fork` rejected, freezing the dataset on a 60-second restart loop.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_honest_reorg_into_straddling_chunk_recovers() -> Result<()> {
    let mut h = start_harness(true).await?;

    if let Err(err) = run_honest_reorg_recovery(&mut h, false).await {
        panic!("honest-reorg recovery failed: {err:?}");
    }
    Ok(())
}

/// The same reorg, with the source cutting every response short so the replay's first chunk ends
/// one block below `fin`: accepting it drops the finalized block, refusing it parks the epoch. Every
/// retry then repeats the identical cut, so without the flush floor the dataset never recovers.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_honest_reorg_recovers_when_the_replay_is_cut_below_finality() -> Result<()> {
    let mut h = start_harness(true).await?;

    if let Err(err) = run_honest_reorg_recovery(&mut h, true).await {
        panic!("cut-replay recovery failed: {err:?}");
    }
    Ok(())
}

/// The subtle equivocation: the source reproduces the finalized block's own hash but rewrites a
/// block *below* it. The replacement stays internally linked, so a guard checking only that hash
/// admits it and the finalized prefix changes under readers (INV-13).
#[tokio::test(flavor = "multi_thread")]
async fn ct4_replacement_rewriting_below_finality_is_refused() -> Result<()> {
    let mut h = start_harness(true).await?;

    if let Err(err) = run_rewrite_below_finality(&mut h).await {
        panic!("below-finality rewrite was not refused: {err:?}");
    }
    Ok(())
}

async fn start_harness(disable_compaction: bool) -> Result<Harness> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(HlFills), START);
    cfg.disable_compaction = disable_compaction;
    Harness::start(cfg).await
}

async fn run_deep_fork(h: &mut Harness) -> Result<()> {
    // A finalized head deeper than one conflict-hint window.
    h.produce(DEEP_FORK_BLOCKS)?;
    h.finalize_with_lag(FINALITY_LAG)?;
    h.settle().await?;
    h.assert_conforms().await?;

    assert_finality_equivocation_rejected(h, START, DEEP_FORK_BLOCKS, START).await
}

async fn run_straddling_chunk_fork(h: &mut Harness) -> Result<()> {
    // Two separate responses commit as [prefix] [finality-straddling chunk].
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

    assert_finality_equivocation_rejected(
        h,
        straddling_chunk_start,
        STRADDLING_CHUNK_BLOCKS,
        straddling_chunk_start
    )
    .await
}

async fn run_rewrite_below_finality(h: &mut Harness) -> Result<()> {
    // The [prefix] [finality-straddling] layout puts the resume boundary below `fin`, so the
    // replay covers finalized ground.
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
    let tampered_at = straddling_chunk_start + (fin.number - straddling_chunk_start) / 2;
    ensure!(
        straddling_chunk_start <= tampered_at && tampered_at < fin.number,
        "the tampered block {tampered_at} must lie inside the replayed range and below finality"
    );
    h.sim.reset_stream_request_observations(&h.dataset);
    let tampered = h.sim.rewrite_hash_below_finality(&h.dataset, tampered_at)?;
    ensure!(
        h.model.hash_at(tampered_at).is_some_and(|h| h != tampered.hash),
        "the fault did not change the stored hash at {tampered_at}"
    );
    // A source fault, so the model stays on the accepted chain: reorg through the simulator
    // rather than `Harness::fork`.
    let reorg_from = fin.number + (head.number - fin.number) / 2;
    h.sim.fork(&h.dataset, reorg_from, STRADDLING_CHUNK_BLOCKS)?;

    await_finality_fault_rejection(h, &head, &fin, straddling_chunk_start).await?;
    h.assert_conforms().await?;
    Ok(())
}

async fn run_honest_reorg_recovery(h: &mut Harness, cut_replay: bool) -> Result<()> {
    // The same layout, with `fin` strictly inside chunk 2.
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

    // An honest tip reorg above `fin` but inside the straddling chunk: its common ancestor sits
    // below `fin`, so the whole chunk is rewritten and must reproduce the finalized block.
    let reorg_from = fin.number + (head.number - fin.number) / 2;
    ensure!(
        fin.number < reorg_from && reorg_from <= head.number,
        "the reorg point {reorg_from} must lie strictly above finality and on the chain"
    );
    if cut_replay {
        // The lagging report is what lets a cut response end a chunk: the client suppresses its
        // own end-of-response commit below the finality it has seen, and a restart resets that to
        // the lagging replica's view. The 200k-row flush bound is the other way to get there.
        let cut = fin.number - straddling_chunk_start;
        ensure!(cut > 0, "the cut response must stop strictly below finality");
        h.sim.inject_fault(&h.dataset, |f| {
            f.max_blocks_per_response = Some(cut as u32);
            f.finality_report_cap = Some(straddling_chunk_start);
        });
        h.sut.restart().await?;
    }
    h.sim.reset_stream_request_observations(&h.dataset);
    h.fork(reorg_from, STRADDLING_CHUNK_BLOCKS)?;

    h.settle().await?;
    assert_replay_started_at(h, straddling_chunk_start)?;
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

async fn assert_finality_equivocation_rejected(
    h: &Harness,
    fork_from: u64,
    replacement_blocks: u32,
    expected_resume: u64
) -> Result<()> {
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
    // The source rewrites a suffix including `fin` and claims the new tip final; a source fault,
    // so the reference model stays on the accepted fork.
    h.sim.reset_stream_request_observations(&h.dataset);
    h.sim
        .equivocate_finalized_prefix(&h.dataset, fork_from, replacement_blocks)?;
    let conflicting_source_head = h.sim.tip(&h.dataset).context("the faulty source has no head")?;
    assert_ne!(
        conflicting_source_head.hash, expected_head.hash,
        "the fault did not mint a distinct source branch"
    );

    await_finality_fault_rejection(h, &expected_head, &expected_fin, expected_resume).await?;
    h.assert_conforms().await?;
    Ok(())
}

async fn await_finality_fault_rejection(
    h: &Harness,
    expected_head: &BlockRef,
    expected_fin: &BlockRef,
    expected_resume: u64
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
            // Engagement: this must be a new HTTP replay request, not the source mutation waking
            // the already-parked long poll. Its exact position also pins the physical rollback
            // boundary; final-state equality alone cannot distinguish it from a full-window replay.
            assert_replay_started_at(h, expected_resume)?;
            return Ok(());
        }
        tokio::time::sleep(POLL).await;
    }
}

fn assert_replay_started_at(h: &Harness, expected_resume: u64) -> Result<()> {
    let stats = h.sim.stats(&h.dataset);
    ensure!(
        stats.stream_http_requests > 0,
        "the SUT never opened a replay request after the fork: {stats:?}"
    );
    ensure!(
        stats.lowest_stream_from == Some(expected_resume),
        "the SUT resumed from {:?}, expected the stored chunk boundary {expected_resume}: {stats:?}",
        stats.lowest_stream_from
    );
    Ok(())
}
