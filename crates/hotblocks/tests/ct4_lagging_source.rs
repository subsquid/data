//! CT-4 — several endpoints per dataset, one of them behind.
//!
//! Production configures a handful of sources per dataset and they do not stay in step: on
//! `base-mainnet` one endpoint has been observed minutes behind two that sit on the head. Every
//! script before this one ran exactly **one** source, so `StandardDataSource`'s multi-endpoint
//! path had never been executed by a test at all — not the fixed-order poll race
//! (`poll_next_event`), not the fork consensus, not the per-endpoint `MaybeOnHead` flush trigger.
//! `crates/data-source` carries no unit tests either.
//!
//! What makes lag worth a script of its own: the selector has no notion of *slow*. An endpoint
//! leaves the rotation only by erroring (`on_error` → `Backoff`); `is_active` knows `Backoff` and
//! nothing else. A healthy endpoint minutes behind the tip therefore keeps full standing in the
//! race forever, and two mechanisms could let it hold the served head back —
//!
//!  - the flush at the tip is `DataEvent::MaybeOnHead`, and it fires only when the endpoint whose
//!    stream just ended is *itself* the one that committed the current head block
//!    (`ep.last_committed_block == Some(prev_block)`). `position` is shared across endpoints while
//!    `last_committed_block` is per-endpoint, so which endpoint may trigger the flush depends on
//!    who won the last race;
//!  - blocks below the shared position are parsed and discarded with no early exit, and
//!    `poll_endpoint` loops until the stream returns `Pending` — so an endpoint's ready backlog is
//!    drained inside one poll of the single ingest task.
//!
//! Neither is asserted anywhere. These scripts assert the property both mechanisms would break:
//! **a source that is behind must not hold the head back.**
//!
//! It holds, in the minority and the majority alike and wherever the laggard sits in the fixed
//! order: a laggard is *below* the shared `position`, so it has nothing to serve and never wins
//! the race. What wedges the service is a source that answers **wrongly** rather than late — one
//! endpoint of three signalling a fork above its tip is enough.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use sqd_hotblocks_harness::{
    Evm,
    harness::{Harness, HarnessConfig}
};

fn config(sources: usize) -> HarnessConfig {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), 1_000);
    cfg.sources = sources;
    cfg
}

/// The baseline: with three endpoints all sitting on the head, the service must behave exactly as
/// it does with one. This is the first multi-source coverage the suite has ever had, and it has to
/// pass before a lag result means anything.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_three_sources_in_step_conform() -> Result<()> {
    let mut h = Harness::start(config(3)).await?;

    h.produce(40)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;
    h.assert_conforms().await?;

    Ok(())
}

/// Production's shape: one endpoint of three left far behind while the other two stay on the head.
/// The healthy majority carries the chain, so the served head must track the canonical tip
/// regardless of the laggard — INV-11/INV-30, LIV-1.
///
/// A failure here is a stalled head, and `settle` reports it as the service not converging on the
/// script within the quiescence timeout.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_a_lagging_source_does_not_hold_the_head_back() -> Result<()> {
    let mut h = Harness::start(config(3)).await?;

    // All three on the head first, so the laggard starts from a state where it has committed
    // blocks itself — that is what scatters `last_committed_block` across endpoints.
    h.produce(20)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;

    // Peer 0 holds while the primary and peer 1 advance. On a 2 s chain this is a ~4 min laggard.
    for _ in 0..12 {
        h.produce_lagging(&[0], 10)?;
        h.finalize_with_lag(5)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    h.settle().await?;
    h.assert_conforms().await?;

    Ok(())
}

/// The margin: the laggards are the *majority* and one healthy endpoint carries the chain alone.
/// Not a shape production has been seen in — but if a healthy minority suffices, lag is not a
/// liveness input at all.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_two_lagging_sources_of_three_do_not_hold_the_head_back() -> Result<()> {
    let mut h = Harness::start(config(3)).await?;

    h.produce(20)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;

    for _ in 0..12 {
        h.produce_ahead(10)?;
        h.finalize_with_lag(5)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    h.settle().await?;
    h.assert_conforms().await?;

    Ok(())
}

/// FM-SRC / FM-1 — **one** endpoint of three signals a fork above its own tip, which RP-5b
/// confines to `from == tip + 1`. A source defect, and two honest endpoints are still on the head.
///
/// It wedges anyway, and takes no majority: `1 > 3/2` and `1 == 3` are both false, so the door is
/// the 2 s `fork_consensus_timeout` — reached only on a poll where *every* endpoint went
/// `Pending`, hence the 3 s lull. Measured 2026-07-20, 5/5 runs:
/// `forks=1 endpoints=3 active=3 majority=false all_active=false timeout=true`, then
/// `compute_rollback` rejects the liar's stale hints as below the finalized head and the epoch
/// parks for `P-EPOCH-RETRY`.
///
/// At a 50 ms cadence the same fault signals repeatedly and the service conforms:
/// `accept_new_block` clears the timer on every commit, so on a chain with block time ≥ 2 s every
/// inter-block gap is a firing window.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "one fork signal above the tip parks ingestion — no alarm, no recovery"]
async fn ct4_a_single_source_signalling_a_fork_above_its_tip_does_not_park_ingestion() -> Result<()> {
    let mut h = Harness::start(config(3)).await?;

    h.produce(20)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;

    h.peers[0].inject_fault(&h.dataset, |f| f.fork_signal_above_tip = true);

    for _ in 0..8 {
        h.produce_lagging(&[0], 10)?;
        h.finalize_with_lag(5)?;
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    h.settle().await?;
    h.assert_conforms().await?;

    Ok(())
}

/// The same defect on two of three — the other door: `forks > endpoints.len() / 2` carries
/// outright, no lull needed. Kept beside the single-liar script because a fix closing only the
/// majority path would leave that one green while a lone bad source still wedges.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a fork-signal majority parks ingestion — no alarm, no recovery"]
async fn ct4_a_fork_signal_majority_above_the_tip_does_not_park_ingestion() -> Result<()> {
    let mut h = Harness::start(config(3)).await?;

    h.produce(20)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;

    for peer in &h.peers {
        peer.inject_fault(&h.dataset, |f| f.fork_signal_above_tip = true);
    }

    for _ in 0..12 {
        h.produce_ahead(10)?;
        h.finalize_with_lag(5)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    h.settle().await?;
    h.assert_conforms().await?;

    Ok(())
}

/// The laggard catches up. Nothing may change: it re-serves blocks the service already holds, and
/// re-offered known blocks must be ignored rather than reopen a settled window (INV-11).
#[tokio::test(flavor = "multi_thread")]
async fn ct4_a_caught_up_source_changes_nothing() -> Result<()> {
    let mut h = Harness::start(config(2)).await?;

    h.produce(20)?;
    h.produce_ahead(40)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;

    h.catch_up_peers()?;
    h.produce(5)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;
    h.assert_conforms().await?;

    Ok(())
}

/// The wedge above is invisible twice over: the fork signal is an `Ok` that never reaches
/// `on_error`, and the park is an `error!` a crash-looping pod may never ship. Neither GAP-41 nor
/// GAP-5 is fixed here — this pins that both became countable, so a head held back by a lying
/// source reads differently from a hung service.
///
/// Not `#[ignore]`d unlike its neighbours: observability holds, conformance does not.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_a_fork_signal_above_the_tip_and_the_park_it_causes_are_observable() -> Result<()> {
    let mut h = Harness::start(config(3)).await?;

    h.produce(20)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;

    for peer in &h.peers {
        peer.inject_fault(&h.dataset, |f| f.fork_signal_above_tip = true);
    }

    for _ in 0..12 {
        h.produce_ahead(10)?;
        h.finalize_with_lag(5)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // No `settle` — the epoch is serving out `P-EPOCH-RETRY`, which is the thing being measured.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let metrics = h.client.metrics().await?;

    let above_tip = metrics
        .get("hotblocks_ingest_fork_signals_total", Some(("standing", "above_tip")))
        .unwrap_or_default();
    assert!(above_tip > 0.0, "a fork signalled above the source's own tip");

    assert!(
        metrics
            .get("hotblocks_ingest_fork_signals_total", Some(("standing", "at_tip")))
            .is_none(),
        "no source held the contested position"
    );

    let parked = metrics
        .get(
            "hotblocks_dataset_epoch_failures_total",
            Some(("reason", "unapplicable_fork"))
        )
        .unwrap_or_default();
    assert!(parked > 0.0, "the park must name the divergence as its cause");

    Ok(())
}

/// The other half: an honest reorg must land in `at_tip`. Without it the label above is
/// unfalsifiable — a discriminator stuck on `above_tip` would pass that script and then cry
/// source defect on every ordinary reorg in production.
#[tokio::test(flavor = "multi_thread")]
async fn ct4_an_honest_reorg_is_not_attributed_to_the_source() -> Result<()> {
    let mut h = Harness::start(config(1)).await?;

    h.produce(20)?;
    h.settle().await?;

    h.fork(1_010, 10)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;
    h.assert_conforms().await?;

    let metrics = h.client.metrics().await?;

    let at_tip = metrics
        .get("hotblocks_ingest_fork_signals_total", Some(("standing", "at_tip")))
        .unwrap_or_default();
    assert!(at_tip > 0.0, "the reorg reaches the service as a fork signal");

    assert!(
        metrics
            .get("hotblocks_ingest_fork_signals_total", Some(("standing", "above_tip")))
            .is_none(),
        "the source signalled where it stood — attributing this to a defect would make the \
         metric fire on every reorg"
    );

    Ok(())
}
