//! CT-10 — retention may pass the finalized head; finality does not move back, so the
//! watermark must survive below the window (RS-2 as revised, INV-5).
//!
//! Covers the three client-visible halves of the behavior: a 200 from the window keeps
//! reporting the head below it, the NO_DATA answers (a live query above the head and a
//! finalized-only query from the window start) carry the watermark too, and a report that
//! climbs back into the window raises it again.

use std::{
    sync::Arc,
    time::{Duration, Instant}
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use sqd_hotblocks_harness::{
    chain::Evm,
    driver::{Outcome, StatusData},
    harness::{Harness, HarnessConfig},
    sut::Retention,
    types::{BlockNumber, BlockRef}
};

const START: u64 = 1_000;
/// `Head(n)`: small enough that the second run pushes the window past the finalized head.
const WINDOW: u64 = 50;
/// Finality lag while it still sits inside the window: `lag < WINDOW` blocks.
const INSIDE_LAG: u64 = 30;
const FIRST_RUN: u32 = 200;
const SECOND_RUN: u32 = 100;
const POLL: Duration = Duration::from_millis(100);
const CONVERGE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn ct10_the_finalized_head_survives_a_window_that_passes_it() -> Result<()> {
    let cfg = HarnessConfig {
        retention: Retention::Api,
        // One chunk per source response, so the trim's floor lands on real boundaries.
        disable_compaction: true,
        ..HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START)
    };
    let mut h = Harness::start(cfg).await?;

    // The source holds the whole first run and its (lagging) finality before the service
    // ingests anything, so the window lands deterministically on top of both.
    h.produce(FIRST_RUN)?;
    h.finalize_with_lag(INSIDE_LAG)?;
    let fin = h
        .model
        .fin
        .clone()
        .context("the script finalized nothing before the window was set")?;

    let status = h.client.set_retention(&json!({ "Head": WINDOW })).await?;
    ensure!(status == 200, "SET-RETENTION was refused with {status}");

    // Wait for the first run to commit as a chunk that ends at the tip: the second run
    // must not arrive while the ingest still streams the first one, or a single response
    // commits one chunk spanning the finalized head.
    await_head_and_finality(&h, fin.clone()).await?;

    // Finality stalls while the window advances: the trim passes the finalized head.
    h.produce(SECOND_RUN)?;

    let data = await_window_past(&h, fin.number).await?;
    ensure!(
        fin.number < data.first_block,
        "the window [{}, {}] never passed the finalized head {}",
        data.first_block,
        data.last_block,
        fin.number
    );

    // FINALIZED-HEAD itself keeps the pre-trim value.
    let observed = h.client.finalized_head().await?;
    ensure!(
        observed.as_ref() == Some(&fin),
        "FINALIZED-HEAD lost the watermark below the window: expected {fin:?}, got {observed:?}"
    );

    // A 200 from inside the window still classifies the whole window as unfinalized.
    let from = data.first_block;
    let query = h.chain.scan_query(from, Some(from + 10), None);
    match h.client.query(&query).await? {
        Outcome::Ok { blocks, watermarks } => {
            ensure!(!blocks.is_empty(), "the window scan covered nothing");
            ensure!(
                watermarks.finalized.as_ref() == Some(&fin),
                "a 200 from the window lost the finalized head: {watermarks:?}"
            );
        }
        other => bail!("the window scan did not answer 200: {other:?}")
    }

    // NO_DATA carries the watermark too: a live query above the head, and a finalized-only
    // query whose whole range is unfinalized while the head sits below the window.
    let above_head = h.chain.scan_query(data.last_block + 1, None, None);
    let finalized_from_window = h.chain.scan_query(from, None, None);
    let (live, finalized) = tokio::join!(
        h.client.query(&above_head),
        h.client.query_finalized(&finalized_from_window)
    );
    let checks = [
        ("a live query above the head", live?),
        ("a finalized-only query from the window", finalized?)
    ];
    for (label, outcome) in checks {
        match outcome {
            Outcome::NoData { watermarks } => ensure!(
                watermarks.finalized.as_ref() == Some(&fin),
                "{label} lost the finalized head on NO_DATA: {watermarks:?}"
            ),
            other => bail!("{label} did not answer NO_DATA: {other:?}")
        }
    }

    // Finality climbing back into the window raises the watermark again.
    let recovered_at = data.last_block - 20;
    h.finalize(recovered_at)?;
    let expected = h
        .model
        .fin
        .clone()
        .context("the recovery finalize did not apply in the model")?;
    let observed = await_finalized_head(&h, &expected).await?;
    ensure!(
        observed == Some(expected),
        "finality inside the window did not raise the head: got {observed:?}"
    );

    Ok(())
}

/// Waits until the service holds the model's head and watermark. The model never trims,
/// so the window itself is not compared here.
async fn await_head_and_finality(h: &Harness, fin: BlockRef) -> Result<()> {
    let expected_head = h
        .model
        .head()
        .context("the model has no head before the window was set")?;
    let deadline = Instant::now() + CONVERGE;
    loop {
        let head = h.client.head().await?;
        let observed_fin = h.client.finalized_head().await?;
        if head.as_ref() == Some(&expected_head) && observed_fin.as_ref() == Some(&fin) {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "the service did not reach head {expected_head:?} / fin {fin:?}: got {head:?} / {observed_fin:?}"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Waits until the whole chain is in and the window starts above `fin`.
async fn await_window_past(h: &Harness, fin: BlockNumber) -> Result<StatusData> {
    let expected_head = h
        .model
        .head()
        .context("the model has no head after the second run")?
        .number;
    let deadline = Instant::now() + CONVERGE;
    loop {
        let status = h.client.status().await?;
        if let Some(data) = &status.data
            && data.last_block == expected_head
            && data.first_block > fin
        {
            return Ok(data.clone());
        }
        ensure!(
            Instant::now() < deadline,
            "the service did not converge on a window past {fin}: {:?}",
            status.data
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn await_finalized_head(h: &Harness, expected: &BlockRef) -> Result<Option<BlockRef>> {
    let deadline = Instant::now() + CONVERGE;
    loop {
        let observed = h.client.finalized_head().await?;
        if observed.as_ref() == Some(expected) {
            return Ok(observed);
        }
        ensure!(
            Instant::now() < deadline,
            "FINALIZED-HEAD never reached {expected:?}: got {observed:?}"
        );
        tokio::time::sleep(POLL).await;
    }
}
