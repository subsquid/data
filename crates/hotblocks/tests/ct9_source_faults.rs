//! CT-9 — source-fault corpus (spec 12 §3): a fault at the source surface must not take the
//! write path down with it (FM-1, LIV-2).
//!
//! CT-1 hit this one on the harness's first run: a JSONL body whose last record had no trailing
//! newline panicked `LineStream::take_final_line`, killing the ingest task and parking the
//! dataset for `P-EPOCH-RETRY` — forever, since the source serves the same body on retry.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use sqd_hotblocks_harness::{
    chain::HlFills,
    harness::{Harness, HarnessConfig}
};

const START: u64 = 1_000;

/// An unterminated record is still a record: the service must ingest it and keep ingesting.
#[tokio::test(flavor = "multi_thread")]
async fn ct9_unterminated_final_record_does_not_stall_ingestion() -> Result<()> {
    let mut h = Harness::start(HarnessConfig::from_block(
        env!("CARGO_BIN_EXE_sqd-hotblocks"),
        Arc::new(HlFills),
        START
    ))
    .await?;

    h.sim.inject_fault(&h.dataset, |f| f.unterminated_final_line = true);

    if let Err(err) = run(&mut h).await {
        panic!("CT-9 failed: {err:?}");
    }
    Ok(())
}

async fn run(h: &mut Harness) -> Result<()> {
    h.produce(20)?;
    h.finalize_with_lag(5)?;

    // The whole assertion: it converges. A panicked ingest task would sit out `P-EPOCH-RETRY`
    // and this would time out on an empty window.
    h.settle().await?;
    h.assert_conforms().await?;

    // And keeps going — not a fault the service survives exactly once.
    h.produce(10)?;
    h.finalize_with_lag(5)?;
    h.settle().await?;
    h.assert_conforms().await?;

    assert!(
        h.sut.last_startup < Duration::from_secs(60),
        "sanity: the service was never restarted by the harness"
    );
    Ok(())
}
