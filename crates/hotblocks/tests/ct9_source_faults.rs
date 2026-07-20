//! CT-9 — source-fault corpus (spec 12 §3): a fault at the source surface must not take the
//! write path down with it (FM-1, LIV-2).
//!
//! CT-1 hit this one on the harness's first run: a JSONL body whose last record had no trailing
//! newline panicked `LineStream::take_final_line`, killing the ingest task and parking the
//! dataset for `P-EPOCH-RETRY` — forever, since the source serves the same body on retry.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use sqd_hotblocks_harness::{
    chain::{Chain, Evm, HlFills},
    driver::Client,
    harness::{Harness, HarnessConfig},
    sut::{DatasetSpec, Retention, Sut, SutConfig}
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

/// Total outage is the one source fault the ingest counter could not see. The head probe gates
/// ingestion and its only exit on total failure is `completed > 0`, so with every source down it
/// spins there forever, `StandardDataSource` is never constructed and `on_error` never runs — the
/// counter would read zero through precisely the outage it exists to expose, while the service
/// stays up and scraped.
///
/// The assertion keys on `source` rather than `kind` so it also pins the label as the full
/// endpoint: a host-only label collapses every source of every dataset into one series.
#[tokio::test(flavor = "multi_thread")]
async fn ct9_a_total_source_outage_is_counted_before_ingestion_starts() -> Result<()> {
    const DS: &str = "outage";

    // Bound to claim a port, then released: probes are refused rather than left hanging.
    let dead = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();

    let sut = Sut::start(SutConfig::new(
        env!("CARGO_BIN_EXE_sqd-hotblocks"),
        vec![DatasetSpec {
            id: DS.to_string(),
            kind: Evm.config_kind().to_string(),
            // `Head` is what routes the dataset through the probe at all.
            retention: Retention::Head(100),
            sources: vec![format!("http://127.0.0.1:{dead}/{DS}")]
        }]
    ))
    .await?;

    let client = Client::new(sut.base_url(), DS)?;
    let source = format!("127.0.0.1:{dead}/{DS}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

    loop {
        let metrics = client.metrics().await?;
        if metrics
            .get("hotblocks_ingest_source_errors_total", Some(("source", &source)))
            .unwrap_or_default()
            > 0.0
        {
            assert!(
                metrics
                    .get("hotblocks_ingest_source_errors_total", Some(("kind", "connect")))
                    .unwrap_or_default()
                    > 0.0,
                "a refused connection must classify as `connect`"
            );
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "ingestion stalled on an unreachable source without counting a single error"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
