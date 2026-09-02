//! CT-2 — exercise the real process-level SIGTERM path (LIV-12, GAP-17).

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use reqwest::StatusCode;
use sqd_hotblocks_harness::{
    chain::HlFills,
    harness::{Harness, HarnessConfig}
};

const START: u64 = 1_000;
const PRE_DRAIN_GRACE: Duration = Duration::from_secs(2);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_marks_unready_serves_through_grace_and_bounds_the_drain() -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(HlFills), START);
    cfg.sut_args.extend([
        "--pre-drain-grace-secs".into(),
        PRE_DRAIN_GRACE.as_secs().to_string(),
        "--drain-timeout-secs".into(),
        DRAIN_TIMEOUT.as_secs().to_string()
    ]);
    let mut harness = Harness::start(cfg).await?;

    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;
    let base_url = harness.sut.base_url();
    let ready_url = format!("{base_url}/ready");
    let stream_url = format!("{base_url}/datasets/{}/stream", harness.dataset);

    assert_eq!(http.get(&ready_url).send().await?.status(), StatusCode::OK);

    // With no blocks produced, this real request waits for P-HEAD-WAIT (5s). It must still
    // be in flight when the shorter 2s grace + 1s drain deadline expires.
    let query = harness.chain.scan_query(START, None, None);
    let long_poll = tokio::spawn({
        let http = http.clone();
        async move { http.post(stream_url).json(&query).send().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    ensure!(
        !long_poll.is_finished(),
        "the long-poll request did not remain in flight"
    );

    harness.sut.signal_shutdown()?;
    await_status(&http, &ready_url, StatusCode::SERVICE_UNAVAILABLE).await?;

    // The endpoint-removal window must not interrupt traffic that reached this replica through
    // a stale route or an existing keep-alive connection.
    assert_eq!(http.get(&base_url).send().await?.status(), StatusCode::OK);

    let report = harness
        .sut
        .wait_for_shutdown(PRE_DRAIN_GRACE + DRAIN_TIMEOUT + Duration::from_secs(3))
        .await?;

    assert!(report.status.success(), "shutdown exited with {}", report.status);
    assert!(
        report.took >= PRE_DRAIN_GRACE + DRAIN_TIMEOUT - Duration::from_millis(250),
        "process exited before the configured grace and drain deadline: {:?}",
        report.took
    );
    assert!(
        report.took < PRE_DRAIN_GRACE + DRAIN_TIMEOUT + Duration::from_secs(2),
        "process exceeded the configured shutdown bound: {:?}",
        report.took
    );

    let request_result = long_poll.await.context("long-poll task panicked")?;
    ensure!(
        request_result.is_err(),
        "the deadline must abort a response that is still waiting for data"
    );

    Ok(())
}

async fn await_status(http: &reqwest::Client, url: &str, expected: StatusCode) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if let Ok(response) = http.get(url).send().await
            && response.status() == expected
        {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "{url} did not start returning {expected} after SIGTERM"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
