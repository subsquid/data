//! CT-7 — moving-window churn (GAP-1/GAP-6).

use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use sqd_hotblocks_harness::{
    ChurnSoakConfig,
    chain::HlFills,
    harness::{Harness, HarnessConfig},
    sut::Retention
};

const START: u64 = 1_000;

/// The real S4 runner is intentionally ignored in the ordinary unit-test lane: its default
/// includes two cleanup periods so physical debt has a chance to converge. Run explicitly with
/// `cargo test -p sqd-hotblocks --test ct7_stall_and_churn ct7_churn_soak -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "endurance scenario; run explicitly"]
async fn ct7_churn_soak() -> Result<()> {
    let mut harness_cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(HlFills), START);
    harness_cfg.retention = Retention::Api;
    harness_cfg
        .sut_args
        .extend(["--disk-reclaim-interval-secs".into(), "2".into()]);
    harness_cfg.rust_log = "sqd_hotblocks=debug".into();
    let mut h = Harness::start(harness_cfg).await?;

    let report = h.run_churn_soak(&ChurnSoakConfig::default()).await?;
    ensure!(!report.samples.is_empty());
    ensure!(report.samples.iter().any(|sample| sample.live_bytes > 0));
    ensure!(report.longest_zero_commit < Duration::from_secs(5));
    ensure!(report.pressure_queries_completed > 0);
    ensure!(
        report.pressure_query_failures == 0,
        "query pressure observed {} failure(s); first: {}",
        report.pressure_query_failures,
        report.first_pressure_query_failure.as_deref().unwrap_or("unknown")
    );
    ensure!(
        h.sut.log_tail(200).contains("snapshot-aware disk reclaim completed"),
        "periodic runtime reclaim did not run under query pressure"
    );
    Ok(())
}
