//! A query task's wait in the query pool's queue and its run on a worker are measured apart:
//! `active_queries` and `completed_queries` only give the mean of their sum.

use std::sync::Arc;

use anyhow::{Result, ensure};
use serde_json::json;
use sqd_hotblocks_harness::{
    Harness, HarnessConfig, Outcome,
    chain::{Chain, Evm},
    driver::Metrics
};

const START: u64 = 1_000;
const QUEUE: &str = "hotblocks_query_task_queue_seconds_count";
const RUN: &str = "hotblocks_query_task_run_seconds_count";
const SLOW: &str = "hotblocks_query_slow_tasks_total";

#[tokio::test(flavor = "multi_thread")]
async fn a_query_task_is_timed_in_the_queue_and_on_a_worker() -> Result<()> {
    let cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    let mut h = Harness::start(cfg).await?;
    h.produce(10)?;
    h.settle().await?;

    let before = h.client.metrics().await?;
    let query = Evm.scan_query(START, None, None);
    let outcome = h.client.query(&query).await?;
    ensure!(
        matches!(outcome, Outcome::Ok { .. }),
        "{query} did not return data: {outcome:?}"
    );
    let after = h.client.metrics().await?;

    let tasks = |metric: &str, step: &str| {
        let count = |metrics: &Metrics| metrics.get(metric, Some(("step", step))).unwrap_or(0.0);
        count(&after) - count(&before)
    };

    assert_eq!(tasks(QUEUE, "first"), 1.0);
    assert_eq!(tasks(RUN, "first"), 1.0);
    // How many next steps follow depends on how the blocks were chunked.
    assert_eq!(tasks(QUEUE, "next"), tasks(RUN, "next"));
    assert_eq!(after.get("hotblocks_query_tasks_queued", None), Some(0.0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_over_the_slow_threshold_is_counted_and_the_threshold_changes_at_runtime() -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    cfg.sut_args
        .extend(["--slow-task-threshold-ms", "0"].into_iter().map(String::from));
    let mut h = Harness::start(cfg).await?;
    h.produce(10)?;
    h.settle().await?;
    let query = Evm.scan_query(START, None, None);

    let slow_first_tasks = async |h: &Harness| -> Result<f64> {
        let metrics = h.client.metrics().await?;
        Ok(metrics.get(SLOW, Some(("step", "first"))).unwrap_or(0.0))
    };

    let outcome = h.client.query(&query).await?;
    ensure!(
        matches!(outcome, Outcome::Ok { .. }),
        "{query} did not return data: {outcome:?}"
    );
    assert_eq!(slow_first_tasks(&h).await?, 1.0);

    assert_eq!(
        h.client
            .update_settings(&json!({"slowTasks": {"thresholdMs": 60_000}}))
            .await?,
        200
    );
    let outcome = h.client.query(&query).await?;
    ensure!(
        matches!(outcome, Outcome::Ok { .. }),
        "{query} did not return data: {outcome:?}"
    );
    assert_eq!(slow_first_tasks(&h).await?, 1.0, "a fast task was counted");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_first_task_that_fails_is_still_counted() -> Result<()> {
    let mut cfg = HarnessConfig::from_block(env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(Evm), START);
    cfg.sut_args
        .extend(["--slow-task-threshold-ms", "0"].into_iter().map(String::from));
    let mut h = Harness::start(cfg).await?;
    h.produce(10)?;
    h.settle().await?;

    let below_retained = Evm.scan_query(START - 1, None, None);
    let outcome = h.client.query(&below_retained).await?;
    ensure!(
        matches!(outcome, Outcome::Error { status: 400, .. }),
        "{below_retained} did not fail in its first task: {outcome:?}"
    );

    let metrics = h.client.metrics().await?;
    assert_eq!(metrics.get(SLOW, Some(("step", "first"))), Some(1.0));
    Ok(())
}
