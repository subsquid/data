//! A query task's wait in the query pool's queue and its run on a worker are measured apart:
//! `active_queries` and `completed_queries` only give the mean of their sum.

use std::sync::Arc;

use anyhow::{Result, ensure};
use sqd_hotblocks_harness::{
    Harness, HarnessConfig, Outcome,
    chain::{Chain, Evm},
    driver::Metrics
};

const START: u64 = 1_000;
const QUEUE: &str = "hotblocks_query_task_queue_seconds_count";
const RUN: &str = "hotblocks_query_task_run_seconds_count";

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
