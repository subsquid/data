//! CT-7 liveness and endurance probes derived from black-box watermarks and the service's
//! existing RocksDB diagnostic-property binding.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering}
    },
    time::{Duration, Instant}
};

use anyhow::{Result, bail, ensure};
use serde_json::json;

use crate::{
    chain::Chain,
    driver::{Client, Outcome},
    harness::Harness,
    types::BlockNumber
};

#[derive(Clone, Debug)]
pub struct StallProbe {
    pub poll: Duration,
    pub timeout: Duration,
    pub stall_budget: Duration
}

impl Default for StallProbe {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(100),
            timeout: Duration::from_secs(30),
            stall_budget: Duration::from_secs(5)
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StallReport {
    pub longest_zero_commit: Duration,
    pub intervals_over_one_second: u64,
    pub crossed_budget: bool,
    pub saw_storage_diagnostic_surface: bool
}

impl StallProbe {
    /// Observes a source-ahead interval until `target` becomes query-visible (SLI-1/SLI-9).
    pub async fn observe_ingest(&self, client: &Client, target: BlockNumber) -> Result<StallReport> {
        let deadline = Instant::now() + self.timeout;
        let mut report = StallReport::default();
        let mut last_head = None;
        let mut zero_since = Instant::now();
        let mut counted_interval = false;
        loop {
            let status = client.status().await?;
            let head = status.data.as_ref().map(|data| data.last_block);
            if head != last_head {
                let interval = zero_since.elapsed();
                report.longest_zero_commit = report.longest_zero_commit.max(interval);
                last_head = head;
                zero_since = Instant::now();
                counted_interval = false;
            } else {
                let interval = zero_since.elapsed();
                report.longest_zero_commit = report.longest_zero_commit.max(interval);
                if interval >= Duration::from_secs(1) && !counted_interval {
                    report.intervals_over_one_second = report.intervals_over_one_second.saturating_add(1);
                    counted_interval = true;
                }
            }

            if !report.saw_storage_diagnostic_surface {
                report.saw_storage_diagnostic_surface = client
                    .rocksdb_property("TABLES", "rocksdb.is-write-stopped")
                    .await?
                    .is_some()
                    && client
                        .rocksdb_property("TABLES", "rocksdb.estimate-pending-compaction-bytes")
                        .await?
                        .is_some();
            }

            if head.is_some_and(|head| head >= target) {
                break;
            }
            if Instant::now() >= deadline {
                bail!("SLI-9: ingest did not reach block {target} within {:?}", self.timeout);
            }
            tokio::time::sleep(self.poll).await;
        }

        report.crossed_budget = report.longest_zero_commit >= self.stall_budget;
        Ok(report)
    }
}

#[derive(Clone, Debug)]
pub struct ChurnSoakConfig {
    pub retention_blocks: u64,
    pub initial_blocks: u32,
    pub rounds: u32,
    pub blocks_per_round: u32,
    pub round_pause: Duration,
    pub reclaim_settle: Duration,
    /// Concurrent clients that continuously scan the moving retention window.
    pub query_concurrency: usize,
    pub stall_probe: StallProbe
}

impl Default for ChurnSoakConfig {
    fn default() -> Self {
        Self {
            retention_blocks: 100,
            initial_blocks: 150,
            rounds: 30,
            blocks_per_round: 20,
            round_pause: Duration::from_millis(200),
            reclaim_settle: Duration::from_secs(22),
            query_concurrency: 2,
            stall_probe: StallProbe::default()
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SpaceSample {
    pub disk_bytes: u64,
    pub live_bytes: u64,
    pub debt_bytes: u64
}

impl SpaceSample {
    pub fn amplification(&self) -> Option<f64> {
        (self.live_bytes > 0).then(|| self.disk_bytes as f64 / self.live_bytes as f64)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChurnSoakReport {
    pub samples: Vec<SpaceSample>,
    pub longest_zero_commit: Duration,
    pub max_window_excess: u64,
    pub pressure_queries_completed: u64,
    pub pressure_query_failures: u64,
    pub first_pressure_query_failure: Option<String>
}

impl Harness {
    /// Runs S4 churn against an API-controlled dataset and samples RocksDB space properties.
    pub async fn run_churn_soak(&mut self, cfg: &ChurnSoakConfig) -> Result<ChurnSoakReport> {
        let anchor = self.sim.anchor_hash(&self.dataset);
        ensure!(
            self.client
                .set_retention(&json!({"FromBlock": {"number": self.start_block, "parent_hash": anchor}}))
                .await?
                == 200,
            "CT-7 requires an API-controlled dataset"
        );

        self.produce(cfg.initial_blocks)?;
        let initial_target = self.model.head().expect("initial blocks were produced").number;
        cfg.stall_probe.observe_ingest(&self.client, initial_target).await?;
        ensure!(
            self.client
                .set_retention(&json!({"Head": cfg.retention_blocks}))
                .await?
                == 200,
            "failed to enable moving-window retention"
        );

        let query_window = Arc::new(QueryWindow {
            from: AtomicU64::new(self.start_block),
            to: AtomicU64::new(initial_target)
        });
        let query_pressure = QueryPressure::start(
            self.client.clone(),
            Arc::clone(&self.chain),
            Arc::clone(&query_window),
            cfg.query_concurrency
        );

        let mut report = ChurnSoakReport::default();
        for _ in 0..cfg.rounds {
            self.produce(cfg.blocks_per_round)?;
            let head = self.model.head().expect("churn produced a head").number;
            let desired_floor = head.saturating_add(1).saturating_sub(cfg.retention_blocks);
            self.model.retain(desired_floor, None);
            query_window.from.store(desired_floor, Ordering::Relaxed);
            query_window.to.store(head, Ordering::Relaxed);

            let stall = cfg.stall_probe.observe_ingest(&self.client, head).await?;
            report.longest_zero_commit = report.longest_zero_commit.max(stall.longest_zero_commit);
            let status = self.client.status().await?;
            let first = status
                .data
                .as_ref()
                .expect("ingested dataset has status data")
                .first_block;
            ensure!(
                first <= desired_floor,
                "RS-3: retention dropped required data (first {first}, required floor {desired_floor})"
            );
            report.max_window_excess = report.max_window_excess.max(desired_floor.saturating_sub(first));
            report.samples.push(sample_space(&self.client).await?);
            tokio::time::sleep(cfg.round_pause).await;
        }

        let before_settle = sample_space(&self.client).await?;
        tokio::time::sleep(cfg.reclaim_settle).await;
        let after_settle = sample_space(&self.client).await?;
        ensure!(
            after_settle.debt_bytes <= before_settle.debt_bytes,
            "LIV-7: reclaim debt grew while churn was quiescent ({} -> {} bytes)",
            before_settle.debt_bytes,
            after_settle.debt_bytes
        );
        report.samples.extend([before_settle, after_settle]);
        (
            report.pressure_queries_completed,
            report.pressure_query_failures,
            report.first_pressure_query_failure
        ) = query_pressure.finish().await;
        Ok(report)
    }
}

struct QueryWindow {
    from: AtomicU64,
    to: AtomicU64
}

struct QueryPressure {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<(u64, u64, Option<String>)>>
}

impl QueryPressure {
    fn start(client: Client, chain: Arc<dyn Chain>, window: Arc<QueryWindow>, concurrency: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let client = client.clone();
            let chain = Arc::clone(&chain);
            let window = Arc::clone(&window);
            let stop = Arc::clone(&stop);
            tasks.push(tokio::spawn(async move {
                let mut completed = 0u64;
                let mut failed = 0u64;
                let mut first_failure = None;
                while !stop.load(Ordering::Relaxed) {
                    let from = window.from.load(Ordering::Relaxed);
                    let to = window.to.load(Ordering::Relaxed).max(from);
                    let query = chain.scan_query(from, Some(to), None);
                    if let Some(message) = pressure_query_failure(client.query(&query).await) {
                        failed = failed.saturating_add(1);
                        first_failure.get_or_insert(message);
                    } else {
                        completed = completed.saturating_add(1);
                    }
                }
                (completed, failed, first_failure)
            }));
        }
        Self { stop, tasks }
    }

    async fn finish(mut self) -> (u64, u64, Option<String>) {
        self.stop.store(true, Ordering::Relaxed);
        let mut completed = 0u64;
        let mut failed = 0u64;
        let mut first_failure = None;
        for task in self.tasks.drain(..) {
            match task.await {
                Ok((task_completed, task_failed, task_first_failure)) => {
                    completed = completed.saturating_add(task_completed);
                    failed = failed.saturating_add(task_failed);
                    if first_failure.is_none() {
                        first_failure = task_first_failure;
                    }
                }
                Err(err) => {
                    failed = failed.saturating_add(1);
                    first_failure.get_or_insert_with(|| format!("query pressure task failed: {err}"));
                }
            }
        }
        (completed, failed, first_failure)
    }
}

fn pressure_query_failure(result: Result<Outcome>) -> Option<String> {
    match result {
        Ok(Outcome::Ok { .. } | Outcome::NoData { .. }) => None,
        Ok(Outcome::Error { status, body }) => Some(format!("HTTP {status}: {body}")),
        Ok(Outcome::Conflict { hints }) => Some(format!("HTTP 409 conflict: {hints:?}")),
        Err(err) => Some(format!("query transport error: {err:#}"))
    }
}

impl Drop for QueryPressure {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn sample_space(client: &Client) -> Result<SpaceSample> {
    let sst_bytes = client
        .rocksdb_property("TABLES", "rocksdb.total-sst-files-size")
        .await?
        .unwrap_or(0);
    let memtable_bytes = client
        .rocksdb_property("TABLES", "rocksdb.cur-size-all-mem-tables")
        .await?
        .unwrap_or(0);
    let live_sst_bytes = client
        .rocksdb_property("TABLES", "rocksdb.estimate-live-data-size")
        .await?
        .unwrap_or(sst_bytes)
        .min(sst_bytes);
    let disk_bytes = sst_bytes.saturating_add(memtable_bytes);
    let live_bytes = live_sst_bytes.saturating_add(memtable_bytes);
    Ok(SpaceSample {
        disk_bytes,
        live_bytes,
        debt_bytes: disk_bytes.saturating_sub(live_bytes)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::Watermarks;

    #[test]
    fn query_pressure_accepts_only_success_and_no_data() {
        assert!(
            pressure_query_failure(Ok(Outcome::NoData {
                watermarks: Watermarks::default()
            }))
            .is_none()
        );
        assert_eq!(
            pressure_query_failure(Ok(Outcome::Error {
                status: 500,
                body: "broken reader".into()
            }))
            .as_deref(),
            Some("HTTP 500: broken reader")
        );
        assert!(pressure_query_failure(Ok(Outcome::Conflict { hints: Vec::new() })).is_some());
    }
}
