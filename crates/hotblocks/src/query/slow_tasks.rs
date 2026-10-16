//! Query tasks that hold a query pool worker for long: every task queued
//! behind one waits for it.

use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use sqd_storage::db::DatasetId;
use tracing::warn;

use super::{
    executor::TaskTime,
    running::{RunningQuery, RunningQueryStats}
};
use crate::{
    log_window::{LogWindow, query_fingerprint, truncate_query},
    metrics::{QueryStep, report_slow_query_task},
    settings::SettingsRef,
    types::ClientId
};

pub type SlowTasksRef = Arc<SlowTasks>;

pub struct SlowTasks {
    settings: SettingsRef,
    log_window: Mutex<LogWindow>
}

/// A finished task of a query stream. The read counts are `None` for a task
/// that failed before it had a running query.
pub struct TaskReport<'a> {
    pub dataset_id: DatasetId,
    pub client_id: &'a ClientId,
    /// The request as the client sent it.
    pub body: &'a [u8],
    pub step: QueryStep,
    pub time: TaskTime,
    pub failed: bool,
    pub chunks_read: Option<u64>,
    pub blocks_read: Option<u64>,
    pub buffered_bytes: Option<usize>
}

impl SlowTasks {
    pub fn new(settings: SettingsRef) -> Self {
        Self {
            settings,
            log_window: Mutex::new(LogWindow::new())
        }
    }

    /// Count a slow task, and log it while its query shape and the log limit allow.
    pub fn record(&self, task: &TaskReport) {
        let settings = self.settings.slow_tasks();
        if task.time.own() < settings.threshold() {
            return;
        }
        report_slow_query_task(task.dataset_id, task.client_id, task.step);

        let fingerprint = query_fingerprint(task.dataset_id, task.body);
        if !self
            .log_window
            .lock()
            .claim(fingerprint, "slow_task", settings.log_limit)
        {
            return;
        }

        warn!(
            dataset = %task.dataset_id,
            client_id = task.client_id.as_str(),
            step = task.step.as_str(),
            own_ms = task.time.own().as_secs_f64() * 1000.0,
            nested_ms = task.time.nested.as_secs_f64() * 1000.0,
            failed = task.failed,
            chunks_read = task.chunks_read,
            blocks_read = task.blocks_read,
            buffered_bytes = task.buffered_bytes,
            query = truncate_query(String::from_utf8_lossy(task.body).into_owned()),
            query_bytes = task.body.len(),
            "query task held a query pool worker for long"
        );
    }
}

/// Reports the tasks of one query stream to [`SlowTasks`].
pub struct TaskReporter {
    slow_tasks: SlowTasksRef,
    dataset_id: DatasetId,
    client_id: ClientId,
    body: Bytes
}

impl TaskReporter {
    pub fn new(slow_tasks: SlowTasksRef, dataset_id: DatasetId, client_id: ClientId, body: Bytes) -> Self {
        Self {
            slow_tasks,
            dataset_id,
            client_id,
            body
        }
    }

    /// `stats_before` are the stream's read counts when the task started.
    pub fn record(
        &self,
        step: QueryStep,
        time: TaskTime,
        runner: Option<&RunningQuery>,
        stats_before: &RunningQueryStats,
        failed: bool
    ) {
        let stats = runner.map(RunningQuery::stats);
        self.slow_tasks.record(&TaskReport {
            dataset_id: self.dataset_id,
            client_id: &self.client_id,
            body: &self.body,
            step,
            time,
            failed,
            chunks_read: stats.map(|stats| stats.chunks_read.saturating_sub(stats_before.chunks_read)),
            blocks_read: stats.map(|stats| stats.blocks_read.saturating_sub(stats_before.blocks_read)),
            buffered_bytes: runner.map(RunningQuery::buffered_bytes)
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        metrics::build_metrics_registry,
        settings::{QuerySettings, RuntimeSettings, Settings, ShadowSettings, SlowTaskSettings}
    };

    #[test]
    fn time_of_nested_tasks_does_not_make_a_task_slow() {
        let settings = RuntimeSettings::new(Settings {
            shadow: ShadowSettings {
                percent: 0.0,
                slow_ratio: 2.0,
                slow_margin_ms: 50,
                log_limit: 0
            },
            slow_tasks: SlowTaskSettings {
                threshold_ms: 500,
                log_limit: 0
            },
            query: QuerySettings { thread_per_task: false }
        })
        .unwrap();
        let slow_tasks = SlowTasks::new(Arc::new(settings));
        let client_id = ClientId::new("slow-task-test");
        let record = |own_ms: u64, nested_ms: u64| {
            slow_tasks.record(&TaskReport {
                dataset_id: DatasetId::from_str("slow-task-test"),
                client_id: &client_id,
                body: b"{}",
                step: QueryStep::Next,
                time: TaskTime {
                    total: Duration::from_millis(own_ms + nested_ms),
                    nested: Duration::from_millis(nested_ms)
                },
                failed: false,
                chunks_read: Some(1),
                blocks_read: Some(1),
                buffered_bytes: Some(0)
            })
        };

        record(499, 0);
        record(400, 600);
        record(500, 0);

        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &build_metrics_registry()).unwrap();
        let counted = output
            .lines()
            .find(|line| line.starts_with("hotblocks_query_slow_tasks_total") && line.contains("slow-task-test"))
            .unwrap_or_else(|| panic!("no slow task series:\n{output}"));
        assert!(counted.ends_with(" 1"), "{counted}");
    }
}
