use super::executor::{QueryExecutor, QuerySlot};
use super::running::{RunningQuery, RunningQueryStats};
use crate::errors::Busy;
use crate::metrics::{
    STREAM_BLOCKS, STREAM_BLOCKS_PER_SECOND, STREAM_BYTES, STREAM_BYTES_PER_SECOND, STREAM_CHUNKS,
    STREAM_DURATIONS,
};
use crate::types::DBRef;
use anyhow::bail;
use bytes::Bytes;
use sqd_primitives::BlockRef;
use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::time::Duration;
use std::time::Instant;

const DEFAULT_QUERY_LIMIT: Duration = Duration::from_secs(10);

pub struct QueryResponse {
    executor: QueryExecutor,
    runner: Option<Box<RunningQuery>>,
    finalized_head: Option<BlockRef>,
    dataset_id: DatasetId,
    stats: QueryStreamStats,
    time_limit: Duration,
}

pub struct QueryStreamStats {
    response_chunks: u64,
    response_blocks: u64,
    response_bytes: u64,
    start_time: Instant,
}

impl QueryStreamStats {
    pub fn new() -> Self {
        Self {
            response_chunks: 0,
            response_blocks: 0,
            response_bytes: 0,
            start_time: Instant::now(),
        }
    }

    pub fn add_running_stats(&mut self, running_stats: &RunningQueryStats) {
        self.response_chunks = self
            .response_chunks
            .saturating_add(running_stats.chunks_read);
        self.response_blocks = self
            .response_blocks
            .saturating_add(running_stats.blocks_read);
    }

    fn report_metrics(&self, dataset_id: &DatasetId) {
        let labels = vec![("dataset_id".to_owned(), dataset_id.as_str().to_owned())];

        let duration = self.start_time.elapsed().as_secs_f64();
        let bytes = self.response_bytes as f64;
        let blocks = self.response_blocks as f64;
        let chunks = self.response_chunks as f64;

        STREAM_DURATIONS.get_or_create(&labels).observe(duration);
        STREAM_BYTES.get_or_create(&labels).observe(bytes);
        STREAM_BLOCKS.get_or_create(&labels).observe(blocks);
        STREAM_CHUNKS.get_or_create(&labels).observe(chunks);
        if duration > 0.0 {
            STREAM_BYTES_PER_SECOND.observe(bytes / duration);
            STREAM_BLOCKS_PER_SECOND
                .get_or_create(&labels)
                .observe(blocks / duration);
        }
    }
}

impl QueryResponse {
    pub(super) async fn new(
        executor: QueryExecutor,
        db: DBRef,
        dataset_id: DatasetId,
        query: Query,
        only_finalized: bool,
        time_limit: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let Some(slot) = executor.get_slot() else {
            bail!(Busy)
        };

        let stats = QueryStreamStats::new();
        let mut runner = slot
            .run(move |slot| -> anyhow::Result<_> {
                let mut runner =
                    RunningQuery::new(db, dataset_id, &query, only_finalized).map(Box::new)?;
                next_run(&mut runner, slot)?;
                Ok(runner)
            })
            .await?;

        let time_limit = time_limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let response = Self {
            executor,
            finalized_head: runner.take_finalized_head(),
            runner: Some(runner),
            stats,
            dataset_id,
            time_limit,
        };

        Ok(response)
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref()
    }

    pub async fn next_data_pack(&mut self) -> anyhow::Result<Option<Bytes>> {
        let Some(mut runner) = self.runner.take() else {
            return Ok(None);
        };

        if !runner.has_next_chunk() {
            return Ok(self.finish_with_runner(runner));
        }

        if self.stats.start_time.elapsed() > self.time_limit {
            // Client is expected to retry the query based on the data that they have received
            tracing::warn!(
                "terminate query that has been running for more than {} seconds",
                self.time_limit.as_secs()
            );
            return Ok(self.finish_with_runner(runner));
        }

        if runner.buffered_bytes() > 0 {
            let bytes = runner.take_buffered_bytes();
            self.stats.response_bytes =
                self.stats.response_bytes.saturating_add(bytes.len() as u64);
            self.runner = Some(runner);
            return Ok(Some(bytes));
        }

        let Some(slot) = self.executor.get_slot() else {
            self.runner = Some(runner);
            bail!(Busy);
        };

        let (mut runner, result) = slot
            .run(move |slot| {
                let mut runner = runner;
                let result = next_run(&mut runner, slot);
                (runner, result)
            })
            .await;

        if let Err(err) = result {
            self.runner = Some(runner);
            return Err(err);
        }

        if runner.has_next_chunk() {
            let bytes = runner.take_buffered_bytes();
            self.stats.response_bytes =
                self.stats.response_bytes.saturating_add(bytes.len() as u64);
            self.runner = Some(runner);
            Ok(Some(bytes))
        } else {
            return Ok(self.finish_with_runner(runner));
        }
    }

    fn finish_with_runner(&mut self, runner: Box<RunningQuery>) -> Option<Bytes> {
        runner.stats().report_metrics(&self.dataset_id);
        self.stats.add_running_stats(runner.stats());
        let bytes = runner.finish();
        self.stats.response_bytes = self.stats.response_bytes.saturating_add(bytes.len() as u64);
        Some(bytes)
    }

    pub fn finish(&mut self) -> Bytes {
        self.runner
            .take()
            .map(|runner| self.finish_with_runner(runner).unwrap())
            .unwrap_or_default()
    }
}

impl Drop for QueryResponse {
    fn drop(&mut self) {
        self.stats.report_metrics(&self.dataset_id)
    }
}

fn next_run(runner: &mut RunningQuery, slot: &QuerySlot) -> anyhow::Result<()> {
    let start = Instant::now();
    let mut elapsed = 0;
    loop {
        let beg = elapsed;
        let processed = runner.next_chunk_size();

        runner.write_next_chunk()?;

        if !runner.has_next_chunk() || runner.buffered_bytes() > 512 * 1024 {
            return Ok(());
        }

        elapsed = start.elapsed().as_millis();

        let chunk_time = elapsed - beg;
        let next_chunk_eta = chunk_time * runner.next_chunk_size() as u128 / processed as u128;
        let next_chunk_eta = next_chunk_eta.min(chunk_time * 5).max(chunk_time / 5);
        let eta = elapsed + next_chunk_eta;
        if eta > slot.time_limit() as u128 {
            return Ok(());
        }
    }
}
