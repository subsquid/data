use std::{fmt::Write, sync::LazyLock, time::Duration};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeLabelSet, EncodeLabelValue, LabelValueEncoder},
    metrics::{
        MetricType,
        counter::Counter,
        family::Family,
        histogram::{Histogram, exponential_buckets}
    },
    registry::Registry
};
use sqd_storage::db::DatasetId;

use crate::{
    data_service::DataServiceRef, dataset_controller::DatasetController, query::QueryExecutorCollector, types::DBRef
};

#[derive(Copy, Clone, Hash, Debug, Default, Ord, PartialOrd, Eq, PartialEq, EncodeLabelSet)]
struct DatasetLabel {
    dataset: DatasetValue
}

#[derive(Copy, Clone, Hash, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
struct DatasetValue(DatasetId);

impl EncodeLabelValue for DatasetValue {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        encoder.write_str(self.0.as_str())
    }
}

macro_rules! dataset_label {
    ($dataset_id:expr) => {
        DatasetLabel {
            dataset: DatasetValue($dataset_id)
        }
    };
}

type Labels = Vec<(&'static str, String)>;

fn buckets(start: f64, count: usize) -> impl Iterator<Item = f64> {
    std::iter::successors(Some(start), |x| Some(x * 10.))
        .flat_map(|x| [x, x * 1.5, x * 2.5, x * 5.0])
        .take(count)
}

pub static HTTP_STATUS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);
pub static HTTP_TTFB: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(0.001, 20))));

pub static QUERY_ERROR_TOO_MANY_TASKS: LazyLock<Counter> = LazyLock::new(Default::default);
pub static QUERY_ERROR_TOO_MANY_DATA_WAITERS: LazyLock<Counter> = LazyLock::new(Default::default);

pub static COMPLETED_QUERIES: LazyLock<Counter> = LazyLock::new(Default::default);

pub static STREAM_DURATIONS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(0.01, 2.0, 20))));
pub static STREAM_BYTES: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1000., 2.0, 20))));
pub static STREAM_BLOCKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30))));
pub static STREAM_CHUNKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(1., 20))));
pub static STREAM_BYTES_PER_SECOND: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(100., 3.0, 20))));
pub static STREAM_BLOCKS_PER_SECOND: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 3.0, 20))));

pub static QUERIED_BLOCKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30))));
pub static QUERIED_CHUNKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(1., 20))));

pub fn report_query_too_many_tasks_error() {
    QUERY_ERROR_TOO_MANY_TASKS.inc();
}

pub fn report_query_too_many_data_waiters_error() {
    QUERY_ERROR_TOO_MANY_DATA_WAITERS.inc();
}

pub fn report_http_response(labels: &Vec<(&'static str, String)>, to_first_byte: Duration) {
    HTTP_STATUS.get_or_create(&labels).inc();
    HTTP_TTFB.get_or_create(&labels).observe(to_first_byte.as_secs_f64());
}

pub struct DatasetMetricsCollector {
    pub data_service: DataServiceRef,
    pub datasets: Vec<DatasetId>
}

impl std::fmt::Debug for DatasetMetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetMetricsCollector")
            .field("datasets", &self.datasets)
            .finish_non_exhaustive()
    }
}

impl Collector for DatasetMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        for dataset_id in self.datasets.iter().copied() {
            // Read from in-memory controller state only - no per-scrape DB access.
            let Ok(controller) = self.data_service.get_dataset(dataset_id) else {
                continue;
            };
            collect_dataset_metrics(&mut encoder, dataset_id, &controller)?;
        }

        Ok(())
    }
}

fn collect_dataset_metrics(
    encoder: &mut DescriptorEncoder,
    dataset_id: DatasetId,
    controller: &DatasetController
) -> Result<(), std::fmt::Error> {
    let last_block = controller.get_head_block_number();
    let stats = controller.get_stats();

    // Nothing ingested yet - emit no series for this dataset.
    if last_block.is_none() && stats.first_block.is_none() {
        return Ok(());
    }

    if let Some(first_block) = stats.first_block {
        encoder
            .encode_descriptor("hotblocks_first_block", "First block", None, MetricType::Gauge)?
            .encode_family(&dataset_label!(dataset_id))?
            .encode_gauge(&first_block)?;
    }

    // `last_block` is the live head; `last_block_time` below is from lagging stats,
    // so the two can briefly disagree after a new block arrives.
    if let Some(last_block) = last_block {
        encoder
            .encode_descriptor("hotblocks_last_block", "Last block", None, MetricType::Gauge)?
            .encode_family(&dataset_label!(dataset_id))?
            .encode_gauge(&last_block)?;
    }

    if let Some(last_block_time) = stats.last_block_time {
        encoder
            .encode_descriptor(
                "hotblocks_last_block_timestamp_ms",
                "Timestamp of the last block",
                None,
                MetricType::Gauge
            )?
            .encode_family(&dataset_label!(dataset_id))?
            .encode_gauge(&last_block_time)?;
    }

    encoder
        .encode_descriptor(
            "hotblocks_last_finalized_block",
            "Last finalized block",
            None,
            MetricType::Gauge
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&controller.get_finalized_head().map_or(0, |h| h.number))?;

    // Skip until computed once, so a fresh process doesn't report a spurious zero.
    if let Some(size_bytes) = stats.size_bytes {
        encoder
            .encode_descriptor(
                "hotblocks_dataset_size_bytes",
                "Approximate on-disk size of the dataset's table data",
                None,
                MetricType::Gauge
            )?
            .encode_family(&dataset_label!(dataset_id))?
            .encode_gauge(&size_bytes)?;
    }

    Ok(())
}

pub type ColumnFamilySizes = Vec<(&'static str, u64)>;

/// On-disk size of each RocksDB column family. Unlike per-dataset sizes, this
/// also covers metadata CFs and orphaned table data. `total-sst-files-size`
/// holds RocksDB's DB mutex and scales with live SST files across all open
/// snapshots, so it's refreshed by [`storage_metrics_loop`], off the scrape path.
#[derive(Debug)]
pub struct StorageMetricsCollector {
    pub receiver: tokio::sync::watch::Receiver<ColumnFamilySizes>
}

impl Collector for StorageMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        for (cf, size) in self.receiver.borrow().iter() {
            let labels: Vec<(&'static str, String)> = vec![("column_family", cf.to_string())];
            encoder
                .encode_descriptor(
                    "hotblocks_column_family_size_bytes",
                    "Approximate on-disk size (total SST files) of a RocksDB column family",
                    None,
                    MetricType::Gauge
                )?
                .encode_family(&labels)?
                .encode_gauge(size)?;
        }

        Ok(())
    }
}

#[tracing::instrument(name = "storage_metrics", skip_all)]
pub async fn storage_metrics_loop(db: DBRef, interval: Duration, sender: tokio::sync::watch::Sender<ColumnFamilySizes>) {
    // Stagger the first run so this doesn't hit RocksDB's DB mutex (see
    // column_family_sizes docs) right at startup alongside every dataset's own
    // initial ingestion/compaction. Only one instance of this loop runs, so a
    // fixed delay suffices (unlike dataset_stats_loop's per-dataset offset).
    tokio::time::sleep(Duration::from_secs(5)).await;

    loop {
        let db = db.clone();
        let span = tracing::Span::current();
        let result = tokio::task::spawn_blocking(move || {
            let _s = span.enter();
            let started = std::time::Instant::now();
            let sizes = db.column_family_sizes();
            (sizes, started.elapsed())
        })
        .await;

        match result {
            Ok((Ok(sizes), elapsed)) => {
                tracing::debug!(
                    elapsed_us = elapsed.as_micros(),
                    column_families = sizes.len(),
                    "measured column family sizes"
                );
                let _ = sender.send(sizes);
            }
            Ok((Err(err), _)) => tracing::warn!(reason =? err, "failed to read column family sizes"),
            Err(err) => tracing::error!(reason =? err, "column family size measurement task panicked")
        }

        tokio::time::sleep(interval).await;
    }
}

pub fn build_metrics_registry() -> Registry {
    let mut top_registry = Registry::default();
    let registry = top_registry.sub_registry_with_prefix("hotblocks");

    registry.register(
        "query_error_too_many_tasks",
        "Number of query tasks rejected due to task queue overflow",
        QUERY_ERROR_TOO_MANY_TASKS.clone()
    );

    registry.register(
        "query_error_too_many_data_waiters",
        "Number of queries rejected, because data is not yet available and there are too many data waiters",
        QUERY_ERROR_TOO_MANY_DATA_WAITERS.clone()
    );

    registry.register("http_status", "Number of sent HTTP responses", HTTP_STATUS.clone());
    registry.register(
        "http_seconds_to_first_byte",
        "Time to first byte of HTTP responses",
        HTTP_TTFB.clone()
    );

    registry.register("stream_bytes", "Number of bytes per stream", STREAM_BYTES.clone());
    registry.register("stream_blocks", "Number of blocks per stream", STREAM_BLOCKS.clone());
    registry.register("stream_chunks", "Number of chunks per stream", STREAM_CHUNKS.clone());
    registry.register(
        "stream_bytes_per_second",
        "Completed streams bandwidth",
        STREAM_BYTES_PER_SECOND.clone()
    );
    registry.register(
        "stream_blocks_per_second",
        "Completed streams speed in blocks",
        STREAM_BLOCKS_PER_SECOND.clone()
    );
    registry.register(
        "stream_duration_seconds",
        "Durations of completed streams",
        STREAM_DURATIONS.clone()
    );
    registry.register(
        "queried_blocks",
        "Number of blocks per running query",
        QUERIED_BLOCKS.clone()
    );
    registry.register(
        "queried_chunks",
        "Number of chunks per running query",
        QUERIED_CHUNKS.clone()
    );
    registry.register(
        "completed_queries",
        "Number of completed queries",
        COMPLETED_QUERIES.clone()
    );

    top_registry
}

impl Collector for QueryExecutorCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let active_queries = self.get_active_queries();

        encoder
            .encode_descriptor(
                "hotblocks_active_queries",
                "Number of currently active queries",
                None,
                MetricType::Gauge
            )?
            .encode_gauge(&active_queries)?;
        Ok(())
    }
}
