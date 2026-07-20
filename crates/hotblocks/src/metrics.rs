use std::{fmt::Write, sync::LazyLock, time::Duration};

use anyhow::bail;
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
use sqd_storage::db::{
    CF_BLOCK_HASHES, CF_CHUNKS, CF_DATASETS, CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TABLES, CF_TRANSACTION_HASHES,
    DatasetId, HashIndexWriteMetrics, ReadSnapshot
};
use tracing::error;

use crate::{errors::UnapplicableFork, query::QueryExecutorCollector, types::DBRef};

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
pub static QUERY_ERROR_WORKER_PANIC: LazyLock<Counter> = LazyLock::new(Default::default);

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

#[derive(Copy, Clone, Hash, Debug, Eq, PartialEq)]
pub(crate) enum WriteStage {
    Prepare,
    Tables,
    Commit,
    Retention,
    BlockHashIndex,
    TransactionHashIndex
}

impl WriteStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Tables => "tables",
            Self::Commit => "commit",
            Self::Retention => "retention",
            Self::BlockHashIndex => "block_hash_index",
            Self::TransactionHashIndex => "transaction_hash_index"
        }
    }
}

impl EncodeLabelValue for WriteStage {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        encoder.write_str(self.as_str())
    }
}

#[derive(Copy, Clone, Hash, Debug, Eq, PartialEq)]
enum WriteOutcome {
    Success,
    Error
}

impl EncodeLabelValue for WriteOutcome {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        encoder.write_str(match self {
            Self::Success => "success",
            Self::Error => "error"
        })
    }
}

#[derive(Copy, Clone, Hash, Debug, Eq, PartialEq, EncodeLabelSet)]
struct WriteLabels {
    dataset: DatasetValue,
    stage: WriteStage,
    outcome: WriteOutcome
}

#[derive(Copy, Clone, Hash, Debug, Eq, PartialEq, EncodeLabelSet)]
struct EpochFailureLabels {
    dataset: DatasetValue,
    reason: &'static str
}

static DATASET_EPOCH_FAILURES: LazyLock<Family<EpochFailureLabels, Counter>> = LazyLock::new(Default::default);

pub(crate) fn report_dataset_epoch_failure(dataset_id: DatasetId, err: &anyhow::Error) {
    // Never label by message — it carries block numbers and hashes.
    let reason = if err.chain().any(|e| e.is::<UnapplicableFork>()) {
        "unapplicable_fork"
    } else {
        "other"
    };
    DATASET_EPOCH_FAILURES
        .get_or_create(&EpochFailureLabels {
            dataset: DatasetValue(dataset_id),
            reason
        })
        .inc();
}

static WRITE_DURATION: LazyLock<Family<WriteLabels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(0.0001, 28))));

pub(crate) fn report_write_duration(dataset_id: DatasetId, stage: WriteStage, duration: Duration, success: bool) {
    let labels = WriteLabels {
        dataset: DatasetValue(dataset_id),
        stage,
        outcome: if success {
            WriteOutcome::Success
        } else {
            WriteOutcome::Error
        }
    };
    WRITE_DURATION.get_or_create(&labels).observe(duration.as_secs_f64());
}

pub(crate) fn report_hash_index_write_metrics(dataset_id: DatasetId, metrics: &HashIndexWriteMetrics, success: bool) {
    if let Some(duration) = metrics.block_hash_index_duration() {
        report_write_duration(dataset_id, WriteStage::BlockHashIndex, duration, success);
    }
    if let Some(duration) = metrics.transaction_hash_index_duration() {
        report_write_duration(dataset_id, WriteStage::TransactionHashIndex, duration, success);
    }
}

pub fn report_query_too_many_tasks_error() {
    QUERY_ERROR_TOO_MANY_TASKS.inc();
}

pub fn report_query_too_many_data_waiters_error() {
    QUERY_ERROR_TOO_MANY_DATA_WAITERS.inc();
}

pub fn report_query_worker_panic() {
    QUERY_ERROR_WORKER_PANIC.inc();
}

pub fn report_http_response(labels: &Vec<(&'static str, String)>, to_first_byte: Duration) {
    HTTP_STATUS.get_or_create(&labels).inc();
    HTTP_TTFB.get_or_create(&labels).observe(to_first_byte.as_secs_f64());
}

#[derive(Debug)]
pub struct DatasetMetricsCollector {
    pub db: DBRef,
    pub datasets: Vec<DatasetId>
}

impl Collector for DatasetMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let db = self.db.snapshot();

        for dataset_id in self.datasets.iter().copied() {
            if let Err(err) = collect_dataset_metrics(&mut encoder, &db, dataset_id) {
                return if err.is::<std::fmt::Error>() {
                    Err(err.downcast().unwrap())
                } else {
                    // subsequent metric collection most likely will fail as well,
                    // hence let's terminate metric collection entirely
                    error!(
                        err =? err,
                        "failed to collect metrics for dataset {}",
                        dataset_id
                    );
                    Ok(())
                };
            }
        }

        Ok(())
    }
}

fn collect_dataset_metrics(
    encoder: &mut DescriptorEncoder,
    db: &ReadSnapshot,
    dataset_id: DatasetId
) -> anyhow::Result<()> {
    let Some(label) = db.get_label(dataset_id)? else {
        return Ok(());
    };

    let Some(first_chunk) = db.get_first_chunk(dataset_id)? else {
        return Ok(());
    };

    let Some(last_chunk) = db.get_last_chunk(dataset_id)? else {
        bail!("first chunk exists, while last does not")
    };

    encoder
        .encode_descriptor("hotblocks_first_block", "First block", None, MetricType::Gauge)?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&first_chunk.first_block())?;

    encoder
        .encode_descriptor("hotblocks_last_block", "Last block", None, MetricType::Gauge)?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&last_chunk.last_block())?;

    encoder
        .encode_descriptor(
            "hotblocks_last_block_timestamp_ms",
            "Timestamp of the last block",
            None,
            MetricType::Gauge
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&last_chunk.last_block_time().unwrap_or(0))?;

    encoder
        .encode_descriptor(
            "hotblocks_last_finalized_block",
            "Last finalized block",
            None,
            MetricType::Gauge
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&label.finalized_head().map_or(0, |h| h.number))?;

    Ok(())
}

/// RocksDB write-pressure and maintenance gauges collected from intrinsic properties.
///
/// Property reads happen only during a Prometheus scrape and do not require RocksDB's
/// optional statistics collection. Labels are bounded by the configured column
/// families.
#[derive(Debug)]
pub struct RocksDbCollector {
    pub db: DBRef
}

#[derive(Copy, Clone)]
enum PropertyMetricType {
    Gauge,
    Counter
}

impl PropertyMetricType {
    const fn metric_type(self) -> MetricType {
        match self {
            Self::Gauge => MetricType::Gauge,
            Self::Counter => MetricType::Counter
        }
    }
}

struct RocksDbPropertyMetric {
    metric: &'static str,
    help: &'static str,
    property: &'static str,
    metric_type: PropertyMetricType
}

// DB-wide properties are exposed through any existing column-family handle.
const ROCKSDB_DB_WIDE: &[RocksDbPropertyMetric] = &[
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_write_stopped",
        help: "1 while RocksDB has hard-stopped all writes",
        property: "rocksdb.is-write-stopped",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_actual_delayed_write_rate_bytes_per_second",
        help: "Current RocksDB soft-stall write rate in bytes per second; 0 when writes are not delayed",
        property: "rocksdb.actual-delayed-write-rate",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_running_compactions",
        help: "Number of RocksDB compactions currently running",
        property: "rocksdb.num-running-compactions",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_running_flushes",
        help: "Number of RocksDB memtable flushes currently running",
        property: "rocksdb.num-running-flushes",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_snapshots",
        help: "Number of unreleased RocksDB snapshots",
        property: "rocksdb.num-snapshots",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_oldest_snapshot_time_seconds",
        help: "Unix timestamp of the oldest unreleased RocksDB snapshot; 0 when none exist",
        property: "rocksdb.oldest-snapshot-time",
        metric_type: PropertyMetricType::Gauge
    }
];

const ROCKSDB_PER_CF: &[RocksDbPropertyMetric] = &[
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_files_at_level0",
        help: "Number of RocksDB SST files at level 0",
        property: "rocksdb.num-files-at-level0",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_pending_compaction_bytes",
        help: "Estimated bytes RocksDB must compact to bring levels under their targets",
        property: "rocksdb.estimate-pending-compaction-bytes",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_compaction_pending",
        help: "1 when RocksDB has determined that a column family needs compaction",
        property: "rocksdb.compaction-pending",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_immutable_memtables",
        help: "Number of immutable RocksDB memtables waiting to be flushed",
        property: "rocksdb.num-immutable-mem-table",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_memtable_flush_pending",
        help: "1 while a RocksDB memtable flush is pending",
        property: "rocksdb.mem-table-flush-pending",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_memtable_bytes",
        help: "Approximate bytes in active and unflushed immutable RocksDB memtables",
        property: "rocksdb.cur-size-all-mem-tables",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_estimated_keys",
        help: "Estimated live keys in a RocksDB column family, including memtables and SST files",
        property: "rocksdb.estimate-num-keys",
        metric_type: PropertyMetricType::Gauge
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_background_errors",
        help: "Accumulated RocksDB background flush and compaction errors",
        property: "rocksdb.background-errors",
        metric_type: PropertyMetricType::Counter
    },
    RocksDbPropertyMetric {
        metric: "hotblocks_rocksdb_live_sst_files_bytes",
        help: "Bytes in RocksDB SST files belonging to the current LSM version",
        property: "rocksdb.live-sst-files-size",
        metric_type: PropertyMetricType::Gauge
    }
];

const ROCKSDB_COLUMN_FAMILIES: &[&str] = &[
    CF_DATASETS,
    CF_CHUNKS,
    CF_TABLES,
    CF_DIRTY_TABLES,
    CF_DELETED_TABLES,
    CF_BLOCK_HASHES,
    CF_TRANSACTION_HASHES
];

#[derive(Copy, Clone, Hash, Debug, Eq, PartialEq, EncodeLabelSet)]
struct ColumnFamilyLabel {
    cf: &'static str
}

impl Collector for RocksDbCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        match collect_rocksdb_metrics(&mut encoder, &self.db) {
            Ok(()) => Ok(()),
            Err(err) => match err.downcast::<std::fmt::Error>() {
                Ok(err) => Err(err),
                Err(err) => {
                    error!(error =? err, "failed to collect RocksDB metrics");
                    Ok(())
                }
            }
        }
    }
}

fn collect_rocksdb_metrics(encoder: &mut DescriptorEncoder, db: &DBRef) -> anyhow::Result<()> {
    for spec in ROCKSDB_DB_WIDE {
        let Some(value) = db.get_int_property(CF_TABLES, spec.property)? else {
            continue;
        };

        encoder
            .encode_descriptor(spec.metric, spec.help, None, spec.metric_type.metric_type())?
            .encode_gauge(&value)?;
    }

    for spec in ROCKSDB_PER_CF {
        let mut metric = encoder.encode_descriptor(spec.metric, spec.help, None, spec.metric_type.metric_type())?;

        for &cf in ROCKSDB_COLUMN_FAMILIES {
            let Some(value) = db.get_int_property(cf, spec.property)? else {
                continue;
            };

            let label = ColumnFamilyLabel { cf };
            let mut sample = metric.encode_family(&label)?;
            match spec.metric_type {
                PropertyMetricType::Gauge => sample.encode_gauge(&value)?,
                PropertyMetricType::Counter => sample.encode_counter::<ColumnFamilyLabel, u64, u64>(&value, None)?
            }
        }
    }

    Ok(())
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

    registry.register(
        "query_error_worker_panic",
        "Number of query worker panics that aborted an in-flight response stream",
        QUERY_ERROR_WORKER_PANIC.clone()
    );

    registry.register(
        "ingest_source_errors",
        "Upstream data source ingestion errors, by source endpoint (host:port/path) and kind \
         (connect/timeout/http/decode/io/request/other). Covers both the pre-ingest head probe \
         and the stream loop",
        sqd_data_source::metrics::INGEST_SOURCE_ERRORS.clone()
    );

    registry.register(
        "ingest_fork_signals",
        "Fork signals from upstream data sources, by source endpoint and whether the source \
         held the contested position (standing=at_tip) or contested one above its own tip \
         (standing=above_tip). GAP-21: an honest source can also land in above_tip when its \
         hint window misses the parent across a hole wider than 100 positions",
        sqd_data_source::metrics::INGEST_FORK_SIGNALS.clone()
    );

    registry.register(
        "dataset_epoch_failures",
        "Dataset update task failures, by dataset and cause; each one parks ingestion for \
         60s before a full restart. reason=unapplicable_fork is a divergence reaching below \
         finalized data",
        DATASET_EPOCH_FAILURES.clone()
    );

    registry.register("http_status", "Number of sent HTTP responses", HTTP_STATUS.clone());
    registry.register(
        "http_seconds_to_first_byte",
        "Time to first byte of HTTP responses",
        HTTP_TTFB.clone()
    );
    registry.register(
        "write_duration_seconds",
        "Write-pipeline stage duration; hash-index stages are nested within commit or retention",
        WRITE_DURATION.clone()
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering}
        },
        thread,
        time::{Duration, Instant}
    };

    use sqd_storage::db::{Chunk, DatabaseSettings, DatasetId, DatasetKind};

    use super::*;

    #[test]
    fn write_duration_exposes_bounded_stage_and_outcome_labels() {
        let dataset_id = DatasetId::from_str("write-metrics-test");
        let stages = [
            WriteStage::Prepare,
            WriteStage::Tables,
            WriteStage::Commit,
            WriteStage::Retention,
            WriteStage::BlockHashIndex,
            WriteStage::TransactionHashIndex
        ];

        for stage in stages {
            report_write_duration(dataset_id, stage, Duration::from_millis(1), true);
        }
        report_write_duration(dataset_id, WriteStage::Commit, Duration::from_millis(2), false);

        let registry = build_metrics_registry();
        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();

        for stage in stages {
            assert!(
                output.lines().any(|line| {
                    line.starts_with("hotblocks_write_duration_seconds_count")
                        && line.contains("dataset=\"write-metrics-test\"")
                        && line.contains(&format!("stage=\"{}\"", stage.as_str()))
                        && line.contains("outcome=\"success\"")
                }),
                "missing successful {} stage:\n{output}",
                stage.as_str()
            );
        }
        assert!(
            output.lines().any(|line| {
                line.starts_with("hotblocks_write_duration_seconds_count")
                    && line.contains("dataset=\"write-metrics-test\"")
                    && line.contains("stage=\"commit\"")
                    && line.contains("outcome=\"error\"")
            }),
            "missing error outcome:\n{output}"
        );
    }

    #[test]
    fn rocksdb_collector_exposes_global_and_per_cf_properties() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(DatabaseSettings::default().open(dir.path()).unwrap());
        let mut registry = Registry::default();
        registry.register_collector(Box::new(RocksDbCollector { db }));

        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();

        for spec in ROCKSDB_DB_WIDE {
            assert_metric_exposed(&output, spec.metric);
        }

        for spec in ROCKSDB_PER_CF {
            assert_metric_exposed(&output, spec.metric);
            for cf in ROCKSDB_COLUMN_FAMILIES {
                let metric = match spec.metric_type {
                    PropertyMetricType::Gauge => spec.metric.to_owned(),
                    PropertyMetricType::Counter => format!("{}_total", spec.metric)
                };
                assert!(
                    output
                        .lines()
                        .any(|line| { line.starts_with(&metric) && line.contains(&format!("cf=\"{cf}\"")) }),
                    "missing {metric} for column family {cf}:\n{output}"
                );
            }
        }
    }

    fn assert_metric_exposed(output: &str, metric: &str) {
        assert!(
            output
                .lines()
                .any(|line| line.starts_with(metric) && !line.starts_with('#')),
            "missing metric {metric}:\n{output}"
        );
    }

    /// Reproducible manual check that continuous Prometheus collection does not create
    /// meaningful contention on RocksDB reads or writes. The scraper intentionally runs
    /// without the production scrape interval, making this a much harsher workload.
    #[test]
    #[ignore = "manual RocksDB metrics overhead benchmark"]
    fn benchmark_rocksdb_metrics_overhead() {
        const PREPARE_WRITES: u64 = 500;
        const MEASURED_WRITES: u64 = 2_000;
        const MEASURED_READS: usize = 20_000;
        const MEASURED_SCRAPES: usize = 1_000;

        let baseline_dir = tempfile::tempdir().unwrap();
        let baseline_db = Arc::new(DatabaseSettings::default().open(baseline_dir.path()).unwrap());
        let stressed_dir = tempfile::tempdir().unwrap();
        let stressed_db = Arc::new(DatabaseSettings::default().open(stressed_dir.path()).unwrap());
        let dataset_id = DatasetId::from_str("metrics-benchmark");
        let dataset_kind = DatasetKind::from_str("benchmark");

        for db in [&baseline_db, &stressed_db] {
            db.create_dataset(dataset_id, dataset_kind).unwrap();
            write_chunks(db, dataset_id, 0, PREPARE_WRITES);
        }

        let scrape_latencies = measure_scrapes(Arc::clone(&baseline_db), MEASURED_SCRAPES);
        let baseline_writes = measure_writes(
            &baseline_db,
            dataset_id,
            PREPARE_WRITES,
            PREPARE_WRITES + MEASURED_WRITES
        );

        let stop = Arc::new(AtomicBool::new(false));
        let scraper = spawn_continuous_scraper(Arc::clone(&stressed_db), Arc::clone(&stop));
        let stressed_writes = measure_writes(
            &stressed_db,
            dataset_id,
            PREPARE_WRITES,
            PREPARE_WRITES + MEASURED_WRITES
        );
        stop.store(true, Ordering::Relaxed);
        let write_scrapes = scraper.join().unwrap();

        let baseline_reads = measure_reads(&baseline_db, dataset_id, MEASURED_READS);
        stop.store(false, Ordering::Relaxed);
        let scraper = spawn_continuous_scraper(Arc::clone(&stressed_db), Arc::clone(&stop));
        let stressed_reads = measure_reads(&stressed_db, dataset_id, MEASURED_READS);
        stop.store(true, Ordering::Relaxed);
        let read_scrapes = scraper.join().unwrap();

        eprintln!("full scrape:        {}", summarize(scrape_latencies));
        eprintln!("writes baseline:    {}", summarize(baseline_writes));
        eprintln!(
            "writes + scraping:  {} ({write_scrapes} concurrent scrapes)",
            summarize(stressed_writes)
        );
        eprintln!("reads baseline:     {}", summarize(baseline_reads));
        eprintln!(
            "reads + scraping:   {} ({read_scrapes} concurrent scrapes)",
            summarize(stressed_reads)
        );
    }

    fn write_chunks(db: &DBRef, dataset_id: DatasetId, from: u64, to: u64) {
        for block in from..to {
            db.insert_chunk(dataset_id, &chunk(block)).unwrap();
        }
    }

    fn measure_writes(db: &DBRef, dataset_id: DatasetId, from: u64, to: u64) -> Vec<Duration> {
        (from..to)
            .map(|block| {
                let started = Instant::now();
                db.insert_chunk(dataset_id, &chunk(block)).unwrap();
                started.elapsed()
            })
            .collect()
    }

    fn measure_reads(db: &DBRef, dataset_id: DatasetId, count: usize) -> Vec<Duration> {
        (0..count)
            .map(|_| {
                let started = Instant::now();
                let snapshot = db.snapshot();
                assert!(snapshot.get_last_chunk(dataset_id).unwrap().is_some());
                started.elapsed()
            })
            .collect()
    }

    fn measure_scrapes(db: DBRef, count: usize) -> Vec<Duration> {
        let mut registry = Registry::default();
        registry.register_collector(Box::new(RocksDbCollector { db }));
        let mut output = String::with_capacity(16 * 1024);

        (0..count)
            .map(|_| {
                output.clear();
                let started = Instant::now();
                prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();
                started.elapsed()
            })
            .collect()
    }

    fn spawn_continuous_scraper(db: DBRef, stop: Arc<AtomicBool>) -> thread::JoinHandle<u64> {
        thread::spawn(move || {
            let mut registry = Registry::default();
            registry.register_collector(Box::new(RocksDbCollector { db }));
            let mut output = String::with_capacity(16 * 1024);
            let mut scrapes = 0;

            while !stop.load(Ordering::Relaxed) {
                output.clear();
                prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();
                scrapes += 1;
            }

            scrapes
        })
    }

    fn chunk(block: u64) -> Chunk {
        Chunk::V0 {
            first_block: block,
            last_block: block,
            last_block_hash: format!("hash-{block}"),
            parent_block_hash: if block == 0 {
                "genesis".to_owned()
            } else {
                format!("hash-{}", block - 1)
            },
            tables: Default::default()
        }
    }

    fn summarize(mut samples: Vec<Duration>) -> String {
        samples.sort_unstable();
        let total: Duration = samples.iter().sum();
        format!(
            "n={}, total={:.3}s, p50={:?}, p95={:?}, p99={:?}, max={:?}",
            samples.len(),
            total.as_secs_f64(),
            percentile(&samples, 50),
            percentile(&samples, 95),
            percentile(&samples, 99),
            samples.last().copied().unwrap_or_default()
        )
    }

    fn percentile(samples: &[Duration], percentile: usize) -> Duration {
        let index = samples.len().saturating_sub(1) * percentile / 100;
        samples[index]
    }
}
