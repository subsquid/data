use crate::query::QueryExecutorCollector;
use crate::types::DBRef;
use anyhow::bail;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{
    DescriptorEncoder, EncodeLabelSet, EncodeLabelValue, LabelValueEncoder,
};
use prometheus_client::metrics::{
    MetricType,
    counter::Counter,
    family::Family,
    histogram::{Histogram, exponential_buckets},
};
use prometheus_client::registry::Registry;
use sqd_storage::db::{DatasetId, ReadSnapshot};
use std::fmt::Write;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::error;

#[derive(Copy, Clone, Hash, Debug, Default, Ord, PartialOrd, Eq, PartialEq, EncodeLabelSet)]
struct DatasetLabel {
    dataset: DatasetValue,
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
            dataset: DatasetValue($dataset_id),
        }
    };
}

type Labels = Vec<(String, String)>;

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

pub static STREAM_DURATIONS: LazyLock<Family<Labels, Histogram>> = LazyLock::new(|| {
    Family::new_with_constructor(|| Histogram::new(exponential_buckets(0.01, 2.0, 20)))
});
pub static STREAM_BYTES: LazyLock<Family<Labels, Histogram>> = LazyLock::new(|| {
    Family::new_with_constructor(|| Histogram::new(exponential_buckets(1000., 2.0, 20)))
});
pub static STREAM_BLOCKS: LazyLock<Family<Labels, Histogram>> = LazyLock::new(|| {
    Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30)))
});
pub static STREAM_CHUNKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(1., 20))));
pub static STREAM_BYTES_PER_SECOND: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(exponential_buckets(100., 3.0, 20)));
pub static STREAM_BLOCKS_PER_SECOND: LazyLock<Family<Labels, Histogram>> = LazyLock::new(|| {
    Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 3.0, 20)))
});

pub static QUERIED_BLOCKS: LazyLock<Family<Labels, Histogram>> = LazyLock::new(|| {
    Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30)))
});
pub static QUERIED_CHUNKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(1., 20))));

pub fn report_query_too_many_tasks_error() {
    QUERY_ERROR_TOO_MANY_TASKS.inc();
}

pub fn report_query_too_many_data_waiters_error() {
    QUERY_ERROR_TOO_MANY_DATA_WAITERS.inc();
}

pub fn report_http_response(labels: &Vec<(String, String)>, to_first_byte: Duration) {
    HTTP_STATUS.get_or_create(&labels).inc();
    HTTP_TTFB
        .get_or_create(&labels)
        .observe(to_first_byte.as_secs_f64());
}

#[derive(Debug)]
pub struct DatasetMetricsCollector {
    pub db: DBRef,
    pub datasets: Vec<DatasetId>,
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
    dataset_id: DatasetId,
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
        .encode_descriptor(
            "hotblocks_first_block",
            "First block",
            None,
            MetricType::Gauge,
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&first_chunk.first_block())?;

    encoder
        .encode_descriptor(
            "hotblocks_last_block",
            "Last block",
            None,
            MetricType::Gauge,
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&last_chunk.last_block())?;

    encoder
        .encode_descriptor(
            "hotblocks_last_block_timestamp_ms",
            "Timestamp of the last block",
            None,
            MetricType::Gauge,
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&last_chunk.last_block_time().unwrap_or(0))?;

    encoder
        .encode_descriptor(
            "hotblocks_last_finalized_block",
            "Last finalized block",
            None,
            MetricType::Gauge,
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&label.finalized_head().map_or(0, |h| h.number))?;

    Ok(())
}

pub fn build_metrics_registry() -> Registry {
    let mut top_registry = Registry::default();
    let registry = top_registry.sub_registry_with_prefix("hotblocks");

    registry.register(
        "query_error_too_many_tasks",
        "Number of query tasks rejected due to task queue overflow",
        QUERY_ERROR_TOO_MANY_TASKS.clone(),
    );

    registry.register(
        "query_error_too_many_data_waiters",
        "Number of queries rejected, because data is not yet available and there are too many data waiters",
        QUERY_ERROR_TOO_MANY_DATA_WAITERS.clone()
    );

    registry.register(
        "http_status",
        "Number of sent HTTP responses",
        HTTP_STATUS.clone(),
    );
    registry.register(
        "http_seconds_to_first_byte",
        "Time to first byte of HTTP responses",
        HTTP_TTFB.clone(),
    );

    registry.register(
        "stream_bytes",
        "Number of bytes per stream",
        STREAM_BYTES.clone(),
    );
    registry.register(
        "stream_blocks",
        "Number of blocks per stream",
        STREAM_BLOCKS.clone(),
    );
    registry.register(
        "stream_chunks",
        "Number of chunks per stream",
        STREAM_CHUNKS.clone(),
    );
    registry.register(
        "stream_bytes_per_second",
        "Completed streams bandwidth",
        STREAM_BYTES_PER_SECOND.clone(),
    );
    registry.register(
        "stream_blocks_per_second",
        "Completed streams speed in blocks",
        STREAM_BLOCKS_PER_SECOND.clone(),
    );
    registry.register(
        "stream_duration_seconds",
        "Durations of completed streams",
        STREAM_DURATIONS.clone(),
    );
    registry.register(
        "queried_blocks",
        "Number of blocks per running query",
        QUERIED_BLOCKS.clone(),
    );
    registry.register(
        "queried_chunks",
        "Number of chunks per running query",
        QUERIED_CHUNKS.clone(),
    );
    registry.register(
        "completed_queries",
        "Number of completed queries",
        COMPLETED_QUERIES.clone(),
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
                MetricType::Gauge,
            )?
            .encode_gauge(&active_queries)?;
        Ok(())
    }
}
