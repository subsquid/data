use crate::types::DBRef;
use anyhow::bail;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::MetricType;
use prometheus_client::registry::Registry;
use sqd_storage::db::{DatasetId, ReadSnapshot};
use std::fmt::Write;
use std::sync::LazyLock;
use tracing::error;


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


macro_rules! metric {
    ($name:ident, $t:ty) => {
        static $name: LazyLock<$t> = LazyLock::new(Default::default);
    };
}


metric!(QUERY_ERROR_TOO_MANY_TASKS, Counter);
metric!(QUERY_ERROR_TOO_MANY_DATA_WAITERS, Counter);


pub fn report_query_too_many_tasks_error() {
    QUERY_ERROR_TOO_MANY_TASKS.inc();
}


pub fn report_query_too_many_data_waiters_error() {
    QUERY_ERROR_TOO_MANY_DATA_WAITERS.inc();
}


#[derive(Debug)]
struct DatasetMetricsCollector {
    db: DBRef,
    datasets: Vec<DatasetId>
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
                }
            }
        }

        Ok(())
    }
}


fn collect_dataset_metrics(
    encoder: &mut DescriptorEncoder,
    db: &ReadSnapshot,
    dataset_id: DatasetId
) -> anyhow::Result<()>
{
    let Some(label) = db.get_label(dataset_id)? else {
        return Ok(())
    };

    let Some(first_chunk) = db.get_first_chunk(dataset_id)? else {
        return Ok(())
    };

    let Some(last_chunk) = db.get_last_chunk(dataset_id)? else {
        bail!("first chunk exists, while last does not")
    };

    encoder.encode_descriptor(
        "hotblocks_first_block",
        "First block",
        None,
        MetricType::Gauge
    )?.encode_family(
        &dataset_label!(dataset_id)
    )?.encode_gauge(
        &first_chunk.first_block()
    )?;

    encoder.encode_descriptor(
        "hotblocks_last_block",
        "Last block",
        None,
        MetricType::Gauge
    )?.encode_family(
        &dataset_label!(dataset_id)
    )?.encode_gauge(
        &last_chunk.last_block()
    )?;

    encoder.encode_descriptor(
        "hotblocks_last_block_timestamp_ms",
        "Timestamp of the last block",
        None,
        MetricType::Gauge
    )?.encode_family(
        &dataset_label!(dataset_id)
    )?.encode_gauge(
        &last_chunk.last_block_time().unwrap_or(0)
    )?;

    encoder.encode_descriptor(
        "hotblocks_last_finalized_block",
        "Last finalized block",
        None,
        MetricType::Gauge
    )?.encode_family(
        &dataset_label!(dataset_id)
    )?.encode_gauge(
        &label.finalized_head().map_or(0, |h| h.number)
    )?;

    Ok(())
}


pub fn build_metrics_registry(db: DBRef, datasets: Vec<DatasetId>) -> Registry {
    let mut registry = Registry::default();

    registry.register(
        "hotblocks_query_error_too_many_tasks",
        "Number of query tasks rejected due to task queue overflow",
        QUERY_ERROR_TOO_MANY_TASKS.clone()
    );

    registry.register(
        "hotblocks_query_error_too_many_data_waiters",
        "Number of queries rejected, because data is not yet available and there are too many data waiters",
        QUERY_ERROR_TOO_MANY_DATA_WAITERS.clone()
    );

    registry.register_collector(Box::new(DatasetMetricsCollector {
        db,
        datasets
    }));

    registry
}