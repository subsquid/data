use crate::sink::Sink;
use crate::writer::Writer;
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::registry::Registry;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;
use std::fmt::{Formatter, Debug};
use std::time::{Duration, UNIX_EPOCH};


pub static PROGRESS: LazyLock<Gauge<f64, AtomicU64>> = LazyLock::new(Gauge::default);
pub static PROCESSING_TIME: LazyLock<Gauge<f64, AtomicU64>> = LazyLock::new(Gauge::default);
pub static LAST_BLOCK: LazyLock<Gauge<u64, AtomicU64>> = LazyLock::new(Gauge::default);
pub static LAST_BLOCK_TIMESTAMP: LazyLock<Gauge<u64, AtomicU64>> = LazyLock::new(Gauge::default);
pub static LAST_SAVED_BLOCK: LazyLock<Gauge<u64, AtomicU64>> = LazyLock::new(Gauge::default);


pub fn register_metrics(registry: &mut Registry) {
    registry.register(
        "sqd_progress_blocks_per_second",
        "Overall block processing speed",
        PROGRESS.clone()
    );
    registry.register(
        "sqd_blocks_processing_time",
        "Time difference between now and block timestamp (in seconds)",
        PROCESSING_TIME.clone()
    );
    registry.register(
        "sqd_latest_processed_block_number",
        "Latest processed block number",
        LAST_BLOCK.clone()
    );
    registry.register(
        "sqd_latest_processed_block_timestamp",
        "Latest processed block timestamp",
        LAST_BLOCK_TIMESTAMP.clone()
    );
    registry.register(
        "sqd_latest_saved_block_number",
        "Latest saved block number",
        LAST_SAVED_BLOCK.clone()
    );
}
