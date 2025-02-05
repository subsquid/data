use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;


lazy_static::lazy_static! {
    pub static ref PROGRESS: Gauge<f64, AtomicU64> = Gauge::default();
    pub static ref LAST_BLOCK: Counter = Counter::default();
    pub static ref LAST_SAVED_BLOCK: Counter = Counter::default();
}


pub fn register_metrics(registry: &mut Registry) {
    registry.register(
        "sqd_progress_blocks_per_second",
        "Overall block processing speed",
        PROGRESS.clone()
    );
    registry.register(
        "sqd_last_block",
        "Last ingested block",
        LAST_BLOCK.clone()
    );
    registry.register(
        "sqd_last_saved_block",
        "Last saved block",
        LAST_SAVED_BLOCK.clone()
    );
}
