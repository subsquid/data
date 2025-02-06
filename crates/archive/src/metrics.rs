use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;


pub static PROGRESS: LazyLock<Gauge<f64, AtomicU64>> = LazyLock::new(Gauge::default);
pub static LAST_BLOCK: LazyLock<Counter> = LazyLock::new(Counter::default);
pub static LAST_SAVED_BLOCK: LazyLock<Counter> = LazyLock::new(Counter::default);


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
