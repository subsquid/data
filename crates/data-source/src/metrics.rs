use std::{sync::LazyLock, time::Duration};

use prometheus_client::metrics::{
    counter::Counter,
    family::Family,
    histogram::{exponential_buckets, Histogram}
};

type Labels = Vec<(&'static str, String)>;

/// Upstream data-source ingestion errors by `source` endpoint and `kind`.
/// Registered in the hotblocks metrics registry.
pub static INGEST_SOURCE_ERRORS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);

/// Fork signals by `source` and whether it held the contested position (`at_tip`/`above_tip`).
pub static INGEST_FORK_SIGNALS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);

/// Time from the first fork signal until a fork decision, by decision path.
pub static INGEST_FORK_CONSENSUS_DURATION: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(0.001, 2.0, 15))));

/// Public because the pre-ingest head probe lives in `hotblocks` and must feed the same counter:
/// it runs before this crate's stream loop, so a total outage never reaches `on_error`.
pub fn record_ingest_source_error(source: &str, kind: &'static str) {
    INGEST_SOURCE_ERRORS
        .get_or_create(&vec![("source", source.to_string()), ("kind", kind.to_string())])
        .inc();
}

pub(crate) fn record_ingest_fork_signal(source: &str, standing: &'static str) {
    INGEST_FORK_SIGNALS
        .get_or_create(&vec![
            ("source", source.to_string()),
            ("standing", standing.to_string()),
        ])
        .inc();
}

pub(crate) fn record_ingest_fork_consensus_duration(decision: &'static str, duration: Duration) {
    INGEST_FORK_CONSENSUS_DURATION
        .get_or_create(&vec![("decision", decision.to_string())])
        .observe(duration.as_secs_f64());
}
