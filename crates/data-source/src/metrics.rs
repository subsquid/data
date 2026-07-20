use std::sync::LazyLock;

use prometheus_client::metrics::{counter::Counter, family::Family};

type Labels = Vec<(&'static str, String)>;

/// Upstream data-source ingestion errors by `source` endpoint and `kind`.
/// Registered in the hotblocks metrics registry.
pub static INGEST_SOURCE_ERRORS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);

/// Fork signals by `source` and whether it held the contested position (`at_tip`/`above_tip`).
pub static INGEST_FORK_SIGNALS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);

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
