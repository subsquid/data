mod api;
mod cli;
mod data_service;
mod dataset_config;
mod dataset_controller;
mod encoding;
mod errors;
mod metrics;
mod query;
mod types;

use std::time::Duration;

use api::build_api;
use clap::Parser;
use cli::CLI;
use tracing::{debug, error, instrument, warn};
use types::DBRef;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    let args = CLI::parse();

    if let Some(n_threads) = args.query_threads {
        unsafe {
            sqd_polars::set_polars_thread_pool_size(n_threads);
        }
    }

    init_tracing();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let app = args.build_app().await?;

            // NB: startup orphan purge, plus optional file unlink gated by
            // --startup-disk-reclaim, already ran inside `build_app` ->
            // `DataService::start` before any controller spawned.

            let runtime_reclaim_enabled = args.disk_reclaim_interval_secs > 0;
            tokio::spawn(db_cleanup_task(app.db.clone()));
            if runtime_reclaim_enabled {
                tokio::spawn(db_reclaim_task(
                    app.db.clone(),
                    Duration::from_secs(args.disk_reclaim_interval_secs)
                ));
            }

            let api = build_api(app);

            let listener = tokio::net::TcpListener::bind(("0.0.0.0", args.port)).await?;

            axum::serve(listener, api)
                .with_graceful_shutdown(shutdown_signal())
                .await?;

            Ok::<_, anyhow::Error>(())
        })
}

fn init_tracing() {
    use std::io::IsTerminal;

    let env_filter = tracing_subscriber::EnvFilter::builder()
        .parse_lossy(std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).unwrap_or("info".to_string()));

    if std::io::stdout().is_terminal() {
        tracing_subscriber::fmt().with_env_filter(env_filter).compact().init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_current_span(false)
            .init();
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
/// Longer pause after a failed tick, so a persistent error (e.g. full disk) doesn't
/// busy-loop failing writes.
const CLEANUP_ERROR_BACKOFF: Duration = Duration::from_secs(30);

/// Routine Phase-1 cleanup: point-delete deleted tables' data. The database captures at
/// open time whether sequence metadata is needed by snapshot-aware physical reclaim.
#[instrument(name = "db_cleanup", skip_all)]
async fn db_cleanup_task(db: DBRef) {
    tokio::time::sleep(CLEANUP_INTERVAL).await;
    loop {
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || db.cleanup()).await;

        let failed = match result {
            Ok(Ok(purged)) => {
                if purged > 0 {
                    debug!("cleanup: logically purged {purged} table(s)");
                }
                false
            }
            Ok(Err(err)) => {
                error!(error =? err, "database cleanup failed");
                true
            }
            Err(_) => {
                error!("database cleanup task panicked");
                true
            }
        };

        tokio::time::sleep(if failed {
            CLEANUP_ERROR_BACKOFF
        } else {
            CLEANUP_INTERVAL
        })
        .await;
    }
}

/// Whole-file reclaim whose watermark includes every deleted table still visible to the
/// oldest RocksDB snapshot. It never waits for the global snapshot count to reach zero.
#[instrument(name = "db_reclaim", skip_all)]
async fn db_reclaim_task(db: DBRef, interval: Duration) {
    tokio::time::sleep(interval).await;
    loop {
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || db.reclaim_disk_space_runtime()).await;

        let failed = match result {
            Ok(Ok(report)) => {
                // TODO: Export this report only when there is a concrete dashboard/alert
                // contract; structured debug fields are sufficient for the current rollout.
                debug!(
                    snapshots = report.snapshot_count,
                    oldest_snapshot_sequence = report.oldest_snapshot_sequence,
                    safe_deleted_tables = report.safe_deleted_tables,
                    unsafe_deleted_tables = report.unsafe_deleted_tables,
                    watermark =? report.watermark,
                    "snapshot-aware disk reclaim completed"
                );
                if report.skipped_malformed_chunks > 0 {
                    warn!(
                        malformed_chunks = report.skipped_malformed_chunks,
                        "runtime reclaim ignored unreadable committed chunks"
                    );
                }
                false
            }
            Ok(Err(err)) => {
                error!(error =? err, "snapshot-aware disk reclaim failed");
                true
            }
            Err(err) => {
                error!(error =? err, "snapshot-aware disk reclaim task panicked");
                true
            }
        };

        tokio::time::sleep(db_reclaim_delay(interval, failed)).await;
    }
}

fn db_reclaim_delay(interval: Duration, failed: bool) -> Duration {
    if failed {
        interval.max(CLEANUP_ERROR_BACKOFF)
    } else {
        interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reclaim_errors_never_retry_faster_than_the_configured_interval() {
        assert_eq!(
            db_reclaim_delay(Duration::from_secs(300), true),
            Duration::from_secs(300)
        );
        assert_eq!(db_reclaim_delay(Duration::from_secs(2), true), CLEANUP_ERROR_BACKOFF);
        assert_eq!(
            db_reclaim_delay(Duration::from_secs(300), false),
            Duration::from_secs(300)
        );
    }
}
