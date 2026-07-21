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

use std::{
    future::{Future, IntoFuture},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering}
    },
    time::{Duration, Instant}
};

use anyhow::Context;
use api::build_api;
use clap::Parser;
use cli::CLI;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
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

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;

    // Outlives the runtime on purpose -- see `flush_and_abandon_db`.
    let mut db = None;

    let result = runtime.block_on(async {
        let app = args.build_app().await?;
        db = Some(app.db.clone());

        // NB: startup disk recovery (orphan purge + file unlink, gated by
        // --startup-disk-reclaim) already ran inside `build_app` -> `DataService::start`,
        // before any controller spawned.
        tokio::spawn(db_cleanup_task(app.db.clone()));

        let shutting_down = Arc::new(AtomicBool::new(false));
        let drain = CancellationToken::new();

        let sigterm = signal(SignalKind::terminate()).context("failed to install the SIGTERM handler")?;
        tokio::spawn(watch_shutdown_signal(
            sigterm,
            shutting_down.clone(),
            drain.clone(),
            Duration::from_secs(args.pre_drain_grace_secs)
        ));

        let api = build_api(app, shutting_down);

        let listener = tokio::net::TcpListener::bind(("0.0.0.0", args.port)).await?;

        info!("server started, listening at {}", listener.local_addr()?);

        let serve = axum::serve(listener, api).with_graceful_shutdown({
            let drain = drain.clone();
            async move { drain.cancelled().await }
        });

        drive_serve_with_drain(serve.into_future(), drain, Duration::from_secs(args.drain_timeout_secs)).await?;

        Ok::<_, anyhow::Error>(())
    });

    // Compactions run back-to-back on the blocking pool, and the runtime's destructor joins
    // whichever is mid-flight -- that, not the drain, is what used to set the exit time.
    // What gets cut is an uncommitted RocksDB transaction, which CN-6 already treats as lost.
    runtime.shutdown_background();

    if let Some(db) = db {
        flush_and_abandon_db(db);
    }

    result
}

/// Exit without closing the database. `rocksdb_close` waits out the background compactions,
/// which is the unbounded tail `--drain-timeout-secs` exists to remove; skipping it makes
/// every exit crash-equivalent, which CN-6 requires us to survive anyway. The memtables are
/// flushed first so the price is not simply moved to the next boot's WAL replay -- a start
/// path with a far tighter budget than this one (GAP-7).
fn flush_and_abandon_db(db: DBRef) {
    let started = Instant::now();
    match db.flush_all() {
        Ok(()) => info!(elapsed_ms = started.elapsed().as_millis() as u64, "memtables flushed"),
        Err(err) => warn!(error =? err, "failed to flush memtables, the next boot replays the WAL")
    }

    // Dropping the last handle is what calls `rocksdb_close`, so this one must not be
    // dropped. Any clone still held by a detached blocking job outlives the process.
    std::mem::forget(db)
}

/// SIGINT stays on the default handler: Ctrl-C in dev must not sit out the grace window.
/// Only the first SIGTERM is acted on; a repeat is ignored on purpose -- anyone wanting an
/// immediate exit already has SIGINT and SIGKILL.
async fn watch_shutdown_signal(
    mut sigterm: Signal,
    shutting_down: Arc<AtomicBool>,
    drain: CancellationToken,
    pre_drain_grace: Duration
) {
    sigterm.recv().await;
    info!("SIGTERM received, starting the shutdown sequence");
    run_shutdown_sequence(shutting_down, drain, pre_drain_grace).await
}

/// Report unready while still serving normally, so the orchestrator withdraws our endpoint
/// before anything closes.
async fn run_shutdown_sequence(shutting_down: Arc<AtomicBool>, drain: CancellationToken, pre_drain_grace: Duration) {
    shutting_down.store(true, Ordering::Relaxed);
    info!(
        grace_secs = pre_drain_grace.as_secs(),
        "/ready now reports 503; serving on until the grace window elapses"
    );
    tokio::time::sleep(pre_drain_grace).await;
    info!("pre-drain grace elapsed, draining the HTTP server");
    drain.cancel()
}

/// Nothing bounds a response today (GAP-29), so without a deadline the drain just waits for
/// the orchestrator's kill.
async fn drive_serve_with_drain<F>(serve: F, drain: CancellationToken, drain_timeout: Duration) -> std::io::Result<()>
where
    F: Future<Output = std::io::Result<()>>
{
    let deadline = async {
        drain.cancelled().await;
        tokio::time::sleep(drain_timeout).await
    };

    tokio::select! {
        res = serve => {
            res?;
            info!("HTTP server drained cleanly");
        }
        _ = deadline => {
            warn!(
                timeout_secs = drain_timeout.as_secs(),
                "drain timeout exceeded, in-flight connections are aborted on exit"
            );
        }
    }

    Ok(())
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
            // Event fields land at the top level instead of nested under `fields`, so a log
            // sink queries `jsonPayload.freed_bytes` rather than `jsonPayload.fields.freed_bytes`.
            // Matches what `sqd-archive` already does.
            .flatten_event(true)
            .with_current_span(false)
            .init();
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    /// Guards the shape of `main`: a runtime dropped rather than backgrounded joins the
    /// blocking job and the drain deadline stops bounding the exit.
    #[test]
    fn exit_does_not_wait_for_a_running_blocking_job() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let entered = Arc::new(std::sync::Barrier::new(2));
        let in_job = entered.clone();
        runtime.spawn_blocking(move || {
            in_job.wait();
            std::thread::sleep(Duration::from_secs(30))
        });
        entered.wait();

        let started = std::time::Instant::now();
        runtime.shutdown_background();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "exit joined the blocking job"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_grace_window_holds_the_drain_but_not_the_unready_flag() {
        let shutting_down = Arc::new(AtomicBool::new(false));
        let drain = CancellationToken::new();
        let grace = Duration::from_secs(5);

        let task = tokio::spawn(run_shutdown_sequence(shutting_down.clone(), drain.clone(), grace));
        tokio::task::yield_now().await;

        assert!(shutting_down.load(Ordering::Relaxed), "unready immediately");
        assert!(!drain.is_cancelled(), "drain held for the whole window");

        tokio::time::advance(grace - Duration::from_millis(1)).await;
        assert!(!drain.is_cancelled());

        tokio::time::advance(Duration::from_millis(2)).await;
        task.await.unwrap();
        assert!(drain.is_cancelled());
    }

    #[tokio::test]
    async fn a_drain_that_finishes_inside_the_deadline_returns_ok() {
        let drain = CancellationToken::new();
        let serve = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        };

        drain.cancel();
        drive_serve_with_drain(serve, drain, Duration::from_secs(30))
            .await
            .expect("clean drain");
    }

    #[tokio::test]
    async fn a_response_that_never_finishes_hits_the_deadline() {
        let drain = CancellationToken::new();
        let serve = std::future::pending::<std::io::Result<()>>();

        drain.cancel();
        drive_serve_with_drain(serve, drain, Duration::from_millis(20))
            .await
            .expect("the deadline path still exits Ok");
    }

    #[tokio::test]
    async fn a_serve_error_propagates() {
        let serve = async { Err(std::io::Error::other("listener failed")) };

        let res = drive_serve_with_drain(serve, CancellationToken::new(), Duration::from_secs(30)).await;
        assert!(res.is_err());
    }
}

const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
/// Longer pause after a failed tick, so a persistent error (e.g. full disk) doesn't
/// busy-loop failing writes.
const CLEANUP_ERROR_BACKOFF: Duration = Duration::from_secs(30);

/// Routine Phase-1 cleanup: point-delete deleted tables' data every tick so compaction
/// can reclaim their space -- in normal operation the entire runtime reclaim path.
///
/// The physical file unlink (`Database::reclaim_disk_space`) is NOT run here: it ignores
/// snapshots and could break an in-flight query, so it runs only at startup. FUTURE:
/// trigger it here under disk pressure too, accepting that risk.
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
