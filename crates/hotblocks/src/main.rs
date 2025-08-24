#![allow(unused)]
mod api;
mod cli;
mod data_service;
mod dataset_config;
mod dataset_controller;
mod errors;
mod metrics;
mod query;
mod types;


use api::build_api;
use clap::Parser;
use cli::CLI;
use std::time::Duration;
use tracing::{debug, error, instrument};
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

            tokio::spawn(db_cleanup_task(app.db.clone()));

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

    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or("info".to_string()),
    );

    if std::io::stdout().is_terminal() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .init();
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
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
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


#[instrument(name = "db_cleanup", skip_all)]
async fn db_cleanup_task(db: DBRef) {
    tokio::time::sleep(Duration::from_secs(10)).await;
    loop {
        debug!("db cleanup started");
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || db.cleanup()).await;
        match result {
            Ok(Ok(deleted)) => {
                if deleted > 0 {
                    debug!("purged {} tables", deleted)
                } else {
                    debug!("nothing to purge, pausing cleanup for 10 seconds");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            },
            Ok(Err(err)) => error!(error =? err, "database cleanup task failed"),
            Err(_) => error!("database cleanup task panicked")
        }
    }
}