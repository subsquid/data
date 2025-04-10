mod cli;
mod dataset_config;
mod app;


use crate::app::build_app;
use crate::cli::CLI;
use clap::Parser;
use sqd_node::DBRef;
use std::time::Duration;
use tokio::signal;
use tower_http::timeout::TimeoutLayer;
use tracing::error;


#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


fn main() -> anyhow::Result<()> {
    let args = CLI::parse();
    
    if let Some(n_threads) = args.query_threads {
        unsafe {
            sqd_polars::set_polars_thread_pool_size(n_threads);
        }
    }

    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or("info".to_string()),
    );

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .compact()
        .init();
    
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let (node, db) = args.build_node().await?;

            tokio::spawn(db_cleanup_task(db));

            let app = build_app(node).layer(TimeoutLayer::new(Duration::from_secs(10)));

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await?;

            Ok::<_, anyhow::Error>(())
        })?;
    
    Ok(())
}


async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
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


async fn db_cleanup_task(db: DBRef) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let db = db.clone();
        let result = tokio::task::spawn_blocking(move || db.cleanup()).await;
        match result {
            Ok(Ok(())) => {},
            Ok(Err(err)) => error!(error =? err, "database cleanup task failed"),
            Err(_) => error!("database cleanup task panicked")
        }
    }
}