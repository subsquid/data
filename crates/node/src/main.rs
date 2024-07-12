use std::sync::Arc;

use anyhow::Context;
use clap::Parser;

use sqd_storage::db::Database;

use crate::api::Api;
use crate::cli::CLI;
use crate::config::Config;


mod config;
mod dataset_kind;
mod cli;
mod api;


fn main() -> anyhow::Result<()> {
    let args = CLI::parse();

    let config = Config::read(&args.config).with_context(|| {
        format!("failed to read config from '{}'", args.config)
    })?;

    let db = Database::open(&args.database_dir).context("failed to open database")?;

    for (dataset, options) in config.datasets.iter() {
        db.create_dataset_if_not_exists(*dataset, options.kind.storage_kind())?;
    }
    
    let api = Api::new(Arc::new(db), &config);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            use axum::Router;
            use axum::routing::get;
            
            let app = Router::new()
                .route("/", get(|| async { "Hello, World!" }))
                .nest("/dataset", api.build_router());

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
            axum::serve(listener, app).await?;
            
            Ok::<(), anyhow::Error>(())
        })?;

    Ok(())
}