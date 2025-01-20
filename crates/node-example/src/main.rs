mod cli;
mod dataset_config;
mod app;


use crate::app::build_app;
use crate::cli::CLI;
use clap::Parser;


#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


fn main() -> anyhow::Result<()> {
    let args = CLI::parse();

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
            let node = args.build_node()?;
            let app = build_app(node);
            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
            axum::serve(listener, app).await?;
            Ok::<_, anyhow::Error>(())
        })?;
    
    Ok(())
}
