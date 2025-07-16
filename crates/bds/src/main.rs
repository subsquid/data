#![allow(unused)]
mod block;
mod cassandra;
mod chain;
mod chain_watch;
mod cmd;
mod ingest;
mod util;


use clap::Parser;
use std::io::IsTerminal;


#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


#[derive(clap::Parser)]
struct CLI {
    #[command(subcommand)]
    command: Command
}


#[derive(clap::Subcommand)]
enum Command {
    /// Run data ingestion
    Ingest(cmd::ingest::Args)
}


fn main() -> anyhow::Result<()> {
    let cli = CLI::parse();

    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or("info".to_string()),
    );

    if std::io::stdout().is_terminal() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_current_span(false)
            .init();
    }
    
    match cli.command { 
        Command::Ingest(args) => {
            cmd::ingest::run(args)
        } 
    }
}