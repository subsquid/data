#![allow(unused)]
mod block;
mod cassandra;
mod chain;
mod chain_watch;
mod cmd;
mod ingest;
mod util;


use clap::Parser;


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
    match cli.command { 
        Command::Ingest(args) => {
            cmd::ingest::run(args)
        } 
    }
}