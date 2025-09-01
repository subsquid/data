mod archive;
mod chunk_writer;
mod cli;
mod fs;
mod ingest;
mod layout;
mod metrics;
mod proc;
mod progress;
mod server;
mod writer;


fn main() -> anyhow::Result<()> {
    let args = <cli::Cli as clap::Parser>::parse();

    init_logging(args.json_log);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(archive::run(args))
}


fn init_logging(json: bool) {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or(format!("{}=info", env!("CARGO_CRATE_NAME"))),
    );

    if json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .json()
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .init();
    }
}
