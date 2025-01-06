mod archive;
mod chain_builder;
mod cli;
mod fs;
mod ingest;
mod layout;
mod processor;
mod progress;
mod sink;
mod writer;


fn init_logging(json: bool) {
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_target(false)
            .json()
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_target(false)
            .init();
    }
}


fn main() -> anyhow::Result<()> {
    let args = <cli::Cli as clap::Parser>::parse();

    init_logging(args.json_log);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    
    runtime.block_on(archive::run(&args))?;
    Ok(())
}
