mod archive;
mod chain_builder;
mod cli;
mod fs;
mod ingest;
mod layout;
mod writer;


fn main() -> anyhow::Result<()> {
    let args = <cli::Cli as clap::Parser>::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    
    runtime.block_on(archive::run(&args))?;
    Ok(())
}
