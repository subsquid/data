use clap::Parser;
use cli::{Cli, NetworkKind};
use ingest::{ingest_from_service, BlockRange};
use layout::ChunkWriter;
use sink::Sink;
use sqd_data::solana::tables::SolanaChunkBuilder;
use writer::ParquetWriter;


mod cli;
mod fs;
mod ingest;
mod layout;
mod sink;
mod writer;


fn chunk_check(filelist: &[String]) -> bool {
    for file in filelist {
        if file.starts_with("blocks.parquet") {
            return true;
        }
    }
    return false;
}


fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime = std::rc::Rc::new(runtime);
    let _guard = runtime.enter();

    let fs = fs::create_fs(&args.dest, runtime.clone())?;
    let chunk_builder = match args.network_kind {
        NetworkKind::Solana => Box::new(SolanaChunkBuilder::new()),
    };
    let writer = ParquetWriter::new(chunk_builder);
    let chunk_writer = ChunkWriter::new(
        &fs,
        chunk_check,
        args.first_block,
        args.last_block,
        args.top_dir_size,
    )?;
    let first_block = chunk_writer.next_block();

    if let Some(last_block) = args.last_block {
        if first_block > last_block {
            println!("nothing to do");
            return Ok(());
        }
    }

    let range = BlockRange::new(first_block, args.last_block);
    let stream = ingest_from_service(&args.src, &range)?;
    let mut sink = Sink::new(writer, chunk_writer, &fs, args.chunk_size);
    sink.write(stream)?;

    Ok(())
}
