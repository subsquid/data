use clap::Parser;
use ingest::{ingest_from_service, BlockRange};
use sink::Sink;
use layout::ChunkWriter;
use writer::ParquetWriter;

use sqd_data::solana::tables::SolanaChunkBuilder;

mod fs;
mod ingest;
mod sink;
mod layout;
mod writer;
mod cli;

fn chunk_check(filelist: &[String]) -> bool {
    for file in filelist {
        if file.starts_with("blocks.parquet") {
            return true;
        }
    }
    return false;
}

fn main() -> anyhow::Result<()> {
    let args = cli::Cli::parse();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime = std::rc::Rc::new(runtime);
    let _guard = runtime.enter();

    let range = BlockRange::new(args.first_block, args.last_block);
    let stream = ingest_from_service(&args.src, &range)?;
    let fs = fs::create_fs(&args.dest, runtime.clone())?;
    let chunk_builder = Box::new(SolanaChunkBuilder::new());
    let writer = ParquetWriter::new(chunk_builder);
    let chunk_writer = ChunkWriter::new(&fs, chunk_check, args.first_block, args.last_block, args.top_dir_size)?;
    let mut sink = Sink::new(writer, chunk_writer, &fs, args.chunk_size);
    sink.write(stream)?;

    Ok(())
}
