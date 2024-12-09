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
    let range = BlockRange::new(250_000_000, Some(250_001_000));
    let stream = ingest_from_service("http://localhost:7373", &range)?;
    let fs = fs::create_fs("data/solana")?;
    let chunk_builder = Box::new(SolanaChunkBuilder::new());
    let writer = ParquetWriter::new(chunk_builder);
    let chunk_writer = ChunkWriter::new(&fs, chunk_check, 250_000_000, Some(250_001_000), 4096)?;
    let chunk_size = 256;
    let mut sink = Sink::new(writer, chunk_writer, &fs, chunk_size);
    sink.write(stream)?;

    Ok(())
}
