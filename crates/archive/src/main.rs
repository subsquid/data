use ingest::{ingest_from_service, BlockRange};
use sink::Sink;
use layout::ChunkWriter;
use writer::ParquetWriter;

use sqd_data::solana::model::Block;

mod fs;
mod ingest;
mod sink;
mod layout;
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
    let range = BlockRange::new(250_000_000, Some(250_001_000));
    let stream = ingest_from_service::<Block>("http://localhost:7373", &range)?;
    let fs = fs::create_fs("data/solana")?;
    let writer = ParquetWriter::new();
    let chunk_writer = ChunkWriter::new(&fs, chunk_check, 250_000_000, Some(250_001_000), 4096)?;
    let chunk_size = 256;
    let mut sink = Sink::new(writer, chunk_writer, &fs, chunk_size);
    sink.write(stream)?;

    Ok(())
}
