use clap::Parser;
use sqd_writer::sink::Sink;
use sqd_writer::fs::create_fs;
use sqd_writer::ingest::{ingest_from_service, BlockRange};
use sqd_writer::layout::ChunkWriter;
use sqd_writer::solana::writer::ParquetWriter;
use sqd_writer::solana::model::Block;

#[derive(Parser)]
struct Cli {
    /// target dir or s3 location to write data to
    pub dest: String,

    /// first block of a range to write
    #[clap(long, default_value = "0", value_name = "N")]
    pub first_block: u64,

    /// last block of a range to write
    #[clap(long, value_name = "N")]
    pub last_block: Option<u64>,

    /// data chunk size in roughly estimated megabytes
    #[clap(long, default_value = "1024", value_name = "MB")]
    pub chunk_size: usize,
}

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

    let fs = create_fs(&args.dest)?;
    let chunk_writer = ChunkWriter::new(
        &fs,
        chunk_check,
        args.first_block,
        args.last_block,
        4096,
    )?;
    let range = BlockRange::new(chunk_writer.next_block(), args.last_block);
    let stream = ingest_from_service::<Block>("http://localhost:7373", &range)?;
    let writer = ParquetWriter::new();
    let mut sink = Sink::new(writer, chunk_writer, &fs, args.chunk_size);
    sink.write(stream)?;

    Ok(())
}
