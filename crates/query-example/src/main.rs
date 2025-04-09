use anyhow::{bail, Context};
use sqd_query::{ParquetChunk, Query};
use std::fs::File;
use std::io::Write;
use std::time::Instant;
use flate2::Compression;
use flate2::write::GzEncoder;


fn main() -> anyhow::Result<()> {
    unsafe {
        sqd_polars::set_polars_thread_pool_size(4);
    }
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        bail!("Usage: {} <output_file> <chunk_path> <query_file>", args[0]);
    }

    let output_file = &args[1];
    let chunk_path = &args[2];
    let query_path = &args[3];

    let start = Instant::now();
    let query_str = std::fs::read_to_string(query_path).context("Failed to read the query file")?;
    let query = Query::from_json_bytes(query_str.as_bytes())?;
    let plan = query.compile();
    let preparation = start.elapsed();

    let chunk = ParquetChunk::new(chunk_path);
    let blocks = plan.execute(&chunk);
    let execution = start.elapsed();

    let data = Vec::with_capacity(1024 * 1024);
    let mut writer = sqd_query::JsonLinesWriter::new(data);
    if let Some(mut blocks) = blocks? {
        writer.write_blocks(&mut blocks)?;
    }
    let bytes = writer.finish()?;
    let rendering = start.elapsed();

    let bytes = {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&bytes)?;
        encoder.finish()?
    };
    let compression = start.elapsed();

    let mut out = File::create(output_file)?;
    out.write_all(&bytes)?;
    out.sync_all()?;
    let writing = start.elapsed();

    println!(
        "preparation: {} ms, execution: {} ms, rendering: {} ms, compression: {} ms, writing: {} ms",
        preparation.as_millis(),
        (execution - preparation).as_millis(),
        (rendering - execution).as_millis(),
        (compression - rendering).as_millis(),
        (writing - compression).as_millis()
    );

    Ok(())
}
