#![allow(dead_code)]
#![allow(unused_imports)]
use anyhow::anyhow;
use sqd_query::{JsonArrayWriter, ParquetChunk, Query, set_polars_thread_pool_size};


fn main() -> anyhow::Result<()> {
    unsafe { set_polars_thread_pool_size(8) };

    let data_chunk_path = std::env::args().skip(1).next().ok_or_else(|| {
        anyhow!("specify a data chunks as a first argument")
    })?;

    let query = read_query()?;
    let plan = query.compile();

    let data_chunk = ParquetChunk::new(data_chunk_path);

    let query_start = std::time::Instant::now();

    let mut result = polars_core::POOL.install(|| plan.execute(&data_chunk))?;

    let query_time = query_start.elapsed().as_millis();

    let serialization_start = std::time::Instant::now();

    let buf = Vec::with_capacity(24 * 1024 * 1024);
    let mut writer = JsonArrayWriter::new(buf);
    writer.write_blocks(&mut result)?;
    let buf = writer.finish()?;

    let serialization_time = serialization_start.elapsed().as_micros();

    println!("{}", std::str::from_utf8(&buf).unwrap());

    println!("query completed in: {} ms", query_time);
    result.print_summary();
    println!("serialization completed in: {} us", serialization_time);
    println!("result size: {:.0} kb", buf.len() as f32 / 1024.0);

    Ok(())
}


fn read_query() -> anyhow::Result<Query> {
    let file = std::env::args().skip(2).next().ok_or_else(|| {
        anyhow!("specify query file as a second argument")
    })?;

    let bytes = std::fs::read(&file)?;

    Query::from_json_bytes(&bytes)
}