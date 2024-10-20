use arrow::array::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use sqd_array::sort::sort_record_batch;


fn sort_setup(c: &mut Criterion) {
    let records = read_parquet("fixtures/solana-transactions.parquet").unwrap();
    
    c.bench_function("sort solana transactions by idx: SQD", |bench| {
        bench.iter(|| {
            sort_record_batch(&records, ["_idx"]).expect("sorting failed")
        })
    });
    
    c.bench_function("sort solana transactions by idx: ARROW", |bench| {
        bench.iter(|| {
            use arrow::compute::*;
            
            let indexes = sort_to_indices(
                records.column_by_name("_idx").unwrap(),
                None,
                None
            ).unwrap();
            
            take_record_batch(&records, &indexes).unwrap()
        })
    });
}


criterion_group!(sorting, sort_setup);
criterion_main!(sorting);


fn read_parquet(path: &str) -> anyhow::Result<RecordBatch> {
    use std::fs::File;
    use std::path::Path;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    let file = File::open(path)?;

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(1_000_000)
        .build()?;

    let record_batch = reader.next().expect("no record batches")?;
    Ok(record_batch)
}