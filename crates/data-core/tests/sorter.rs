use arrow::array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqd_array::builder::AnyTableBuilder;
use sqd_array::slice::AsSlice;
use sqd_data_core::TableSorter;
use std::fs::File;
use std::path::Path;


#[test]
fn sort_ethereum_transactions() -> anyhow::Result<()> {
    let src_reader = open_parquet("fixtures/ethereum-transactions.parquet")?.build()?;

    let mut sorter = TableSorter::new(
        src_reader.schema().fields(),
        [
            "sighash",
            "to",
            "block_number",
            "transaction_index"
        ].iter().map(|name| {
            src_reader.schema().index_of(name).unwrap()
        }).collect(),
    )?;

    for record_batch in src_reader {
        let record_batch = record_batch?;
        sorter.push_batch(&record_batch.as_slice())?
    }

    let mut sorted = sorter.finish()?;

    let mut ref_reader = open_parquet("fixtures/ethereum-transactions.sorted.parquet")?
        .with_batch_size(33)
        .build()?;

    let mut pos = 0;
    for record_batch in ref_reader {
        let record_batch = record_batch?;
        
        let mut builder = AnyTableBuilder::new(record_batch.schema());
        for i in 0..builder.num_columns() {
            let mut dst = builder.column_writer(i);
            sorted.read_column(&mut dst, i, pos, record_batch.num_rows())?;
        }

        let result = builder.finish();
        if result != record_batch {
            for i in 0..record_batch.num_columns() {
                let res = result.column(i);
                let reference = record_batch.column(i);
                assert_eq!(
                    res, reference,
                    "record batches at column={}, pos={} are different",
                    i, pos
                );
            }
            panic!("record batches at pos={} are different", pos);
        }

        pos += record_batch.num_rows();
    }

    Ok(())
}


fn open_parquet(path: &str) -> anyhow::Result<ParquetRecordBatchReaderBuilder<File>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(4000);
    Ok(reader)
}
