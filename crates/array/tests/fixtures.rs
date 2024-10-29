use arrow::array::{Array, RecordBatch, StructArray, UInt32Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::io::file::ArrayFile;
use sqd_array::reader::ArrayReader;
use sqd_array::slice::{AsSlice, Slice};
use sqd_array::sort::sort_record_batch;
use std::fs::File;
use std::path::Path;


#[test]
fn test_write_slice_to_builder() -> anyhow::Result<()> {
    let records = load_parquet_fixture("solana-instructions.parquet")?;
    let src = StructArray::from(records);
    
    let mut builder = AnyBuilder::new(src.data_type());
    src.as_slice().write(&mut builder)?;
    let result = builder.finish();
    
    assert_eq!(result.to_data(), src.to_data());
    
    Ok(())
}


#[test]
fn test_write_reversed_slice_to_builder() -> anyhow::Result<()> {
    let records = load_parquet_fixture("solana-instructions.parquet")?;
    let src = StructArray::from(records);
    let indexes = UInt32Array::from_iter_values((0..src.len() as u32).rev());
    let reference = arrow::compute::take(&src, &indexes, None)?;

    let result = {
        let mut builder = AnyBuilder::new(src.data_type());
        
        src.as_slice().write_indexes(
            &mut builder,
            indexes.values().iter().map(|i| *i as usize)
        )?;
        
        builder.finish()
    };
    
    assert_eq!(result.to_data(), reference.to_data());
    
    Ok(())
}


#[test]
fn test_in_memory_sort() -> anyhow::Result<()> {
    let src = load_parquet_fixture("solana-instructions.parquet")?;
    
    let result = sort_record_batch(&src, [
        "program_id",
        "block_number",
        "transaction_index",
        "instruction_address"
    ])?;

    let reference = load_parquet_fixture("solana-instructions.sorted.parquet")?;

    assert_eq!(result, reference);

    Ok(())
}


#[test]
fn test_array_file_write_read() -> anyhow::Result<()> {
    let records = load_parquet_fixture("solana-instructions.parquet")?;
    let src = StructArray::from(records);
    
    let file = {
        let mut writer = ArrayFile::new_temporary(src.data_type().clone())?.write()?;
        src.as_slice().write(&mut writer)?;
        writer.finish()?
    };
    
    let result = {
        let mut builder = AnyBuilder::new(src.data_type());
        file.read()?.read(&mut builder)?;
        builder.finish()
    };
    
    assert_eq!(result.to_data(), src.to_data());
    
    Ok(())
}


fn load_parquet_fixture(name: &str) -> anyhow::Result<RecordBatch> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures").join(name);
    let file = File::open(path)?;
    
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(1_000_000)
        .build()?;
    
    let record_batch = reader.next().unwrap()?;
    Ok(record_batch)
}