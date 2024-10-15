use arrow::array::{Array, RecordBatch, StructArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::io::file::ArrayFile;
use sqd_array::reader::ArrayReader;
use sqd_array::slice::{AsSlice, Slice};
use std::fs::File;
use std::path::Path;


#[test]
fn builder_write() -> anyhow::Result<()> {
    let records = load_parquet_fixture("solana-instructions.parquet")?;
    let src = StructArray::from(records);
    
    let mut builder = AnyBuilder::new(src.data_type());
    src.as_slice().write(&mut builder)?;
    let result = builder.finish();
    
    assert_eq!(result.to_data(), src.to_data());
    
    Ok(())
}


#[test]
fn array_file_write_read() -> anyhow::Result<()> {
    let records = load_parquet_fixture("solana-instructions.parquet")?;
    let src = StructArray::from(records);
    
    let mut file = ArrayFile::new_temporary(src.data_type().clone())?;
    {
        let mut writer = file.write()?;
        src.as_slice().write(&mut writer)?;
        writer.finish()?;
    }
    
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