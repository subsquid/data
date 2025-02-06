use sqd_array::builder::UInt32Builder;
use sqd_array::slice::AsSlice;
use sqd_data_core::table_builder;


table_builder! {
    TableBuilder {
        col: UInt32Builder,
    }

    description(d) {}
}


#[test]
fn test_processor_reuse() -> anyhow::Result<()> {
    let mut table_builder = TableBuilder::new();
    let mut processor = table_builder.new_table_processor();

    for i in 0..10 {
        table_builder.col.append_option((i % 2 == 0).then_some(i));
    }

    processor.push_batch(&table_builder.as_slice())?;
    
    let mut prepared = processor.finish()?;
    let _ = prepared.read_record_batch(0, prepared.num_rows())?;
    
    processor = prepared.into_processor()?;
    processor.push_batch(&table_builder.as_slice())?;
    prepared = processor.finish()?;

    let _records = prepared.read_record_batch(0, prepared.num_rows())?;
    
    Ok(())
}