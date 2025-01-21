use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use proptest::prelude::{prop, ProptestConfig};
use proptest::proptest;
use proptest::strategy::Strategy;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::reader::ArrayReader;
use sqd_storage::db::{Database, DatabaseSettings};
use sqd_storage::table::write::use_small_buffers;
use std::sync::Arc;


mod arb_array;


fn to_record_batch(array: ArrayRef) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("c0", array.data_type().clone(), true)
    ]);

    RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap()
}


fn check_write_read(db: &Database, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
    let schema = batches[0].schema();

    let mut builder = db.new_table_builder(schema.clone());
    for batch in batches.iter() {
        builder.write_record_batch(batch)?;
    }
    let table_id = builder.finish()?;

    let snapshot = db.snapshot();
    let reader = snapshot.create_table_reader(table_id)?;

    let input_table = arrow::compute::concat_batches(&schema, batches.iter())?;

    let par_result = reader.read_table(None, None)?;
    assert_eq!(input_table, par_result);

    let seq_result = {
        let mut array_builder = AnyBuilder::new(schema.field(0).data_type());
        let mut seq_reader = reader.create_column_reader(0)?;
        seq_reader.read(&mut array_builder)?;
        to_record_batch(array_builder.finish())
    };
    assert_eq!(input_table, seq_result);

    Ok(())
}


fn test_write_read(array: impl Strategy<Value=ArrayRef>) -> anyhow::Result<()> {
    let db_dir = tempfile::tempdir()?;
    let db = DatabaseSettings::default().open(db_dir.path())?;
    let _sg = use_small_buffers();

    let tables_strategy = prop::collection::vec(array.prop_map(to_record_batch), 1..=2);

    proptest!(ProptestConfig::with_cases(1000), |(tables in tables_strategy)| {
        check_write_read(&db, tables).unwrap()
    });

    Ok(())
}


#[test]
fn uint32_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::uint32(0..200))).unwrap()
}