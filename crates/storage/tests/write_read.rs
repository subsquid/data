use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use proptest::prelude::{prop, ProptestConfig};
use proptest::proptest;
use proptest::strategy::Strategy;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::reader::ArrayReader;
use sqd_storage::db::{Database, DatabaseSettings};
use sqd_storage::table::write::use_small_buffers;
use core::assert_eq;
use core::convert::TryFrom;
use std::sync::Arc;

mod arb_array;

const WRITE_READ_ARRAY_SIZE: usize = 300;
// const WRITE_READ_ITERATIONS: u32 = 100;
const WRITE_READ_ITERATIONS: u32 = 10;

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
    let have_stats = builder.set_stats([0]).is_ok();
    let table_id = builder.finish()?;

    let snapshot = db.snapshot();
    let reader = snapshot.create_table_reader(table_id)?;

    let input_table = arrow::compute::concat_batches(&schema, batches.iter())?;
    if have_stats {
        let stats = reader.get_column_stats(0)?.unwrap();
        assert_eq!(input_table.num_rows(), *stats.offsets.last().unwrap() as usize);
        // TODO: MIN MAX
    }
    assert_eq!(input_table.num_rows(), reader.num_rows());

    let par_result = reader.read_table(None, None)?;
    assert_eq!(input_table, par_result);

    let midway = input_table.num_rows() / 2;
    if midway > 0 {
        let range_list = sqd_primitives::range::RangeList::try_from([0..midway as u32].to_vec()).unwrap();
        let half_result = reader.read_table(None, Some(&range_list))?;
        assert_eq!(input_table.slice(0, midway), half_result);
    }

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

    proptest!(ProptestConfig::with_cases(WRITE_READ_ITERATIONS), |(tables in tables_strategy)| {
        check_write_read(&db, tables).unwrap()
    });

    Ok(())
}

#[test]
fn uint8_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::uint8(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn uint16_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::uint16(0..WRITE_READ_ARRAY_SIZE))).unwrap()
    // test_write_read(arb_array::uint16(0..WRITE_READ_ARRAY_SIZE)).unwrap()
}

#[test]
fn uint32_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::uint32(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn uint64_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::uint64(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn int8_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::int8(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn int16_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::int16(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn int32_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::int32(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn int64_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::int64(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn boolean_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::boolean(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn binary_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::binary(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn fixed_size_binary_write_read() {
    // Strategy returned from `fixed_size_binary` within a single call to `test_write_read` has to generate
    // arrays with the same sizes so that they can be concatenated into a single RecordBatch.
    test_write_read(arb_array::with_nullmask(arb_array::fixed_size_binary(0..WRITE_READ_ARRAY_SIZE, 12))).unwrap();
    test_write_read(arb_array::with_nullmask(arb_array::fixed_size_binary(0..WRITE_READ_ARRAY_SIZE, 128))).unwrap();
}

#[test]
fn string_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::string(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn timestamp_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::timestamp(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn list_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::list(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}

#[test]
fn struct_write_read() {
    test_write_read(arb_array::with_nullmask(arb_array::structs(0..WRITE_READ_ARRAY_SIZE))).unwrap()
}
