use arrow::array::{
    ArrayRef, AsArray, Float32Array, Int32Array, RecordBatch, UInt16Array, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use sqd_array::schema_metadata::set_sort_key;
use sqd_storage::db::ops::MIN_CHUNK_SIZE;
use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind, ReadSnapshot};
use sqd_storage::table::write::use_small_buffers;
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn setup_db() -> (Database, DatasetId) {
    let db_dir = tempfile::tempdir().unwrap();
    let db = DatabaseSettings::default()
        .with_rocksdb_stats(true)
        .open(db_dir.path())
        .unwrap();
    let _sg = use_small_buffers();

    let name = "solana";
    let kind = "solana";
    let dataset_id = DatasetId::from_str(name);
    let dataset_kind = DatasetKind::from_str(kind);
    db.create_dataset(dataset_id, dataset_kind).unwrap();
    (db, dataset_id)
}

pub fn validate_chunks(db: &Database, dataset_id: DatasetId, valid_chunks: Vec<&Chunk>) {
    let snapshot = db.snapshot();

    let mut chunks = snapshot.list_chunks(dataset_id, 0, None);
    for chunk in &valid_chunks {
        let res_chunks = chunks.next();
        assert_eq!(res_chunks.unwrap().unwrap(), (*chunk).clone());
    }
    assert!(chunks.next().is_none());

    if valid_chunks.is_empty() {
        assert_eq!(snapshot.get_first_chunk(dataset_id).unwrap(), None);
        assert_eq!(snapshot.get_last_chunk(dataset_id).unwrap(), None);
    } else {
        assert_eq!(
            snapshot.get_first_chunk(dataset_id).unwrap().unwrap(),
            **valid_chunks.first().unwrap()
        );
        assert_eq!(
            snapshot.get_last_chunk(dataset_id).unwrap().unwrap(),
            **valid_chunks.last().unwrap()
        );
    }
}

pub fn read_chunk(snapshot: &ReadSnapshot, chunk: Chunk) -> Vec<(u32, u32)> {
    let chunk_reader = snapshot.create_chunk_reader(chunk);
    let table_reader = chunk_reader.get_table_reader("block").unwrap();
    let tmp_k = table_reader.read_column(0, None).unwrap();
    let keys = tmp_k.as_primitive::<UInt32Type>().values().to_vec();
    let tmp_v = table_reader.read_column(1, None).unwrap();
    let vals = tmp_v.as_primitive::<UInt32Type>().values().to_vec();
    keys.iter()
        .zip(vals)
        .map(|(a, b)| (*a, b))
        .collect::<Vec<_>>()
}

pub fn make_block(
    static_data: &Vec<Vec<u16>>,
    idx: usize,
    local_schema: Arc<Schema>,
    db: &Database,
) -> (Chunk, Vec<(u32, u32)>) {
    make_irregular_block(static_data, idx * 10, (idx + 1) * 10, local_schema, db)
}

pub fn make_irregular_block(
    static_data: &Vec<Vec<u16>>,
    start: usize,
    end: usize,
    local_schema: Arc<Schema>,
    db: &Database,
) -> (Chunk, Vec<(u32, u32)>) {
    let base_hash = format!("last_{}", start);
    let last_hash = format!("last_{}", end);

    let mut builder = db.new_table_builder(local_schema.clone());

    // let mut vec_1 = (0..10)
    //     .map(|v| (idx + 1) as u16 * 10 + v)
    //     .collect::<Vec<u16>>();
    // vec_1[0] = idx as u16;
    // let vec_2 = vec_1.iter().map(|v| 310 - v).collect::<Vec<u16>>();
    let vec_1 = static_data[0][start..end].to_vec();
    let vec_2 = static_data[1][start..end].to_vec();

    let type_1 = local_schema.fields()[0].data_type();
    let type_2 = local_schema.fields()[1].data_type();

    let array_1 = match type_1 {
        DataType::UInt32 => Arc::new(UInt32Array::from(
            vec_1
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<u32>>(),
        )) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(
            vec_1
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<i32>>(),
        )) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(vec_1.clone())) as ArrayRef,
        DataType::Float32 => Arc::new(Float32Array::from(
            vec_1
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<f32>>(),
        )) as ArrayRef,
        _ => todo!(),
    };
    let array_2 = match type_2 {
        DataType::UInt32 => Arc::new(UInt32Array::from(
            vec_2
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<u32>>(),
        )) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(
            vec_2
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<i32>>(),
        )) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(vec_2.clone())) as ArrayRef,
        DataType::Float32 => Arc::new(Float32Array::from(
            vec_2
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<f32>>(),
        )) as ArrayRef,
        _ => todo!(),
    };
    let batch = RecordBatch::try_new(local_schema, vec![array_1, array_2]).unwrap();
    assert!(builder.write_record_batch(&batch).is_ok());

    let mut tables = BTreeMap::new();
    tables.insert("block".to_owned(), builder.finish().unwrap());

    let chunk = Chunk::V0 {
        first_block: start as u64,
        last_block: (end - 1) as u64,
        last_block_hash: last_hash,
        parent_block_hash: base_hash,
        tables,
    };
    let data = vec_1
        .iter()
        .zip(vec_2)
        .map(|(a, b)| (*a as u32, b as u32))
        .collect();
    (chunk, data)
}

pub fn make_schema(type_1: DataType, type_2: DataType, is_sorted: bool) -> Arc<Schema> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("data", type_1, true),
        Field::new("atad", type_2, false),
    ]));
    match is_sorted {
        true => set_sort_key(schema, &[0]),
        false => schema,
    }
}

pub fn chunkify_data(data: Vec<(u32, u32)>, do_sort: bool) -> Vec<Vec<(u32, u32)>> {
    data.chunks(MIN_CHUNK_SIZE as usize)
        .map(|sl| {
            let mut vec = sl.to_vec();
            if do_sort {
                vec.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
            }
            vec
        })
        .collect()
}