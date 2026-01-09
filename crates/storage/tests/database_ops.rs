use core::{assert, assert_eq};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use sqd_primitives::sid::SID;
use sqd_primitives::BlockRef;
use sqd_storage::db::{Chunk, DatabaseSettings, DatasetId, DatasetKind};
use sqd_storage::table::write::use_small_buffers;

mod utils;
use utils::{setup_db, validate_chunks};

#[test]
fn create_dataset() {
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

    let res = db.create_dataset(dataset_id, dataset_kind).is_ok();
    assert!(res);
    let res = db.create_dataset(dataset_id, dataset_kind).is_err();
    assert!(res);
    let res = db
        .create_dataset_if_not_exists(dataset_id, dataset_kind)
        .is_ok();
    assert!(res);
    let datasets = db.get_all_datasets().unwrap();
    assert_eq!(datasets.len(), 1);
    let stats = db.get_statistics();
    assert!(stats.is_some());
}

#[test]
fn basic_chunks_test() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 201,
        last_block: 300,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());

    validate_chunks(&db, dataset_id, [&chunk1, &chunk2, &chunk3].to_vec());
}

#[test]
fn basic_chunks_test_rev() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 201,
        last_block: 300,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());

    validate_chunks(&db, dataset_id, [&chunk1, &chunk2, &chunk3].to_vec());
}

#[test]
fn basic_chunks_bad_hash() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "BAD".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 201,
        last_block: 300,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_err());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());

    validate_chunks(&db, dataset_id, [&chunk1, &chunk3].to_vec());
}

#[test]
fn basic_chunks_bad_range() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 99,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 201,
        last_block: 300,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_err());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());

    validate_chunks(&db, dataset_id, [&chunk1, &chunk3].to_vec());
}

#[test]
fn basic_fork_test() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 201,
        last_block: 300,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };
    let fork = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "fork_hash".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());
    assert!(db.insert_fork(dataset_id, &fork).is_ok());

    validate_chunks(&db, dataset_id, [&chunk1, &fork].to_vec());
}

#[test]
fn delete_chunks() {
    let (db, dataset_id) = setup_db();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "data",
        DataType::UInt32,
        true,
    )]));

    let mut builder = db.new_table_builder(schema.clone());

    let array = Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    assert!(builder.write_record_batch(&batch).is_ok());

    let mut tables = BTreeMap::new();
    tables.insert("block".to_owned(), builder.finish().unwrap());

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: tables.clone(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables,
    };
    // let chunk3 = Chunk::V0 { first_block: 201, last_block: 300, last_block_hash: "last_3".to_owned(), parent_block_hash: "last_2".to_owned(), tables: Default::default() };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    validate_chunks(&db, dataset_id, [&chunk1, &chunk2].to_vec());

    assert!(db
        .update_dataset(dataset_id, |tx| { tx.delete_chunk(&chunk2) })
        .is_ok());

    validate_chunks(&db, dataset_id, [&chunk1].to_vec());

    assert!(db
        .update_dataset(dataset_id, |tx| { tx.delete_chunk(&chunk1) })
        .is_ok());

    validate_chunks(&db, dataset_id, [].to_vec());

    assert!(db.cleanup().is_ok());
}

#[test]
fn chunk_reader() {
    let (db, dataset_id) = setup_db();

    let schema = Arc::new(Schema::new(vec![
        Field::new("data", DataType::UInt32, true),
        Field::new("atad", DataType::UInt32, true),
    ]));

    let mut builder = db.new_table_builder(schema.clone());

    let array1 = Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5]));
    let array2 = Arc::new(UInt32Array::from(vec![5, 4, 3, 2, 1]));
    let batch = RecordBatch::try_new(schema, vec![array1, array2]).unwrap();
    assert!(builder.write_record_batch(&batch).is_ok());

    let mut tables = BTreeMap::new();
    tables.insert("block".to_owned(), builder.finish().unwrap());

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: tables.clone(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables,
    };
    // let chunk3 = Chunk::V0 { first_block: 201, last_block: 300, last_block_hash: "last_3".to_owned(), parent_block_hash: "last_2".to_owned(), tables: Default::default() };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    validate_chunks(&db, dataset_id, [&chunk1, &chunk2].to_vec());

    let snapshot = db.snapshot();
    let chunk_reader = snapshot.create_chunk_reader(chunk1);
    assert_eq!(chunk_reader.first_block(), 0);
    assert_eq!(chunk_reader.last_block(), 100);
    assert_eq!(chunk_reader.last_block_hash(), "last_1".to_owned());
    assert_eq!(chunk_reader.base_block_hash(), "base".to_owned());
    assert!(chunk_reader.has_table("block"));
    assert_eq!(chunk_reader.has_table("tx"), false);

    let table_reader = chunk_reader.get_table_reader("block").unwrap();
    let mut proj = HashSet::new();
    proj.insert("atad");
    let batch = table_reader.read_table(Some(&proj), None).unwrap();
    assert_eq!(**batch.column(0), UInt32Array::from(vec![5, 4, 3, 2, 1]));
    // let column_reader = table_reader.create_column_reader(0).unwrap();
    // let typed_reader = column_reader.as_primitive();

    // println!("wtf {:?}", batch);
    // let cursor = table_reader.new_cursor();
    // for v in typed_reader {
    //     println!("V:");
    // }

    // assert!(db.update_dataset(dataset_id, |tx| {
    //     tx.delete_chunk(&chunk2)
    // }).is_ok());

    // validate_chunks(&db, dataset_id, [&chunk1].to_vec());

    // assert!(db.update_dataset(dataset_id, |tx| {
    //     tx.delete_chunk(&chunk1)
    // }).is_ok());

    // validate_chunks(&db, dataset_id, [].to_vec());

    // assert!(db.cleanup().is_ok());
}

#[test]
fn labels() {
    let (db, dataset_id) = setup_db();
    let snapshot = db.snapshot();
    let label = snapshot.get_label(dataset_id).unwrap().unwrap();
    assert_eq!(label.kind(), SID::from_str("solana"));
    assert_eq!(label.version(), 0);
    assert_eq!(label.finalized_head(), None);

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "BAD".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 201,
        last_block: 300,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_err());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());

    let finalized_head = BlockRef {
        number: 300,
        hash: "last_3".to_owned(),
    };

    assert!(db
        .update_dataset(dataset_id, |tx| {
            tx.set_finalized_head(Some(finalized_head.clone()));
            Ok(())
        })
        .is_ok());

    let snapshot = db.snapshot();
    let label = snapshot.get_label(dataset_id).unwrap().unwrap();
    assert_eq!(label.kind(), SID::from_str("solana"));
    assert_eq!(label.version(), 3); // 2 succesfull updates, 1 set finalized head
    assert_eq!(label.finalized_head(), Some(&finalized_head));
}

#[test]
fn delete_dataset() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 100,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 101,
        last_block: 200,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());

    let datasets = db.get_all_datasets().unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].id, dataset_id);
    validate_chunks(&db, dataset_id, [&chunk1, &chunk2].to_vec());

    assert!(db.delete_dataset(dataset_id).is_ok());

    let datasets = db.get_all_datasets().unwrap();
    assert_eq!(datasets.len(), 0);

    validate_chunks(&db, dataset_id, [].to_vec());
}
