use std::sync::Arc;
use rand::seq::SliceRandom;

mod utils;
use arrow::datatypes::DataType;
use rand::rng;
use sqd_storage::db::{ops::CompactionStatus, Chunk};
use utils::{read_chunk, setup_db, validate_chunks, make_block, make_schema, chunkify_data};
use sqd_storage::db::ops::schema_merge::can_merge_schemas;

use proptest::prelude::{prop, ProptestConfig};
use proptest::proptest;


#[test]
fn small_chunks_test() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V0 {
        first_block: 0,
        last_block: 0,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    let chunk2 = Chunk::V0 {
        first_block: 1,
        last_block: 1,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default(),
    };
    let chunk3 = Chunk::V0 {
        first_block: 2,
        last_block: 2,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default(),
    };
    let chunk4 = Chunk::V0 {
        first_block: 3,
        last_block: 3,
        last_block_hash: "last_4".to_owned(),
        parent_block_hash: "last_3".to_owned(),
        tables: Default::default(),
    };

    assert!(db.insert_chunk(dataset_id, &chunk4).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());

    validate_chunks(
        &db,
        dataset_id,
        [&chunk1, &chunk2, &chunk3, &chunk4].to_vec(),
    );
    assert!(db.perform_dataset_compaction(dataset_id).is_ok());
    let compacted = Chunk::V0 {
        first_block: 0,
        last_block: 3,
        last_block_hash: "last_4".to_owned(),
        parent_block_hash: "base".to_owned(),
        tables: Default::default(),
    };
    validate_chunks(&db, dataset_id, [&compacted].to_vec());
}

#[test]
fn compaction_wo_tables_test() {
    let (db, dataset_id) = setup_db();
    let mut chunks = Vec::default();

    for i in 0..150 {
        let base_hash = format!("last_{}", i);
        let last_hash = format!("last_{}", i + 1);
        let chunk = Chunk::V0 {
            first_block: i,
            last_block: i,
            last_block_hash: last_hash,
            parent_block_hash: base_hash,
            tables: Default::default(),
        };
        assert!(db.insert_chunk(dataset_id, &chunk).is_ok());
        chunks.push(chunk);
    }
    validate_chunks(&db, dataset_id, chunks.iter().collect());
    assert!(db.perform_dataset_compaction(dataset_id).is_ok());
    assert!(db.perform_dataset_compaction(dataset_id).is_ok());

    let chungus1 = Chunk::V0 {
        first_block: 0,
        last_block: 99,
        last_block_hash: "last_100".to_owned(),
        parent_block_hash: "last_0".to_owned(),
        tables: Default::default(),
    };
    let chungus2 = Chunk::V0 {
        first_block: 100,
        last_block: 149,
        last_block_hash: "last_150".to_owned(),
        parent_block_hash: "last_100".to_owned(),
        tables: Default::default(),
    };
    validate_chunks(&db, dataset_id, [&chungus1, &chungus2].to_vec());
}


fn universal_compaction_test(static_data: &Vec<Vec<u16>>, type_a: DataType, type_b: DataType, do_sort: bool, n_blocks: usize, n_compactions: usize) {
    let (db, dataset_id) = setup_db();
    let mut chunks = Vec::default();
    let schema_a = make_schema(type_a.clone(), type_b.clone(), do_sort);
    let schema_b = make_schema(type_b, type_a, do_sort);
    assert!(can_merge_schemas(&schema_a, &schema_b));
    let mut global_data = vec![(0u32, 0u32); 0];

    for i in 0..n_blocks {
        let local_schema = if i % 2 == 0 {
            Arc::clone(&schema_a)
        } else {
            Arc::clone(&schema_b)
        };
        let (chunk, data) = make_block(static_data, i, local_schema, &db);
        assert!(db.insert_chunk(dataset_id, &chunk).is_ok());
        chunks.push(chunk);
        global_data.extend(data);
    }
    let chunk_data = chunkify_data(global_data, do_sort);
    validate_chunks(&db, dataset_id, chunks.iter().collect());
    for _ in 0..n_compactions {
        assert!(matches!(db.perform_dataset_compaction(dataset_id), Ok(CompactionStatus::Ok)));
    }
    assert!(matches!(db.perform_dataset_compaction(dataset_id), Ok(CompactionStatus::NotingToCompact)));

    let snapshot = db.snapshot();
    let chunks = snapshot.list_chunks(dataset_id, 0, None);
    for (chunk, gt) in chunks.zip(chunk_data) {
        let data = read_chunk(&snapshot, chunk.unwrap());
        assert_eq!(data, gt);
    }
}

#[test]
fn compaction_with_tables_test() {
    let data_strategy = prop::collection::vec(prop::collection::vec(0..100u16, 500), 2);
    proptest!(ProptestConfig::with_cases(10), |(mut data in data_strategy)| {
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt32, false, 15, 2);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt32, false, 30, 3);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt32, false, 50, 5);

        data[0].iter_mut().fold(0u16, |acc, x| {
            *x += acc + 1;
            *x
        });
        data[0].shuffle(&mut rng());

        universal_compaction_test(&data, DataType::UInt32, DataType::UInt32, true, 15, 2);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt32, true, 30, 3);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt32, false, 50, 5);
    });
}

#[test]
fn compaction_fuzzy_tables_test() {
    let data_strategy = prop::collection::vec(prop::collection::vec(0..100u16, 500), 2);
    proptest!(ProptestConfig::with_cases(10), |(mut data in data_strategy)| {
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt16, false, 15, 2);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt16, false, 30, 3);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt16, false, 50, 5);

        data[0].iter_mut().fold(0u16, |acc, x| {
            *x += acc + 1;
            *x
        });
        data[0].shuffle(&mut rng());

        universal_compaction_test(&data, DataType::UInt32, DataType::UInt16, true, 15, 2);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt16, true, 30, 3);
        universal_compaction_test(&data, DataType::UInt32, DataType::UInt16, false, 50, 5);
    });
}
