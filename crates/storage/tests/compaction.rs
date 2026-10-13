use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array, RecordBatch, UInt32Array},
    datatypes::DataType
};
use proptest::{
    prelude::{prop, ProptestConfig},
    proptest
};
use rand::{rng, seq::SliceRandom};
use sqd_storage::{
    db::{
        ops::{schema_merge::can_merge_schemas, CompactionStatus, COMPACTION_LEN_LIMIT, MERGE_HORIZON},
        Chunk, Database, DatasetId
    },
    table::write::use_small_buffers
};
use utils::{chunkify_data, make_block, make_irregular_block, make_schema, read_chunk, setup_db, validate_chunks};

mod utils;

fn compact(db: &Database, dataset_id: DatasetId) -> anyhow::Result<CompactionStatus> {
    db.perform_dataset_compaction(dataset_id, Some(100), Some(1.25), None)
}

#[test]
fn small_chunks_test() {
    let (db, dataset_id) = setup_db();

    let chunk1 = Chunk::V1 {
        first_block: 0,
        last_block: 0,
        last_block_hash: "last_1".to_owned(),
        parent_block_hash: "base".to_owned(),
        first_block_time: Some(5),
        last_block_time: None,
        tables: Default::default()
    };
    let chunk2 = Chunk::V0 {
        first_block: 1,
        last_block: 1,
        last_block_hash: "last_2".to_owned(),
        parent_block_hash: "last_1".to_owned(),
        tables: Default::default()
    };
    let chunk3 = Chunk::V0 {
        first_block: 2,
        last_block: 2,
        last_block_hash: "last_3".to_owned(),
        parent_block_hash: "last_2".to_owned(),
        tables: Default::default()
    };
    let chunk4 = Chunk::V1 {
        first_block: 3,
        last_block: 3,
        last_block_hash: "last_4".to_owned(),
        parent_block_hash: "last_3".to_owned(),
        first_block_time: None,
        last_block_time: Some(10),
        tables: Default::default()
    };

    assert!(db.insert_chunk(dataset_id, &chunk4).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk3).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk2).is_ok());
    assert!(db.insert_chunk(dataset_id, &chunk1).is_ok());

    validate_chunks(&db, dataset_id, [&chunk1, &chunk2, &chunk3, &chunk4].to_vec());

    compact(&db, dataset_id).unwrap();

    let compacted = Chunk::V1 {
        first_block: 0,
        last_block: 3,
        last_block_hash: "last_4".to_owned(),
        parent_block_hash: "base".to_owned(),
        first_block_time: Some(5),
        last_block_time: Some(10),
        tables: Default::default()
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
            tables: Default::default()
        };
        assert!(db.insert_chunk(dataset_id, &chunk).is_ok());
        chunks.push(chunk);
    }
    validate_chunks(&db, dataset_id, chunks.iter().collect());

    while matches!(
        db.perform_dataset_compaction(dataset_id, Some(100), Some(1.25), Some(10)),
        Ok(CompactionStatus::Ok(_))
    ) {}

    let chungus1 = Chunk::V1 {
        first_block: 0,
        last_block: 99,
        last_block_hash: "last_100".to_owned(),
        parent_block_hash: "last_0".to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables: Default::default()
    };
    let chungus2 = Chunk::V1 {
        first_block: 100,
        last_block: 149,
        last_block_hash: "last_150".to_owned(),
        parent_block_hash: "last_100".to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables: Default::default()
    };
    validate_chunks(&db, dataset_id, [&chungus1, &chungus2].to_vec());
}

fn universal_compaction_test(
    static_data: &Vec<Vec<u16>>,
    type_a: DataType,
    type_b: DataType,
    do_sort: bool,
    n_blocks: usize,
    n_compactions: usize
) {
    let (db, dataset_id) = setup_db();
    let _sb = use_small_buffers();
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
        assert!(matches!(compact(&db, dataset_id).unwrap(), CompactionStatus::Ok(_)));
    }
    assert!(matches!(
        compact(&db, dataset_id).unwrap(),
        CompactionStatus::NotingToCompact
    ));

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

/// Runs one compaction pass and returns the first and last block of the merged chunks.
fn compaction_step(db: &Database, dataset_id: DatasetId, wa: f64) -> Option<(u64, u64)> {
    let status = db
        .perform_dataset_compaction(dataset_id, Some(100), Some(wa), None)
        .unwrap();

    match status {
        CompactionStatus::Ok(merged) => {
            let first_block = merged.first().unwrap().first_block;
            let last_block = merged.last().unwrap().last_block;
            Some((first_block, last_block))
        }
        CompactionStatus::NotingToCompact => None,
        CompactionStatus::Canceled => panic!("compaction canceled without concurrent writes")
    }
}

fn compact_to_completion(db: &Database, dataset_id: DatasetId, wa: f64) -> Vec<(u64, u64)> {
    let mut merges = Vec::new();
    while let Some(merge) = compaction_step(db, dataset_id, wa) {
        merges.push(merge);
        assert!(merges.len() <= 100, "compaction did not converge");
    }
    merges
}

#[test]
fn compaction_finishes_closed_schemas_and_preserves_rows() {
    for compact_on_each_insert in [false, true] {
        let (db, dataset_id) = setup_db();
        let schemas = [
            make_schema(DataType::UInt32, DataType::Int32, false),
            make_schema(DataType::Int32, DataType::UInt32, false)
        ];
        assert!(!can_merge_schemas(&schemas[0], &schemas[1]));

        let data = vec![(0..38u16).collect::<Vec<_>>(); 2];
        let mut offset = 0;
        let mut steps = Vec::new();
        for (schema, sizes) in schemas.iter().zip([&[19, 8, 1][..], &[5, 5][..]]) {
            for &size in sizes {
                let (chunk, _) = make_irregular_block(&data, offset, offset + size, schema.clone(), &db);
                db.insert_chunk(dataset_id, &chunk).unwrap();
                offset += size;

                if compact_on_each_insert {
                    steps.push(compaction_step(&db, dataset_id, 1.50));
                }
            }
        }

        // [19, 8, 1] stays below the open tail's 1.5 merge ratio until the schema
        // change closes it.
        let remaining = compact_to_completion(&db, dataset_id, 1.50);
        if compact_on_each_insert {
            assert_eq!(steps, [None, None, None, Some((0, 27)), Some((28, 37))]);
            assert!(remaining.is_empty());
        } else {
            assert_eq!(remaining, [(0, 27), (28, 37)]);
        }

        let snapshot = db.snapshot();
        let chunks = snapshot
            .list_chunks(dataset_id, 0, None)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(chunks.len(), 2);

        for ((chunk, schema), (start, end)) in chunks.iter().zip(&schemas).zip([(0, 28), (28, 38)]) {
            assert_eq!(chunk.first_block(), start as u64);
            assert_eq!(chunk.last_block(), (end - 1) as u64);
            assert_eq!(chunk.parent_block_hash(), format!("last_{start}"));
            assert_eq!(chunk.last_block_hash(), format!("last_{end}"));
            assert_eq!(chunk.tables().len(), 1);

            let reader = snapshot
                .create_table_reader(*chunk.tables().get("block").unwrap())
                .unwrap();
            assert_eq!(reader.schema().as_ref(), schema.as_ref());

            let actual = RecordBatch::try_new(
                reader.schema(),
                vec![
                    reader.read_column(0, None).unwrap(),
                    reader.read_column(1, None).unwrap(),
                ]
            )
            .unwrap();
            let unsigned = Arc::new(UInt32Array::from((start..end).map(|v| v as u32).collect::<Vec<_>>())) as ArrayRef;
            let signed = Arc::new(Int32Array::from((start..end).collect::<Vec<_>>())) as ArrayRef;
            let columns = if start == 0 {
                vec![unsigned, signed]
            } else {
                vec![signed, unsigned]
            };
            let expected = RecordBatch::try_new(schema.clone(), columns).unwrap();
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn compaction_merges_runs_split_by_schema_switches_and_gaps() {
    let (db, dataset_id) = setup_db();
    let a = make_schema(DataType::UInt32, DataType::Int32, false);
    let b = make_schema(DataType::Int32, DataType::UInt32, false);
    let data = vec![(0..55u16).collect::<Vec<_>>(); 2];

    // A [0, 28), B [28, 38), A [38, 50), no block 50, A [51, 55).
    let chunks = [
        (&a, 0, 19),
        (&a, 19, 27),
        (&a, 27, 28),
        (&b, 28, 33),
        (&b, 33, 38),
        (&a, 38, 42),
        (&a, 42, 46),
        (&a, 46, 50),
        (&a, 51, 53),
        (&a, 53, 55)
    ];
    for (schema, start, end) in chunks {
        let (chunk, _) = make_irregular_block(&data, start, end, schema.clone(), &db);
        db.insert_chunk(dataset_id, &chunk).unwrap();
    }

    // Each pass skips the chunks that the passes before it merged.
    let merges = compact_to_completion(&db, dataset_id, 1.50);
    assert_eq!(merges, [(0, 27), (28, 37), (38, 49), (51, 54)]);
}

#[test]
fn compaction_plans_only_within_the_merge_horizon() {
    let (db, dataset_id) = setup_db();
    let n_chunks = MERGE_HORIZON as u64 + 10;
    for i in 0..n_chunks {
        let chunk = Chunk::V0 {
            first_block: i,
            last_block: i,
            last_block_hash: format!("last_{}", i + 1),
            parent_block_hash: format!("last_{i}"),
            tables: Default::default()
        };
        db.insert_chunk(dataset_id, &chunk).unwrap();
    }

    let first_block = n_chunks - MERGE_HORIZON as u64;
    let last_block = first_block + COMPACTION_LEN_LIMIT as u64 - 1;
    assert_eq!(compaction_step(&db, dataset_id, 1.25), Some((first_block, last_block)));
}

#[test]
fn compaction_keeps_chunks_with_different_tables_apart() {
    let (db, dataset_id) = setup_db();
    let schema = make_schema(DataType::UInt32, DataType::Int32, false);
    let data = vec![(0..4u16).collect::<Vec<_>>(); 2];

    let (with_tables, _) = make_irregular_block(&data, 0, 4, schema, &db);
    let without_tables = Chunk::V0 {
        first_block: 4,
        last_block: 7,
        last_block_hash: "last_8".to_owned(),
        parent_block_hash: "last_4".to_owned(),
        tables: Default::default()
    };
    db.insert_chunk(dataset_id, &with_tables).unwrap();
    db.insert_chunk(dataset_id, &without_tables).unwrap();

    assert_eq!(compaction_step(&db, dataset_id, 1.25), None);
}
