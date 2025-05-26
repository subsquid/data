use arrow::datatypes::{DataType, Schema};
use proptest::prelude::{prop, ProptestConfig};
use proptest::proptest;
use rand::rng;
use rand::seq::SliceRandom;
use sqd_storage::db::ops::schema_merge::can_merge_schemas;
use sqd_storage::db::{ops::CompactionStatus, Chunk, Database, DatasetId};
use sqd_storage::table::write::use_small_buffers;
use std::sync::Arc;
use utils::{
    chunkify_data, make_block, make_irregular_block, make_schema, read_chunk, setup_db,
    validate_chunks,
};


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
    let chunk4 = Chunk::V1 {
        first_block: 3,
        last_block: 3,
        last_block_hash: "last_4".to_owned(),
        parent_block_hash: "last_3".to_owned(),
        first_block_time: None,
        last_block_time: Some(10),
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

    compact(&db, dataset_id).unwrap();

    let compacted = Chunk::V1 {
        first_block: 0,
        last_block: 3,
        last_block_hash: "last_4".to_owned(),
        parent_block_hash: "base".to_owned(),
        first_block_time: Some(5),
        last_block_time: Some(10),
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

    while matches!(
        db.perform_dataset_compaction(dataset_id, Some(100), Some(1.25), None),
        Ok(CompactionStatus::Ok(_))
    ) {}

    let chungus1 = Chunk::V1 {
        first_block: 0,
        last_block: 99,
        last_block_hash: "last_100".to_owned(),
        parent_block_hash: "last_0".to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables: Default::default(),
    };
    let chungus2 = Chunk::V1 {
        first_block: 100,
        last_block: 149,
        last_block_hash: "last_150".to_owned(),
        parent_block_hash: "last_100".to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables: Default::default(),
    };
    validate_chunks(&db, dataset_id, [&chungus1, &chungus2].to_vec());
}

fn universal_compaction_test(
    static_data: &Vec<Vec<u16>>,
    type_a: DataType,
    type_b: DataType,
    do_sort: bool,
    n_blocks: usize,
    n_compactions: usize,
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
        assert!(matches!(
            compact(&db, dataset_id).unwrap(),
            CompactionStatus::Ok(_)
        ));
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

#[test]
fn compaction_plan_test() {
    let data_strategy = prop::collection::vec(prop::collection::vec(1..20usize, 10..40), 1..5);
    proptest!(ProptestConfig::with_cases(200), |(block_sizes in data_strategy)| {
        compaction_plan_test_execution(&block_sizes, 100, 1.25, false);
        compaction_plan_test_execution(&block_sizes, 150, 1.25, false);
        compaction_plan_test_execution(&block_sizes, 100, 1.50, false);
        compaction_plan_test_execution(&block_sizes, 100, 1.25, true);
        compaction_plan_test_execution(&block_sizes, 150, 1.25, true);
        compaction_plan_test_execution(&block_sizes, 100, 1.50, true);
    });
}

fn compaction_plan_test_execution(
    block_sizes: &Vec<Vec<usize>>,
    max_chunk_size: usize,
    wa_limit: f64,
    compact_on_each_insert: bool
) {
    let type_a = DataType::UInt32;
    let type_b = DataType::Int32;
    let do_sort = false;

    let total_blocks = block_sizes
        .iter()
        .map(|v| v.iter().sum::<usize>())
        .sum::<usize>();
    let static_data = vec![
        (0..total_blocks as u16).collect::<Vec<u16>>(),
        (0..total_blocks as u16).collect::<Vec<u16>>(),
    ];

    let (db, dataset_id) = setup_db();
    let mut chunks = Vec::default();
    let schema_a = make_schema(type_a.clone(), type_b.clone(), do_sort);
    let schema_b = make_schema(type_b, type_a, do_sort);
    let mut global_data = vec![(0u32, 0u32); 0];

    let mut total_offset = 0;

    for (i, sizes) in block_sizes.iter().enumerate() {
        for size in sizes {
            let local_schema = if i % 2 == 0 {
                Arc::clone(&schema_a)
            } else {
                Arc::clone(&schema_b)
            };
            let (chunk, data) = make_irregular_block(
                &static_data,
                total_offset,
                total_offset + *size,
                local_schema,
                &db,
            );
            assert!(db.insert_chunk(dataset_id, &chunk).is_ok());
            if compact_on_each_insert {
                db.perform_dataset_compaction(dataset_id, Some(max_chunk_size), Some(wa_limit), None).unwrap();
            }
            chunks.push(chunk);
            global_data.extend(data);
            total_offset += *size;
        }
    }

    if !compact_on_each_insert {
        validate_chunks(&db, dataset_id, chunks.iter().collect());
    }

    while matches!(
        db.perform_dataset_compaction(dataset_id, Some(max_chunk_size), Some(wa_limit), None),
        Ok(CompactionStatus::Ok(_))
    ) {}

    let snapshot = db.snapshot();
    let mut chunks = snapshot.list_chunks(dataset_id, 0, None);
    let mut last_schema_option: Option<Arc<Schema>> = None;
    let mut chunk_data_sizes: Vec<Vec<usize>> = Default::default();
    chunk_data_sizes.push(Default::default());
    while let Some(el) = chunks.next().transpose().unwrap() {
        let tables = el.tables();
        assert_eq!(tables.len(), 1);
        let mut schema_compatible = true;
        let (_, table_id) = tables.first_key_value().unwrap();
        let reader = snapshot.create_table_reader(*table_id).unwrap();
        let total_rows = reader.num_rows();
        let this_schema = reader.schema();
        match last_schema_option {
            Some(last_schema) => {
                schema_compatible = can_merge_schemas(&this_schema, &last_schema);
            }
            None => {}
        }
        last_schema_option = Some(this_schema);

        if schema_compatible {
            chunk_data_sizes.last_mut().unwrap().push(total_rows);
        } else {
            chunk_data_sizes.push(vec![total_rows; 1]);
        }
    }

    for (run_idx, run) in chunk_data_sizes.iter().enumerate() {
        if run_idx + 1 < chunk_data_sizes.len() {
            // all chunks (except last) in non-final run should be not sorter than MAX_MERGEABLE_CHUNK_SIZE, last chunk may be whatever
            for (chunk_idx, &chunk_size) in run.iter().enumerate() {
                if chunk_idx + 1 < run.len() {
                    assert!(chunk_size >= max_chunk_size);
                }
            }
        } else {
            // in the last run, chunks should be split in two continous groups:
            // - chunks in the first group all should be not shorter than MAX_MERGEABLE_CHUNK_SIZE
            // - chunks in the second (maybe empty) group are all shorter than MAX_MERGEABLE_CHUNK_SIZE and longest of them should dominate (break wa formula)
            let mut iter = run.iter().peekable();
            while let Some(&&chunk_size) = iter.peek() {
                if chunk_size < max_chunk_size {
                    break;
                }
                iter.next();
            }
            let longest_chunk_option = iter.clone().max();
            match longest_chunk_option {
                Some(&longest_chunk) => {
                    let total_len = iter.sum::<usize>();
                    assert!(wa_limit * longest_chunk as f64 >= total_len as f64);
                }
                None => {}
            }
        }
    }
}
