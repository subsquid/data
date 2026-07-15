use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc},
    thread,
    time::Duration
};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema}
};
use sqd_primitives::TransactionRef;
use sqd_storage::{
    db::{Chunk, CompactionStatus, Database, DatabaseSettings, DatasetId, DatasetKind, HashIndexWriteMetrics},
    table::write::use_small_buffers
};
use tempfile::TempDir;

#[derive(Clone, Debug)]
struct TransactionRow {
    block_number: u64,
    transaction_index: u32,
    hash: String
}

fn open_db_with(kind: &str, block_hash_index: bool, transaction_hash_index: bool) -> (TempDir, Database, DatasetId) {
    let db_dir = tempfile::tempdir().unwrap();
    let db = reopen(&db_dir, block_hash_index, transaction_hash_index);
    let dataset_id = DatasetId::from_str("test-dataset");
    db.create_dataset(dataset_id, DatasetKind::from_str(kind)).unwrap();
    (db_dir, db, dataset_id)
}

fn reopen(dir: &TempDir, block_hash_index: bool, transaction_hash_index: bool) -> Database {
    DatabaseSettings::default()
        .with_block_hash_index(block_hash_index)
        .with_transaction_hash_index(transaction_hash_index)
        .open(dir.path())
        .unwrap()
}

fn setup_evm_db() -> (TempDir, Database, DatasetId) {
    open_db_with("evm", false, true)
}

fn block_hash(n: u64) -> String {
    format!("0x{n:064x}")
}

fn transaction_hash(block_number: u64, transaction_index: u32, fork: u32) -> String {
    format!("0x{fork:08x}{block_number:048x}{transaction_index:08x}")
}

fn transaction_rows(first: u64, last: u64, per_block: u32, fork: u32) -> Vec<TransactionRow> {
    (first..=last)
        .flat_map(|block_number| {
            (0..per_block).map(move |transaction_index| TransactionRow {
                block_number,
                transaction_index,
                hash: transaction_hash(block_number, transaction_index, fork)
            })
        })
        .collect()
}

fn make_evm_chunk(db: &Database, first: u64, last: u64, parent_hash: &str, transactions: &[TransactionRow]) -> Chunk {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let block_numbers = Arc::new(UInt64Array::from(
        transactions.iter().map(|row| row.block_number).collect::<Vec<_>>()
    )) as ArrayRef;
    let transaction_indexes = Arc::new(UInt32Array::from(
        transactions.iter().map(|row| row.transaction_index).collect::<Vec<_>>()
    )) as ArrayRef;
    let hashes = Arc::new(StringArray::from(
        transactions.iter().map(|row| row.hash.as_str()).collect::<Vec<_>>()
    )) as ArrayRef;

    let mut builder = db.new_table_builder(schema.clone());
    let batch = RecordBatch::try_new(schema, vec![block_numbers, transaction_indexes, hashes]).unwrap();
    builder.write_record_batch(&batch).unwrap();

    let mut tables = BTreeMap::new();
    tables.insert("transactions".to_owned(), builder.finish().unwrap());

    Chunk::V1 {
        first_block: first,
        last_block: last,
        last_block_hash: block_hash(last),
        parent_block_hash: parent_hash.to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables
    }
}

fn make_evm_chunk_with_u64_transaction_index(db: &Database, transaction_index: u64) -> (Chunk, String) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let hash = transaction_hash(0, 0, 0);
    let block_numbers = Arc::new(UInt64Array::from(vec![0])) as ArrayRef;
    let transaction_indexes = Arc::new(UInt64Array::from(vec![transaction_index])) as ArrayRef;
    let hashes = Arc::new(StringArray::from(vec![hash.as_str()])) as ArrayRef;

    let mut builder = db.new_table_builder(schema.clone());
    let batch = RecordBatch::try_new(schema, vec![block_numbers, transaction_indexes, hashes]).unwrap();
    builder.write_record_batch(&batch).unwrap();

    let mut tables = BTreeMap::new();
    tables.insert("transactions".to_owned(), builder.finish().unwrap());

    (
        Chunk::V1 {
            first_block: 0,
            last_block: 0,
            last_block_hash: block_hash(0),
            parent_block_hash: "base".to_owned(),
            first_block_time: None,
            last_block_time: None,
            tables
        },
        hash
    )
}

fn lookup(db: &Database, dataset_id: DatasetId, hash: &str) -> Option<TransactionRef> {
    db.snapshot().find_transaction_by_hash(dataset_id, hash).unwrap()
}

fn assert_resolves(db: &Database, dataset_id: DatasetId, row: &TransactionRow) {
    assert_eq!(
        lookup(db, dataset_id, &row.hash),
        Some(TransactionRef {
            block_number: row.block_number,
            transaction_index: row.transaction_index,
            hash: row.hash.clone()
        }),
        "transaction {} should resolve to ({}, {})",
        row.hash,
        row.block_number,
        row.transaction_index
    );
}

fn assert_absent(db: &Database, dataset_id: DatasetId, hash: &str) {
    assert_eq!(lookup(db, dataset_id, hash), None, "hash {hash} should not resolve");
}

#[test]
fn index_ingest_and_lookup() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let rows = transaction_rows(0, 9, 3, 0);
    let chunk = make_evm_chunk(&db, 0, 9, "base", &rows);

    db.insert_chunk(dataset_id, &chunk).unwrap();

    for row in &rows {
        assert_resolves(&db, dataset_id, row);
    }
    assert_absent(&db, dataset_id, "0xdeadbeef");
}

#[test]
fn observed_update_reports_transaction_hash_index_work() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let rows = transaction_rows(0, 9, 3, 0);
    let chunk = make_evm_chunk(&db, 0, 9, "base", &rows);
    let mut metrics = HashIndexWriteMetrics::default();

    db.update_dataset_with_hash_index_metrics(dataset_id, &mut metrics, |tx| tx.insert_chunk(&chunk))
        .unwrap();

    assert_eq!(metrics.block_hash_index_operations(), 0);
    assert_eq!(metrics.transaction_hash_index_operations(), 1);
    assert!(metrics.block_hash_index_duration().is_none());
    assert!(metrics.transaction_hash_index_duration().is_some());
    for row in &rows {
        assert_resolves(&db, dataset_id, row);
    }
}

#[test]
fn transaction_index_rejects_out_of_range_position_without_publishing_the_chunk() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let oversized_index = u64::from(u32::MAX) + 1;
    let (chunk, hash) = make_evm_chunk_with_u64_transaction_index(&db, oversized_index);

    let err = db.insert_chunk(dataset_id, &chunk).unwrap_err();

    assert!(
        format!("{err:#}").contains(&format!("transaction_index {oversized_index} does not fit u32")),
        "unexpected error: {err:#}"
    );
    assert!(db.snapshot().get_last_chunk(dataset_id).unwrap().is_none());
    assert_absent(&db, dataset_id, &hash);
}

#[test]
fn index_large_chunk_spans_multiple_read_batches() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let rows = transaction_rows(0, 4, 1_025, 0);
    assert!(rows.len() > 4096);
    let chunk = make_evm_chunk(&db, 0, 4, "base", &rows);

    db.insert_chunk(dataset_id, &chunk).unwrap();

    for index in [0, 1, 4095, 4096, 4097, rows.len() - 1] {
        assert_resolves(&db, dataset_id, &rows[index]);
    }
}

#[test]
fn fork_removes_stale_hashes_and_reinclusion_names_the_new_position() {
    let (_dir, db, dataset_id) = setup_evm_db();

    let rows1 = transaction_rows(0, 9, 2, 0);
    let mut rows2 = transaction_rows(10, 19, 2, 0);
    let reincluded_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    rows2[4].hash = reincluded_hash.to_owned(); // old position: block 12, index 0
    let chunk1 = make_evm_chunk(&db, 0, 9, "base", &rows1);
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9), &rows2);
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();

    let mut fork_rows = transaction_rows(10, 19, 2, 1);
    fork_rows[11].hash = reincluded_hash.to_owned(); // new position: block 15, index 1
    let fork = make_evm_chunk(&db, 10, 19, &block_hash(9), &fork_rows);
    db.insert_fork(dataset_id, &fork).unwrap();

    for row in &rows1 {
        assert_resolves(&db, dataset_id, row);
    }
    for row in &rows2 {
        if row.hash != reincluded_hash {
            assert_absent(&db, dataset_id, &row.hash);
        }
    }
    for row in &fork_rows {
        assert_resolves(&db, dataset_id, row);
    }
    assert_eq!(
        lookup(&db, dataset_id, reincluded_hash),
        Some(TransactionRef {
            block_number: 15,
            transaction_index: 1,
            hash: reincluded_hash.to_owned()
        })
    );
}

#[test]
fn retention_removes_only_the_pruned_chunks_transactions() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let rows1 = transaction_rows(0, 9, 2, 0);
    let rows2 = transaction_rows(10, 19, 2, 0);
    let chunk1 = make_evm_chunk(&db, 0, 9, "base", &rows1);
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9), &rows2);
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();

    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk1)).unwrap();

    for row in &rows1 {
        assert_absent(&db, dataset_id, &row.hash);
    }
    for row in &rows2 {
        assert_resolves(&db, dataset_id, row);
    }
}

#[test]
fn delete_dataset_purges_all_transaction_hashes() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let rows = transaction_rows(0, 19, 2, 0);
    let chunk = make_evm_chunk(&db, 0, 19, "base", &rows);
    db.insert_chunk(dataset_id, &chunk).unwrap();

    db.delete_dataset(dataset_id).unwrap();

    assert!(db.get_all_datasets().unwrap().is_empty());
    for row in &rows {
        assert_absent(&db, dataset_id, &row.hash);
    }
}

#[test]
fn compaction_preserves_the_transaction_index() {
    let (_dir, db, dataset_id) = setup_evm_db();
    let _small_buffers = use_small_buffers();
    let mut all_rows = Vec::new();
    let mut parent = "base".to_owned();

    for block in 0..60 {
        let rows = transaction_rows(block, block, 2, 0);
        let chunk = make_evm_chunk(&db, block, block, &parent, &rows);
        db.insert_chunk(dataset_id, &chunk).unwrap();
        all_rows.extend(rows);
        parent = block_hash(block);
    }

    let mut merged = false;
    while let CompactionStatus::Ok(_) = db
        .perform_dataset_compaction(dataset_id, Some(100), Some(1.25), None)
        .unwrap()
    {
        merged = true;
    }
    assert!(merged, "expected compaction to merge at least one chunk range");

    for row in &all_rows {
        assert_resolves(&db, dataset_id, row);
    }
}

#[test]
fn transaction_index_is_evm_only_and_independent_from_block_index() {
    let (_dir, db, dataset_id) = open_db_with("solana", true, true);
    let rows = transaction_rows(0, 1, 2, 0);
    let chunk = make_evm_chunk(&db, 0, 1, "base", &rows);
    db.insert_chunk(dataset_id, &chunk).unwrap();
    for row in &rows {
        assert_absent(&db, dataset_id, &row.hash);
    }

    let (_dir, db, dataset_id) = open_db_with("evm", true, false);
    let rows = transaction_rows(0, 1, 2, 0);
    let chunk = make_evm_chunk(&db, 0, 1, "base", &rows);
    db.insert_chunk(dataset_id, &chunk).unwrap();
    for row in &rows {
        assert_absent(&db, dataset_id, &row.hash);
    }
}

#[test]
fn enabling_the_index_does_not_backfill_existing_chunks() {
    let (dir, db, dataset_id) = open_db_with("evm", false, false);
    let old_rows = transaction_rows(0, 9, 2, 0);
    let old_chunk = make_evm_chunk(&db, 0, 9, "base", &old_rows);
    db.insert_chunk(dataset_id, &old_chunk).unwrap();
    drop(db);

    let db = reopen(&dir, false, true);
    let new_rows = transaction_rows(10, 19, 2, 0);
    let new_chunk = make_evm_chunk(&db, 10, 19, &block_hash(9), &new_rows);
    db.insert_chunk(dataset_id, &new_chunk).unwrap();

    for row in &old_rows {
        assert_absent(&db, dataset_id, &row.hash);
    }
    for row in &new_rows {
        assert_resolves(&db, dataset_id, row);
    }
}

#[test]
fn entries_drain_after_the_flag_is_turned_off() {
    let (dir, db, dataset_id) = setup_evm_db();
    let rows1 = transaction_rows(0, 9, 2, 0);
    let rows2 = transaction_rows(10, 19, 2, 0);
    let chunk1 = make_evm_chunk(&db, 0, 9, "base", &rows1);
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9), &rows2);
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();
    drop(db);

    let db = reopen(&dir, false, false);
    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk1)).unwrap();

    for row in &rows1 {
        assert_absent(&db, dataset_id, &row.hash);
    }
    for row in &rows2 {
        assert_resolves(&db, dataset_id, row);
    }
}

#[test]
fn pending_large_index_write_does_not_block_reads_or_another_dataset_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DatabaseSettings::default()
            .with_transaction_hash_index(true)
            .open(dir.path())
            .unwrap()
    );
    let dataset_a = DatasetId::from_str("dataset-a");
    let dataset_b = DatasetId::from_str("dataset-b");
    for dataset_id in [dataset_a, dataset_b] {
        db.create_dataset(dataset_id, DatasetKind::from_str("evm")).unwrap();
    }

    let seed_rows = transaction_rows(0, 0, 1, 0);
    let seed = make_evm_chunk(&db, 0, 0, "base", &seed_rows);
    db.insert_chunk(dataset_a, &seed).unwrap();

    let staged_rows = transaction_rows(1, 1, 10_000, 0);
    let staged_hash = staged_rows.last().unwrap().hash.clone();
    let staged = make_evm_chunk(&db, 1, 1, &block_hash(0), &staged_rows);
    let other_rows = transaction_rows(0, 0, 1, 0);
    let other = make_evm_chunk(&db, 0, 0, "base", &other_rows);

    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let writer_db = Arc::clone(&db);
    let writer = thread::spawn(move || {
        writer_db.update_dataset(dataset_a, |tx| {
            tx.insert_chunk(&staged)?;
            ready_tx.send(())?;
            release_rx.recv_timeout(Duration::from_secs(10))?;
            Ok(())
        })
    });
    ready_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("large index transaction should reach the pre-commit gate");

    let (probe_tx, probe_rx) = mpsc::sync_channel(0);
    let probe_db = Arc::clone(&db);
    let committed_hash = seed_rows[0].hash.clone();
    let other_hash = other_rows[0].hash.clone();
    let probe = thread::spawn(move || {
        let result = (|| -> anyhow::Result<()> {
            anyhow::ensure!(
                probe_db
                    .snapshot()
                    .find_transaction_by_hash(dataset_a, &committed_hash)?
                    .is_some(),
                "a committed point read disappeared while another transaction was pending"
            );
            anyhow::ensure!(
                probe_db
                    .snapshot()
                    .find_transaction_by_hash(dataset_a, &staged_hash)?
                    .is_none(),
                "an uncommitted index entry became visible"
            );
            probe_db.insert_chunk(dataset_b, &other)?;
            anyhow::ensure!(
                probe_db
                    .snapshot()
                    .find_transaction_by_hash(dataset_b, &other_hash)?
                    .is_some(),
                "the independent writer did not publish its index entry"
            );
            Ok(())
        })();
        probe_tx.send(result).unwrap();
    });

    let probe_result = probe_rx.recv_timeout(Duration::from_secs(10));
    release_tx.send(()).unwrap();
    probe.join().unwrap();
    writer.join().unwrap().unwrap();

    probe_result
        .expect("read/independent-write probe was blocked by an uncommitted index transaction")
        .unwrap();
    assert!(lookup(&db, dataset_a, &staged_rows.last().unwrap().hash).is_some());
}
