use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema}
};
use sqd_primitives::BlockRef;
use sqd_storage::{
    db::{Chunk, CompactionStatus, Database, DatabaseSettings, DatasetId, DatasetKind},
    table::write::use_small_buffers
};
use tempfile::TempDir;

fn open_db_with(kind: &str, block_hash_index: bool) -> (TempDir, Database, DatasetId) {
    // The TempDir guard is returned and kept alive for the whole test so the
    // on-disk database isn't removed out from under RocksDB.
    let db_dir = tempfile::tempdir().unwrap();
    let db = reopen(&db_dir, block_hash_index);
    let dataset_id = DatasetId::from_str("test-dataset");
    db.create_dataset(dataset_id, DatasetKind::from_str(kind)).unwrap();
    (db_dir, db, dataset_id)
}

/// Opens the database at `dir` again, e.g. to simulate a restart with a
/// different `block_hash_index` setting. Any previous `Database` over the same
/// directory must be dropped first - RocksDB holds an exclusive lock on it.
fn reopen(dir: &TempDir, block_hash_index: bool) -> Database {
    DatabaseSettings::default()
        .with_block_hash_index(block_hash_index)
        .open(dir.path())
        .unwrap()
}

fn open_db(kind: &str) -> (TempDir, Database, DatasetId) {
    open_db_with(kind, true)
}

fn setup_evm_db() -> (TempDir, Database, DatasetId) {
    open_db("evm")
}

/// Canonical hash for a block number.
fn block_hash(n: u64) -> String {
    format!("0x{:064x}", n)
}

/// Builds an EVM-shaped chunk: a `blocks` table with `number` (UInt64) and
/// `hash` (Utf8) columns covering `first..=last`, hashes derived via `hash_fn`.
fn make_evm_chunk_with(
    db: &Database,
    first: u64,
    last: u64,
    parent_hash: &str,
    hash_fn: impl Fn(u64) -> String
) -> Chunk {
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
    ]));

    let numbers: Vec<u64> = (first..=last).collect();
    let hashes: Vec<String> = numbers.iter().map(|n| hash_fn(*n)).collect();

    let number_arr = Arc::new(UInt64Array::from(numbers)) as ArrayRef;
    let hash_arr = Arc::new(StringArray::from(
        hashes.iter().map(String::as_str).collect::<Vec<&str>>()
    )) as ArrayRef;

    let mut builder = db.new_table_builder(schema.clone());
    let batch = RecordBatch::try_new(schema, vec![number_arr, hash_arr]).unwrap();
    builder.write_record_batch(&batch).unwrap();

    let mut tables = BTreeMap::new();
    tables.insert("blocks".to_owned(), builder.finish().unwrap());

    Chunk::V1 {
        first_block: first,
        last_block: last,
        last_block_hash: hash_fn(last),
        parent_block_hash: parent_hash.to_owned(),
        first_block_time: None,
        last_block_time: None,
        tables
    }
}

fn make_evm_chunk(db: &Database, first: u64, last: u64, parent_hash: &str) -> Chunk {
    make_evm_chunk_with(db, first, last, parent_hash, block_hash)
}

fn lookup(db: &Database, dataset_id: DatasetId, hash: &str) -> Option<BlockRef> {
    db.snapshot().find_block_by_hash(dataset_id, hash).unwrap()
}

fn assert_resolves(db: &Database, dataset_id: DatasetId, n: u64) {
    assert_eq!(
        lookup(db, dataset_id, &block_hash(n)),
        Some(BlockRef {
            number: n,
            hash: block_hash(n)
        }),
        "block {} should resolve via its canonical hash",
        n
    );
}

fn assert_absent(db: &Database, dataset_id: DatasetId, hash: &str) {
    assert_eq!(lookup(db, dataset_id, hash), None, "hash {} should not resolve", hash);
}

#[test]
fn index_ingest_and_lookup() {
    let (_dir, db, dataset_id) = setup_evm_db();

    let chunk = make_evm_chunk(&db, 0, 9, "base");
    db.insert_chunk(dataset_id, &chunk).unwrap();

    for n in 0..=9 {
        assert_resolves(&db, dataset_id, n);
    }
    assert_absent(&db, dataset_id, "0xdeadbeef");
    assert_absent(&db, dataset_id, &block_hash(10));
}

#[test]
fn index_large_chunk_spans_multiple_read_batches() {
    // > 4096 rows forces `for_each_block_hash` through more than one batch,
    // exercising the offset advancement across the batch boundary.
    let (_dir, db, dataset_id) = setup_evm_db();

    let last = 5000;
    let chunk = make_evm_chunk(&db, 0, last, "base");
    db.insert_chunk(dataset_id, &chunk).unwrap();

    for n in [0, 1, 4095, 4096, 4097, last] {
        assert_resolves(&db, dataset_id, n);
    }
}

#[test]
fn index_fork_replaces_hashes() {
    let (_dir, db, dataset_id) = setup_evm_db();

    let chunk1 = make_evm_chunk(&db, 0, 9, "base");
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9));
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();

    // Fork rewrites blocks 10..=19 with different hashes.
    let fork = make_evm_chunk_with(&db, 10, 19, &block_hash(9), |n| format!("fork_{}", n));
    db.insert_fork(dataset_id, &fork).unwrap();

    // chunk1's hashes are untouched.
    for n in 0..=9 {
        assert_resolves(&db, dataset_id, n);
    }
    // old canonical hashes of the forked range are gone, forked ones resolve.
    for n in 10..=19 {
        assert_absent(&db, dataset_id, &block_hash(n));
        assert_eq!(
            lookup(&db, dataset_id, &format!("fork_{}", n)),
            Some(BlockRef {
                number: n,
                hash: format!("fork_{}", n)
            })
        );
    }
}

#[test]
fn index_delete_chunk_removes_hashes() {
    // Models the retention path (DatasetUpdate::delete_chunk).
    let (_dir, db, dataset_id) = setup_evm_db();

    let chunk1 = make_evm_chunk(&db, 0, 9, "base");
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9));
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();

    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk1)).unwrap();

    for n in 0..=9 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }
    for n in 10..=19 {
        assert_resolves(&db, dataset_id, n);
    }
}

#[test]
fn index_delete_dataset_removes_all_hashes() {
    let (_dir, db, dataset_id) = setup_evm_db();

    let chunk1 = make_evm_chunk(&db, 0, 9, "base");
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9));
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();

    db.delete_dataset(dataset_id).unwrap();

    assert!(db.get_all_datasets().unwrap().is_empty());
    for n in 0..=19 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }
}

#[test]
fn index_survives_compaction() {
    // Regression guard for the "compaction must not touch the index" decision.
    // Many small chunks (>= 50) ensure real merging is triggered.
    let (_dir, db, dataset_id) = setup_evm_db();
    let _sb = use_small_buffers();

    let n_chunks = 60u64;
    let blocks_per_chunk = 4u64;
    let mut parent = "base".to_owned();
    for c in 0..n_chunks {
        let first = c * blocks_per_chunk;
        let last = first + blocks_per_chunk - 1;
        let chunk = make_evm_chunk(&db, first, last, &parent);
        db.insert_chunk(dataset_id, &chunk).unwrap();
        parent = block_hash(last);
    }
    let total_blocks = n_chunks * blocks_per_chunk;

    for n in 0..total_blocks {
        assert_resolves(&db, dataset_id, n);
    }

    let mut merged = false;
    loop {
        match db
            .perform_dataset_compaction(dataset_id, Some(100), Some(1.25), None)
            .unwrap()
        {
            CompactionStatus::Ok(_) => merged = true,
            _ => break
        }
    }
    assert!(merged, "expected compaction to merge at least once");

    // Sanity: chunks really were merged (fewer than we inserted).
    let chunk_count = db.snapshot().list_chunks(dataset_id, 0, None).count();
    assert!(chunk_count < n_chunks as usize, "compaction should reduce chunk count");

    // The index is untouched: every hash still resolves to the same number.
    for n in 0..total_blocks {
        assert_resolves(&db, dataset_id, n);
    }
}

#[test]
fn non_evm_dataset_is_not_indexed() {
    // Same EVM-shaped blocks table, but a solana dataset -> nothing is indexed.
    let (_dir, db, dataset_id) = open_db("solana");

    let chunk = make_evm_chunk(&db, 0, 9, "base");
    db.insert_chunk(dataset_id, &chunk).unwrap();

    for n in 0..=9 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }
}

#[test]
fn index_disabled_writes_nothing() {
    // An EVM dataset still isn't indexed while the flag is off.
    let (_dir, db, dataset_id) = open_db_with("evm", false);

    let chunk = make_evm_chunk(&db, 0, 9, "base");
    db.insert_chunk(dataset_id, &chunk).unwrap();

    for n in 0..=9 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }

    // Pruning a never-indexed chunk short-circuits on the prefix probe.
    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk)).unwrap();
}

#[test]
fn index_entries_drain_after_flag_is_turned_off() {
    // Guards the asymmetric gating: `index_block_hashes` honours the flag,
    // `unindex_block_hashes` does not. Entries written while the flag was on must
    // still be reclaimed by retention once it goes off, or they would be stranded.
    let (dir, db, dataset_id) = setup_evm_db();

    let chunk1 = make_evm_chunk(&db, 0, 9, "base");
    let chunk2 = make_evm_chunk(&db, 10, 19, &block_hash(9));
    db.insert_chunk(dataset_id, &chunk1).unwrap();
    db.insert_chunk(dataset_id, &chunk2).unwrap();
    drop(db);

    // Restart with indexing disabled.
    let db = reopen(&dir, false);

    // New chunks are no longer indexed...
    let chunk3 = make_evm_chunk(&db, 20, 29, &block_hash(19));
    db.insert_chunk(dataset_id, &chunk3).unwrap();
    for n in 20..=29 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }

    // ...but pruning still reclaims what the previous run wrote.
    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk1)).unwrap();
    for n in 0..=9 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }
    for n in 10..=19 {
        assert_resolves(&db, dataset_id, n);
    }

    // Down to the last entry, after which the probe short-circuits.
    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk2)).unwrap();
    for n in 10..=19 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }
    db.update_dataset(dataset_id, |tx| tx.delete_chunk(&chunk3)).unwrap();
}

#[test]
fn probe_sees_pending_writes_within_the_same_transaction() {
    // Verifies the claim in `has_block_hash_entries`: iterating the transaction
    // merges its own uncommitted puts, so a chunk indexed and pruned inside one
    // `update_dataset` closure leaves nothing behind, even though the dataset had
    // zero committed entries when the probe ran.
    let (_dir, db, dataset_id) = setup_evm_db();

    let chunk = make_evm_chunk(&db, 0, 9, "base");
    db.update_dataset(dataset_id, |tx| {
        tx.insert_chunk(&chunk)?;
        tx.delete_chunk(&chunk)
    })
    .unwrap();

    for n in 0..=9 {
        assert_absent(&db, dataset_id, &block_hash(n));
    }
}
