//! Tests for table cleanup:
//!   * Phase 1 ([`Database::cleanup`]) -- logical, snapshot-safe range tombstones.
//!   * Physical reclaim ([`Database::reclaim_disk_space`]) -- SST-file unlink below the
//!     live watermark. It ignores snapshots, so it runs only where there are no live
//!     readers (startup in production); fully decoupled from Phase 1.
//!
//! Everything runs against [`MockDB`] (see `mock_db`), which hides the shared setup so
//! each test reads as domain steps (commit / delete / cleanup / reclaim / snapshot /
//! purge) asserting on observable effects (rows read, files freed).

mod mock_db;
mod utils;

use mock_db::{MockDB, Table};

/// Phase 1 must be invisible to readers that already hold a snapshot: a query
/// in flight when its chunk is deleted keeps reading every row (RocksDB MVCC --
/// range tombstones respect older snapshots), while new snapshots see nothing.
#[test]
fn logical_delete_is_snapshot_safe() {
    let mut db = MockDB::new();
    let t = db.commit_table(50);

    // Snapshot taken BEFORE the deletion -- models an in-flight query.
    let reader = db.snapshot();

    db.delete(&t);
    assert_eq!(db.cleanup(), 1, "one table logically deleted");

    // A fresh snapshot no longer sees the chunk...
    assert!(!db.has_visible_chunk());
    // ...but the pre-deletion snapshot still reads every row.
    assert_eq!(db.read(&reader, &t), t.rows);

    // Running Phase 1 again is a no-op (already logically deleted).
    assert_eq!(db.cleanup(), 0);
}

/// Physical reclaim unlinks dead SST files below the watermark. With no live
/// table left, the watermark is unbounded and every dead file is dropped.
#[test]
fn reclaim_unlinks_dead_sst_files() {
    let mut db = MockDB::new();
    let tables: Vec<Table> = (0..3).map(|_| db.commit_table(1000)).collect();
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0, "expected flushed SST data");

    for t in &tables {
        db.delete(t);
    }
    assert_eq!(db.cleanup(), 3);
    // Flush the Phase-1 range tombstones out of the memtable. Auto-compaction is
    // off, so the flushed tombstone SST won't trigger a background compaction
    // that races the assertions -- the unlink is the only thing freeing space.
    db.flush();

    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "expected physical reclaim: before={before} after={after}"
    );
}

/// The watermark is the min live `TableId` over ALL datasets, so a single old
/// *live* table pins it low and dead tables with larger ids are NOT
/// file-reclaimable until that live table is gone. Verifies live data is never
/// unlinked and documents the known limitation (heterogeneous retention).
#[test]
fn live_table_pins_reclaim_watermark() {
    let mut db = MockDB::new();
    // Creation order == id order, so `older` is the smaller id (the
    // watermark-pinning live table) and `newer` is deleted first.
    let older = db.commit_table(1000);
    let newer = db.commit_table(1000);
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    // Delete the NEWER table; the older one stays live and pins the watermark.
    db.delete(&newer);
    db.cleanup();
    db.flush();

    // Dead `newer` sits above the watermark pinned by live `older` -> not
    // unlinked; its files survive and `older` stays fully readable.
    db.reclaim();
    assert!(
        db.sst_size() * 4 > before,
        "a dead table above the watermark must NOT be unlinked"
    );
    assert_eq!(db.read(&db.snapshot(), &older), older.rows);

    // Remove the older table too -> watermark lifts -> everything reclaimable.
    db.delete(&older);
    db.cleanup();
    db.flush();
    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "expected reclaim once the watermark lifts: before={before} after={after}"
    );
}

/// Both steps are idempotent (crash-safety relies on it): re-running after
/// completion does no work and does not error.
#[test]
fn cleanup_and_reclaim_are_idempotent() {
    let mut db = MockDB::new();
    let t = db.commit_table(1000);
    db.delete(&t);

    assert_eq!(db.cleanup(), 1);
    assert_eq!(db.cleanup(), 0);
    db.flush();
    db.reclaim();
    db.reclaim(); // a second run must not panic or error
    assert_eq!(db.cleanup(), 0);
}

/// End-to-end: `delete_dataset` runs Phase 1 synchronously (now cheap), and the
/// subsequent startup reclaim (no readers) frees the disk.
#[test]
fn delete_dataset_then_reclaim_frees_space() {
    let mut db = MockDB::new();
    let _t0 = db.commit_table(1000);
    let _t1 = db.commit_table(1000);
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    db.delete_dataset();
    assert!(db.has_no_datasets());
    db.flush(); // flush Phase-1 tombstones so the unlink can drop the files

    db.reclaim();
    let after = db.sst_size();
    assert!(after * 4 < before, "expected reclaim: before={before} after={after}");
}

/// Documents the trade-off behind running [`Database::reclaim_disk_space`] only with
/// no live readers (startup): the unlink IGNORES snapshots, so reclaiming under a live
/// pre-deletion snapshot pulls its files out and the read fails loudly (error), not
/// silently wrong. Cache disabled so the data is genuinely gone, not served warm.
#[test]
fn reclaim_breaks_a_live_pre_deletion_reader() {
    let mut db = MockDB::uncached();
    let t = db.commit_table(3000);
    db.compact_to_bottom();

    // In-flight query: snapshot taken BEFORE the deletion.
    let reader = db.snapshot();

    db.delete(&t);
    assert_eq!(db.cleanup(), 1);
    db.flush();

    // Baseline: the reader still reads every row while its files are present.
    assert_eq!(db.read(&reader, &t), t.rows);

    // Reclaim while that snapshot is STILL live. No live table remains, so the
    // watermark is unbounded and the reader's files are unlinked from under it.
    db.reclaim();

    // The same reader can no longer read the table -- it fails, not returns wrong
    // rows. Running reclaim only at startup (no live readers) is what avoids this.
    assert!(
        db.try_read(&reader, &t).is_err(),
        "reading a table whose files were unlinked under a live snapshot must fail, not return wrong rows"
    );
}

/// An orphaned `DIRTY_TABLES` marker -- left when a build's chunk is never
/// committed (a crash/abandon before `write_chunk` removes it) -- is counted as a
/// live table by the watermark, so it pins disk reclaim for every later table.
/// Startup recovery (`purge_orphan_dirty_tables`) drops it so the watermark lifts
/// and dead tables become reclaimable again.
#[test]
fn orphan_dirty_marker_unpinned_by_purge() {
    let mut db = MockDB::new();
    // Orphan created FIRST -> smaller id -> pins the watermark low. Its chunk is
    // never committed, so nothing ever removes its dirty marker.
    let orphan = db.orphan_table(1000);
    let live = db.commit_table(1000);
    assert!(
        orphan.id < live.id,
        "orphan must be the smaller id to pin the watermark"
    );

    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    // Delete the live table and tombstone it (Phase 1).
    db.delete(&live);
    assert_eq!(db.cleanup(), 1);
    db.flush();

    // The orphan marker pins the watermark at the orphan's id, so the dead
    // (higher-id) `live` table cannot be unlinked.
    db.reclaim();
    assert!(
        db.sst_size() * 4 > before,
        "orphan pins the watermark, the dead table's files survive"
    );

    // Startup recovery removes the orphan marker (and tombstones its data).
    assert_eq!(db.purge_orphans(), 1, "one orphan marker purged");
    db.flush();

    // Watermark lifts -> the dead table is now reclaimable.
    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "expected physical reclaim once the orphan no longer pins the watermark: before={before} after={after}"
    );

    // Purge is idempotent: no markers remain.
    assert_eq!(db.purge_orphans(), 0);
}

/// Phase 1 and physical reclaim are decoupled: reclaim unlinks dead files purely
/// by the watermark (id ordering), with no dependence on a preceding `cleanup()`.
/// This mirrors the startup reclaim path, which runs before any Phase 1, and it
/// must not consume the deletion bookkeeping that Phase 1 still owes work on.
#[test]
fn reclaim_is_independent_of_phase1() {
    let mut db = MockDB::new();
    let t = db.commit_table(2000);
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    // Logical-delete but DO NOT run Phase 1 (no range tombstone issued).
    db.delete(&t);

    // No live table remains -> watermark unbounded -> the dead table's files are
    // unlinked anyway. Reclaim needs no preceding tombstone.
    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "reclaim frees dead files without a preceding Phase 1: before={before} after={after}"
    );

    // Reclaim left the bookkeeping entry untouched, so a later Phase 1 still
    // finds and tombstones it (covering any boundary/above-watermark remnants).
    assert_eq!(
        db.cleanup(),
        1,
        "the deletion record survived reclaim and is handled by Phase 1"
    );
    assert_eq!(db.cleanup(), 0);
}
