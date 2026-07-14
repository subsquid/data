//! Tests for table cleanup:
//!   * Phase 1 ([`Database::cleanup`]) -- logical, snapshot-safe point deletes.
//!   * Physical reclaim ([`Database::reclaim_disk_space`]) -- SST-file unlink below the
//!     live watermark. It ignores snapshots, so it runs only where there are no live
//!     readers (startup in production); fully decoupled from Phase 1.
//!   * Runtime physical reclaim ([`Database::reclaim_disk_space_runtime`]) -- the same
//!     unlink with deleted tables pinned until RocksDB's oldest snapshot passes their
//!     point-tombstone sequence.

mod mock_db;
#[allow(dead_code)] // shared with the other integration tests; this one uses a subset
mod utils;

use mock_db::{MockDB, Table};

/// Phase 1 is invisible to readers that already hold a snapshot: a query in flight when
/// its chunk is deleted keeps reading every row, while new snapshots see nothing.
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

    assert_eq!(db.cleanup(), 0, "re-running Phase 1 is a no-op");
}

/// `DeleteFilesInRange` starts its level loop at 1, so dead data still sitting in L0 is not
/// unlinkable however far below the watermark it is. `reclaim-measure` reports those bytes
/// separately for exactly this reason.
#[test]
fn reclaim_skips_level_zero_files() {
    let mut db = MockDB::new();
    let t = db.commit_table(2000);
    db.flush(); // memtable -> L0, and auto-compaction is off, so it stays there
    let before = db.sst_size();
    assert!(before > 0, "expected flushed SST data");

    // No live table remains, so the watermark is unbounded and every file is below it.
    db.delete(&t);
    db.reclaim();
    assert_eq!(db.sst_size(), before, "an L0 file is never unlinked");

    // The identical call frees the same data once compaction has moved it off L0.
    db.compact_to_bottom();
    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "expected reclaim below L0: before={before} after={after}"
    );
}

/// Physical reclaim unlinks dead SST files below the watermark. With no live table left,
/// the watermark is unbounded and every dead file is dropped.
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
    // Flush the Phase-1 tombstones out of the memtable. Auto-compaction is off, so
    // nothing but the unlink can free space here.
    db.flush();

    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "expected physical reclaim: before={before} after={after}"
    );
}

/// The watermark is the min live `TableId` over ALL datasets, so a single old *live*
/// table pins it low and dead tables with larger ids are not file-reclaimable until that
/// live table is gone. Live data is never unlinked; the known limitation is heterogeneous
/// retention.
#[test]
fn live_table_pins_reclaim_watermark() {
    let mut db = MockDB::new();
    // Creation order == id order, so `older` is the smaller id.
    let older = db.commit_table(1000);
    let newer = db.commit_table(1000);
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    // Delete the NEWER table; the older one stays live and pins the watermark.
    db.delete(&newer);
    db.cleanup();
    db.flush();

    // Dead `newer` sits above the watermark pinned by live `older`, so its files survive
    // and `older` stays readable. Auto-compaction is off, so only the unlink can shrink
    // CF_TABLES -- nothing was unlinked iff the size did not drop.
    db.reclaim();
    assert!(
        db.sst_size() >= before,
        "a dead table above the watermark must NOT be unlinked"
    );
    assert_eq!(db.read(&db.snapshot(), &older), older.rows);

    // Remove the older table too: the watermark lifts and everything is reclaimable.
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

/// Both steps are idempotent -- crash safety relies on it.
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

/// End-to-end: `delete_dataset` runs Phase 1 synchronously, and the
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

/// The trade-off behind running [`Database::reclaim_disk_space`] only at startup: the
/// unlink ignores snapshots, so reclaiming under a live pre-deletion snapshot pulls its
/// files out and the read fails loudly, rather than returning wrong rows. Cache disabled
/// so the data is genuinely gone, not served warm.
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

    // Reclaim while that snapshot is STILL live. No live table remains, so the watermark
    // is unbounded and the reader's files are unlinked from under it.
    db.reclaim();

    assert!(
        db.try_read(&reader, &t).is_err(),
        "reading a table whose files were unlinked under a live snapshot must fail, not return wrong rows"
    );
}

/// Runtime reclaim pins a deleted table while a pre-deletion query snapshot can still read
/// it. Once that one snapshot is dropped, a continuously live *newer* snapshot does not
/// prevent the same files from being unlinked.
#[test]
fn runtime_reclaim_waits_only_for_pre_delete_snapshots() {
    let mut db = MockDB::uncached();
    let t = db.commit_table(3000);
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    let old_reader = db.snapshot();
    db.delete(&t);
    assert_eq!(db.cleanup(), 1);
    db.flush();

    let first = db.runtime_reclaim();
    assert_eq!(first.safe_deleted_tables, 0);
    assert_eq!(first.unsafe_deleted_tables, 1);
    assert!(first.oldest_snapshot_sequence.is_some());
    assert!(db.sst_size() >= before, "the old reader must pin its table files");
    assert_eq!(db.read(&old_reader, &t), t.rows);

    // This models constant query traffic: at least one newer snapshot remains live while
    // the last pre-deletion snapshot goes away.
    let newer_reader = db.snapshot();
    drop(old_reader);

    let second = db.runtime_reclaim();
    assert!(second.snapshot_count >= 1, "the newer query snapshot is still live");
    assert_eq!(second.safe_deleted_tables, 1);
    assert_eq!(second.unsafe_deleted_tables, 0);
    assert!(
        db.sst_size() * 4 < before,
        "newer snapshots must not pin pre-tombstone files: before={before} after={}",
        db.sst_size()
    );
    assert!(!db.has_visible_chunk());
    drop(newer_reader);
}

/// A deletion request is not sequence-safe until Phase 1 has actually written its point
/// tombstones. Runtime reclaim therefore leaves both its files and cleanup record alone.
#[test]
fn runtime_reclaim_waits_for_logical_cleanup() {
    let mut db = MockDB::new();
    let t = db.commit_table(2000);
    db.compact_to_bottom();
    let before = db.sst_size();

    db.delete(&t);
    let report = db.runtime_reclaim();
    assert_eq!(report.safe_deleted_tables, 0);
    assert_eq!(report.unsafe_deleted_tables, 1);
    assert!(db.sst_size() >= before);

    assert_eq!(db.cleanup(), 1, "runtime reclaim must preserve pending Phase 1 work");
}

/// Disabling runtime reclaim also disables its sequence bookkeeping. Point deletes remain
/// snapshot-safe, but completed purge records must not accumulate forever.
#[test]
fn cleanup_drops_sequence_records_when_runtime_reclaim_is_disabled() {
    let mut db = MockDB::without_runtime_reclaim();
    let first_deleted = db.commit_table(1000);
    let newly_deleted = db.commit_table(1000);
    let old_reader = db.snapshot();

    db.delete(&first_deleted);
    assert_eq!(db.cleanup(), 1);
    db.delete(&newly_deleted);
    assert_eq!(db.cleanup(), 1);
    assert_eq!(db.read(&old_reader, &first_deleted), first_deleted.rows);
    assert_eq!(db.read(&old_reader, &newly_deleted), newly_deleted.rows);
    drop(old_reader);

    assert!(db.try_runtime_reclaim().is_err());
    assert_eq!(db.cleanup(), 0);
}

/// An orphaned `DIRTY_TABLES` marker -- left when a build dies before committing its
/// chunk -- counts as a live table, pinning disk reclaim for every later table. Startup
/// recovery drops it so the watermark lifts.
#[test]
fn orphan_dirty_marker_unpinned_by_purge() {
    let mut db = MockDB::new();
    // Orphan created first, so it has the smaller id and pins the watermark low.
    let orphan = db.orphan_table(1000);
    let live = db.commit_table(1000);
    assert!(
        orphan.id < live.id,
        "orphan must be the smaller id to pin the watermark"
    );

    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    db.delete(&live);
    assert_eq!(db.cleanup(), 1);
    db.flush();

    // The orphan pins the watermark at its own id, so the dead (higher-id) table cannot
    // be unlinked.
    db.reclaim();
    assert!(
        db.sst_size() >= before,
        "orphan pins the watermark, the dead table's files survive"
    );

    // Startup recovery removes the orphan marker (and tombstones its data).
    assert_eq!(db.purge_orphans(), 1, "one orphan marker purged");
    db.flush();

    // The watermark lifts, so the dead table is now reclaimable.
    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "expected physical reclaim once the orphan no longer pins the watermark: before={before} after={after}"
    );

    assert_eq!(db.purge_orphans(), 0, "purge is idempotent");
}

/// Phase 1 and physical reclaim are decoupled: reclaim unlinks dead files purely by the
/// watermark, with no dependence on a preceding `cleanup()` -- mirroring the startup path,
/// which runs before any Phase 1. It must also leave the bookkeeping Phase 1 still owes.
#[test]
fn reclaim_is_independent_of_phase1() {
    let mut db = MockDB::new();
    let t = db.commit_table(2000);
    db.compact_to_bottom();
    let before = db.sst_size();
    assert!(before > 0);

    // Logical-delete but do NOT run Phase 1, so no point deletes are issued.
    db.delete(&t);

    // No live table remains, so the watermark is unbounded and the dead table's files are
    // unlinked with no preceding tombstone.
    db.reclaim();
    let after = db.sst_size();
    assert!(
        after * 4 < before,
        "reclaim frees dead files without a preceding Phase 1: before={before} after={after}"
    );

    // Reclaim left the bookkeeping entry untouched, so a later Phase 1 still finds it.
    assert_eq!(
        db.cleanup(),
        1,
        "the deletion record survived reclaim and is handled by Phase 1"
    );
    assert_eq!(db.cleanup(), 0);
}
