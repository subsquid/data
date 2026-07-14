use anyhow::Context;
use tracing::warn;

use crate::{
    db::{
        db::{
            RocksDB, RocksIterator, RocksSnapshot, RocksWriteBatch, CF_CHUNKS, CF_DELETED_TABLES, CF_DIRTY_TABLES,
            CF_TABLES
        },
        deleted_table::DeletedTableState,
        reclaim::{reclaim_upper_bound, watermark, RuntimeReclaimResult, RECLAIM_LOWER_BOUND},
        table_id::TableId,
        write::{inflight::ReclaimFence, storage::WRITE_BATCH_SIZE_LIMIT},
        Chunk
    },
    table::key::TableKeyFactory
};

struct DeletedTableScan {
    records: Vec<(TableId, DeletedTableState)>,
    malformed_keys: Vec<Vec<u8>>,
    recovered_values: usize
}

/// Phase 1 -- logical, snapshot-safe purge of deleted tables.
///
/// Point-deletes the `CF_TABLES` keys of every newly requested table. When runtime reclaim
/// is enabled, records an upper bound on the tombstone batch's RocksDB sequence and retains
/// it until every live snapshot is new enough for whole-file reclaim. Otherwise removes the
/// record atomically with the point deletes. Idempotent. Returns tables newly point-deleted.
pub(crate) fn logical_cleanup(db: &RocksDB, retain_reclaim_metadata: bool) -> anyhow::Result<usize> {
    // Collect first, then mutate: writing mid-iteration disturbs the cursor.
    let scan = scan_deleted_tables(db)?;
    drop_malformed_keys(db, CF_DELETED_TABLES, &scan.malformed_keys)?;
    if scan.recovered_values > 0 {
        warn!(
            records = scan.recovered_values,
            "recovering malformed deletion states as pending deletes"
        );
    }

    let mut purged = 0;
    for (id, state) in scan.records {
        match state {
            DeletedTableState::DeleteRequested => {
                purge_deleted_table(db, &id, retain_reclaim_metadata)?;
                purged += 1;
            }
            DeletedTableState::PurgedUnstamped => {
                if retain_reclaim_metadata {
                    stamp_purged_table(db, &id)?
                } else {
                    drop_deleted_table_record(db, &id)?
                }
            }
            DeletedTableState::Purged { .. } => {
                if !retain_reclaim_metadata {
                    drop_deleted_table_record(db, &id)?
                }
            }
        }
    }

    Ok(purged)
}

fn scan_deleted_tables(db: &RocksDB) -> anyhow::Result<DeletedTableScan> {
    let cf = db.cf_handle(CF_DELETED_TABLES).unwrap();
    collect_deleted_tables(db.raw_iterator_cf(cf))
}

fn scan_deleted_tables_at(db: &RocksDB, snapshot: &RocksSnapshot<'_, RocksDB>) -> anyhow::Result<DeletedTableScan> {
    let cf = db.cf_handle(CF_DELETED_TABLES).unwrap();
    collect_deleted_tables(snapshot.raw_iterator_cf(cf))
}

fn collect_deleted_tables(mut it: RocksIterator<'_, RocksDB>) -> anyhow::Result<DeletedTableScan> {
    let mut records = Vec::new();
    let mut malformed_keys = Vec::new();
    let mut recovered_values = 0usize;

    it.seek_to_first();
    while it.valid() {
        let key = it.key().unwrap();
        match TableId::try_from_key(key) {
            Some(id) => match DeletedTableState::decode(it.value().unwrap()) {
                Ok(state) => records.push((id, state)),
                Err(_) => {
                    // A key in CF_DELETED_TABLES is no longer referenced by a committed
                    // chunk. Replaying its point deletes is idempotent and, unlike dropping
                    // the bad value, creates a fresh sequence pin for old snapshots.
                    records.push((id, DeletedTableState::DeleteRequested));
                    recovered_values = recovered_values.saturating_add(1);
                }
            },
            None => malformed_keys.push(key.to_vec())
        }
        it.next();
    }
    it.status()?;

    Ok(DeletedTableScan {
        records,
        malformed_keys,
        recovered_values
    })
}

/// Scan `cf`, splitting keys into well-formed `TableId`s and malformed leftovers.
/// Malformed keys are returned rather than skipped, so callers can delete them via
/// [`drop_malformed_keys`] -- left in place, every pass would rescan them forever.
fn scan_table_ids(db: &RocksDB, cf: &str) -> anyhow::Result<(Vec<TableId>, Vec<Vec<u8>>)> {
    let cf = db.cf_handle(cf).unwrap();
    collect_table_ids(db.raw_iterator_cf(cf))
}

fn scan_table_ids_at(
    db: &RocksDB,
    snapshot: &RocksSnapshot<'_, RocksDB>,
    cf: &str
) -> anyhow::Result<(Vec<TableId>, Vec<Vec<u8>>)> {
    let cf = db.cf_handle(cf).unwrap();
    collect_table_ids(snapshot.raw_iterator_cf(cf))
}

fn collect_table_ids(mut it: RocksIterator<'_, RocksDB>) -> anyhow::Result<(Vec<TableId>, Vec<Vec<u8>>)> {
    let mut ids = Vec::new();
    let mut malformed = Vec::new();

    it.seek_to_first();
    while it.valid() {
        let key = it.key().unwrap();
        match TableId::try_from_key(key) {
            Some(id) => ids.push(id),
            None => malformed.push(key.to_vec())
        }
        it.next();
    }
    it.status()?;

    Ok((ids, malformed))
}

/// Delete malformed bookkeeping keys outright -- no other code path can remove them.
fn drop_malformed_keys(db: &RocksDB, cf: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let cf_name = cf;
    let cf = db.cf_handle(cf_name).unwrap();
    let mut batch = RocksWriteBatch::default();
    for key in keys {
        batch.delete_cf(cf, key);
    }
    db.write(batch)?;
    warn!(
        column_family = cf_name,
        records = keys.len(),
        "dropped malformed bookkeeping records"
    );
    Ok(())
}

/// Point-delete all of `id`'s `CF_TABLES` data in bounded batches, retain an unstamped
/// deletion record in the final batch, then persist a conservative upper bound on the
/// tombstones' RocksDB sequence.
///
/// Point deletes rather than a single `delete_range` tombstone: range deletions are not
/// officially supported on the transactional `OptimisticTransactionDB`.
fn purge_deleted_table(db: &RocksDB, id: &TableId, retain_reclaim_metadata: bool) -> anyhow::Result<()> {
    purge_deleted_table_impl(db, id, retain_reclaim_metadata, WRITE_BATCH_SIZE_LIMIT, &mut |batch| {
        db.write(batch)?;
        Ok(())
    })
}

fn purge_deleted_table_impl(
    db: &RocksDB,
    id: &TableId,
    retain_reclaim_metadata: bool,
    batch_size_limit: usize,
    write_batch: &mut impl FnMut(RocksWriteBatch) -> anyhow::Result<()>
) -> anyhow::Result<()> {
    let cf_deleted = db.cf_handle(CF_DELETED_TABLES).unwrap();
    let cf_dirty = db.cf_handle(CF_DIRTY_TABLES).unwrap();

    let mut batch = RocksWriteBatch::default();
    tombstone_table_data(db, id, &mut batch, batch_size_limit, write_batch)?;
    if retain_reclaim_metadata {
        batch.put_cf(cf_deleted, id, DeletedTableState::PurgedUnstamped.encode());
    } else {
        // The point tombstones and record removal share a batch, so a crash cannot lose
        // pending Phase 1 work even when runtime reclaim metadata is disabled.
        batch.delete_cf(cf_deleted, id);
    }
    batch.delete_cf(cf_dirty, id);
    write_batch(batch)?;

    if retain_reclaim_metadata {
        stamp_purged_table(db, id)?;
    }

    Ok(())
}

fn drop_deleted_table_record(db: &RocksDB, id: &TableId) -> anyhow::Result<()> {
    db.delete_cf(db.cf_handle(CF_DELETED_TABLES).unwrap(), id)?;
    Ok(())
}

/// Recoverable second half of a purge. If the process dies between the point-delete batch
/// and this write, `PurgedUnstamped` remains and the next cleanup stamps the already-present
/// tombstones with a newer (therefore conservative) sequence.
fn stamp_purged_table(db: &RocksDB, id: &TableId) -> anyhow::Result<()> {
    let sequence = db.latest_sequence_number();
    let cf_deleted = db.cf_handle(CF_DELETED_TABLES).unwrap();
    db.put_cf(cf_deleted, id, DeletedTableState::Purged { sequence }.encode())?;

    Ok(())
}

/// Startup-only orphan cleanup. No query exists, so no sequence record has to survive.
fn purge_orphan_table(db: &RocksDB, id: &TableId) -> anyhow::Result<()> {
    let cf_deleted = db.cf_handle(CF_DELETED_TABLES).unwrap();
    let cf_dirty = db.cf_handle(CF_DIRTY_TABLES).unwrap();

    let mut batch = RocksWriteBatch::default();
    tombstone_table_data(db, id, &mut batch, WRITE_BATCH_SIZE_LIMIT, &mut |batch| {
        db.write(batch)?;
        Ok(())
    })?;
    batch.delete_cf(cf_deleted, id);
    batch.delete_cf(cf_dirty, id);
    db.write(batch)?;

    Ok(())
}

fn tombstone_table_data(
    db: &RocksDB,
    id: &TableId,
    batch: &mut RocksWriteBatch,
    batch_size_limit: usize,
    write_batch: &mut impl FnMut(RocksWriteBatch) -> anyhow::Result<()>
) -> anyhow::Result<()> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();

    let mut start = TableKeyFactory::new(id);
    let mut end = TableKeyFactory::new(id);
    let start_key = start.start();
    let end_key = end.end();

    // Keep iteration stable while partial tombstone batches are committed beneath it.
    let snapshot = db.snapshot();
    let mut it = snapshot.raw_iterator_cf(cf_tables);
    it.seek(start_key);
    while it.valid() {
        let key = it.key().unwrap();
        if key >= end_key {
            break;
        }
        batch.delete_cf(cf_tables, key);
        if let Some(full_batch) = take_full_tombstone_batch(batch, batch_size_limit) {
            write_batch(full_batch)?;
        }
        it.next();
    }
    it.status()?;

    Ok(())
}

fn take_full_tombstone_batch(batch: &mut RocksWriteBatch, batch_size_limit: usize) -> Option<RocksWriteBatch> {
    (batch.size_in_bytes() >= batch_size_limit).then(|| std::mem::take(batch))
}

#[cfg(test)]
pub(in crate::db) fn purge_deleted_table_with_writer(
    db: &RocksDB,
    id: &TableId,
    retain_reclaim_metadata: bool,
    batch_size_limit: usize,
    write_batch: &mut impl FnMut(RocksWriteBatch) -> anyhow::Result<()>
) -> anyhow::Result<()> {
    purge_deleted_table_impl(db, id, retain_reclaim_metadata, batch_size_limit, write_batch)
}

/// Physically reclaim disk by unlinking whole `CF_TABLES` SST files below the live
/// watermark ([`min_live_table_id`]). Table ids are time-ordered UUIDv7s, so dead tables
/// form a contiguous low range; boundary, level-0 and above-watermark garbage is left to
/// compaction.
///
/// Needs no scratch space, where compaction must write its merged output before dropping
/// the inputs and so deadlocks on a full disk. Not literally write-free: RocksDB appends a
/// `VersionEdit` to the MANIFEST, a few hundred bytes against gigabytes freed.
///
/// SAFETY: the unlink IGNORES snapshots -- it can break an in-flight query reading a
/// just-deleted table below the watermark. So it runs only at STARTUP, before any
/// controller/query exists. Do not use this path for runtime disk pressure unless query
/// semantics explicitly permit invalidating live readers; use snapshot-aware runtime reclaim
/// otherwise. Getting here at all requires the database to have opened, and opening replays
/// the WAL and flushes it to L0.
pub(crate) fn reclaim_disk_space(db: &RocksDB) -> anyhow::Result<()> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let hi = reclaim_upper_bound(min_live_table_id(db)?);
    db.delete_file_in_range_cf(cf_tables, RECLAIM_LOWER_BOUND.as_slice(), hi.as_slice())?;
    Ok(())
}

const NUM_SNAPSHOTS_PROPERTY: &str = "rocksdb.num-snapshots";
const OLDEST_SNAPSHOT_SEQUENCE_PROPERTY: &str = "rocksdb.oldest-snapshot-sequence";

/// Runtime whole-file reclaim guarded by RocksDB's MVCC sequence horizon.
///
/// A purged table pins the watermark while any snapshot can predate its point tombstones.
/// Once the oldest live snapshot reaches the recorded purge sequence, that table is safe
/// forever: future snapshots can only be newer. Continuous query traffic therefore does
/// not require a moment with zero snapshots. A genuinely long-lived snapshot intentionally
/// pins physical deletion; storage does not expire a live query behind its caller's back.
pub(crate) fn reclaim_disk_space_runtime(
    db: &RocksDB,
    build_fence: ReclaimFence
) -> anyhow::Result<RuntimeReclaimResult> {
    // One RocksDB snapshot makes the three bookkeeping scans a consistent view. It is
    // dropped before reading the safety horizon, so it does not count as a live reader.
    let metadata = db.snapshot();
    // TODO: Cache the per-dataset minimum committed table id if this full chunk scan shows
    // up in profiles. It is snapshot-consistent and no longer runs under a write gate.
    let live_scan = min_committed_table_id_at(db, &metadata)?;
    let live = live_scan.min;
    let (dirty, _) = scan_table_ids_at(db, &metadata, CF_DIRTY_TABLES)?;
    let deleted = scan_deleted_tables_at(db, &metadata)?;
    drop(metadata);

    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let snapshot_count = db
        .property_int_value_cf(cf_tables, NUM_SNAPSHOTS_PROPERTY)?
        .context("RocksDB did not expose rocksdb.num-snapshots")?;
    let oldest_snapshot_sequence = if snapshot_count == 0 {
        None
    } else {
        Some(
            db.property_int_value_cf(cf_tables, OLDEST_SNAPSHOT_SEQUENCE_PROPERTY)?
                .context("RocksDB did not expose rocksdb.oldest-snapshot-sequence")?
        )
    };

    let mut safe = Vec::new();
    let mut unsafe_watermark = None;
    let mut unsafe_deleted_tables = 0usize;
    for (id, state) in deleted.records {
        if state.is_reclaim_safe(oldest_snapshot_sequence) {
            safe.push(id);
        } else {
            unsafe_watermark = watermark(unsafe_watermark, Some(id));
            unsafe_deleted_tables += 1;
        }
    }

    // A build active when the fence was taken remains pinned even if it finishes between
    // fence creation and the metadata snapshot. Builds registered later have ids strictly
    // above the process-monotonic boundary. Neither class needs to retain a lock here.
    let watermark = runtime_reclaim_watermark(live, dirty, unsafe_watermark, build_fence);
    let hi = reclaim_upper_bound(watermark);
    db.delete_file_in_range_cf(cf_tables, RECLAIM_LOWER_BOUND.as_slice(), hi.as_slice())?;

    // Safe is monotonic: no future snapshot can predate one that already passed the purge
    // sequence, so these records need not pin later passes even if no SST was eligible now.
    if !safe.is_empty() {
        let cf_deleted = db.cf_handle(CF_DELETED_TABLES).unwrap();
        let mut batch = RocksWriteBatch::default();
        for id in &safe {
            batch.delete_cf(cf_deleted, id);
        }
        db.write(batch)?;
    }

    Ok(RuntimeReclaimResult {
        snapshot_count,
        oldest_snapshot_sequence,
        safe_deleted_tables: safe.len(),
        unsafe_deleted_tables,
        skipped_malformed_chunks: live_scan.malformed,
        watermark
    })
}

fn runtime_reclaim_watermark(
    live: Option<TableId>,
    dirty: impl IntoIterator<Item = TableId>,
    unsafe_deleted: Option<TableId>,
    build_fence: ReclaimFence
) -> Option<TableId> {
    watermark(
        live,
        dirty
            .into_iter()
            .chain(unsafe_deleted)
            .chain(build_fence.active_build_watermark)
            .chain(Some(build_fence.boundary))
    )
}

/// Crash recovery -- purge orphaned `CF_DIRTY_TABLES` markers.
///
/// A dirty marker is written when a build starts and removed when its chunk commits.
/// One left by a build that died before commit counts as live in [`min_live_table_id`],
/// pinning [`reclaim_disk_space`]'s watermark forever. We drop the marker and
/// point-delete its unreferenced data.
///
/// MUST run only with no build in flight (e.g. startup before ingest): it treats EVERY
/// dirty marker as an orphan, so it would tombstone a live build's data. Returns orphans
/// purged.
///
/// TODO: If restart-free recovery becomes necessary, extend in-flight ownership through
/// chunk commit or add durable build state. Marker age plus the builder registry is not
/// sufficient: a finished table can still be waiting for its chunk transaction. Until then,
/// every startup attempts recovery before ingest; failures are logged and retried at the
/// next startup.
pub(crate) fn purge_orphan_dirty_tables(db: &RocksDB) -> anyhow::Result<usize> {
    // Collect first, mutate after: writing while iterating disturbs the cursor.
    let (orphans, malformed) = scan_table_ids(db, CF_DIRTY_TABLES)?;
    drop_malformed_keys(db, CF_DIRTY_TABLES, &malformed)?;

    for id in &orphans {
        purge_orphan_table(db, id)?;
    }

    Ok(orphans.len())
}

/// See [`crate::db::reclaim::watermark`].
///
/// An undecodable `CF_CHUNKS` value aborts with `Err` rather than being skipped, since
/// skipping could lift the watermark over live data. The watermark is global, so one bad
/// chunk anywhere disables the unlink for all datasets -- the safe failure mode.
fn min_live_table_id(db: &RocksDB) -> anyhow::Result<Option<TableId>> {
    let min = min_committed_table_id(db)?;

    // Malformed dirty keys pin no data, and the startup purge deletes them anyway. This
    // path must not write -- it serves reclaim on a disk with no room for writes.
    let (dirty, _) = scan_table_ids(db, CF_DIRTY_TABLES)?;

    Ok(watermark(min, dirty))
}

fn min_committed_table_id(db: &RocksDB) -> anyhow::Result<Option<TableId>> {
    let cf_chunks = db.cf_handle(CF_CHUNKS).unwrap();
    let mut it = db.raw_iterator_cf(cf_chunks);
    Ok(collect_min_committed_table_id(&mut it, MalformedChunkPolicy::Abort)?.min)
}

fn min_committed_table_id_at(
    db: &RocksDB,
    snapshot: &RocksSnapshot<'_, RocksDB>
) -> anyhow::Result<CommittedTableScan> {
    let cf_chunks = db.cf_handle(CF_CHUNKS).unwrap();
    let mut it = snapshot.raw_iterator_cf(cf_chunks);
    collect_min_committed_table_id(&mut it, MalformedChunkPolicy::Skip)
}

#[derive(Clone, Copy)]
enum MalformedChunkPolicy {
    Abort,
    Skip
}

#[derive(Default)]
struct CommittedTableScan {
    min: Option<TableId>,
    malformed: usize
}

fn collect_min_committed_table_id(
    it: &mut RocksIterator<'_, RocksDB>,
    malformed_policy: MalformedChunkPolicy
) -> anyhow::Result<CommittedTableScan> {
    let mut min = None;
    let mut malformed = 0usize;
    it.seek_to_first();
    while it.valid() {
        match borsh::from_slice::<Chunk>(it.value().unwrap()) {
            Ok(chunk) => min = watermark(min, chunk.tables().values().copied()),
            Err(err) => match malformed_policy {
                MalformedChunkPolicy::Abort => return Err(err).context("invalid committed chunk"),
                MalformedChunkPolicy::Skip => malformed = malformed.saturating_add(1)
            }
        }
        it.next();
    }
    it.status()?;
    Ok(CommittedTableScan { min, malformed })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> TableId {
        TableId::try_from_key(&[byte; 16]).unwrap()
    }

    #[test]
    fn runtime_watermark_never_crosses_the_build_fence() {
        let fence = ReclaimFence {
            active_build_watermark: Some(id(5)),
            boundary: id(7)
        };

        assert_eq!(runtime_reclaim_watermark(None, None, None, fence), Some(id(5)));
        assert_eq!(
            runtime_reclaim_watermark(Some(id(3)), [id(8)], None, fence),
            Some(id(3))
        );

        let fence = ReclaimFence {
            active_build_watermark: None,
            boundary: id(7)
        };
        assert_eq!(
            runtime_reclaim_watermark(Some(id(9)), [id(8)], None, fence),
            Some(id(7))
        );
    }

    #[test]
    fn full_tombstone_batches_are_taken_for_flushing() {
        let empty_batch_size = RocksWriteBatch::default().size_in_bytes();
        let mut batch = RocksWriteBatch::default();
        batch.put(b"key", vec![0u8; WRITE_BATCH_SIZE_LIMIT]);

        let full = take_full_tombstone_batch(&mut batch, WRITE_BATCH_SIZE_LIMIT);
        assert!(full.is_some());
        assert_eq!(batch.size_in_bytes(), empty_batch_size);
    }
}
