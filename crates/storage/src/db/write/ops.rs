use crate::{
    db::{
        db::{RocksDB, RocksWriteBatch, CF_CHUNKS, CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TABLES},
        reclaim::{reclaim_upper_bound, watermark, RECLAIM_LOWER_BOUND},
        table_id::TableId,
        Chunk
    },
    table::key::TableKeyFactory
};

/// Phase 1 -- logical, snapshot-safe purge of deleted tables.
///
/// Point-deletes the `CF_TABLES` keys of every table in `CF_DELETED_TABLES`, then drops
/// its bookkeeping entry. The deletes are MVCC-versioned, so in-flight queries are
/// unaffected; the space is freed later by compaction. Idempotent. Returns tables purged.
pub(crate) fn logical_cleanup(db: &RocksDB) -> anyhow::Result<usize> {
    // Collect first, then mutate: writing mid-iteration disturbs the cursor.
    let (pending, malformed) = scan_table_ids(db, CF_DELETED_TABLES)?;
    drop_malformed_keys(db, CF_DELETED_TABLES, &malformed)?;

    for id in &pending {
        purge_table(db, id)?;
    }

    Ok(pending.len())
}

/// Scan `cf`, splitting keys into well-formed `TableId`s and malformed leftovers.
/// Malformed keys are returned rather than skipped, so callers can delete them via
/// [`drop_malformed_keys`] -- left in place, every pass would rescan them forever.
fn scan_table_ids(db: &RocksDB, cf: &str) -> anyhow::Result<(Vec<TableId>, Vec<Vec<u8>>)> {
    let cf = db.cf_handle(cf).unwrap();
    let mut ids = Vec::new();
    let mut malformed = Vec::new();

    let mut it = db.raw_iterator_cf(cf);
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
    let cf = db.cf_handle(cf).unwrap();
    let mut batch = RocksWriteBatch::default();
    for key in keys {
        batch.delete_cf(cf, key);
    }
    db.write(batch)?;
    Ok(())
}

/// Point-delete all of `id`'s `CF_TABLES` data and drop its bookkeeping entries. One write
/// batch per table, so a large purge never holds every table's deletes in memory at once.
///
/// A table is normally in only one bookkeeping CF, but clearing both costs one tombstone
/// and guarantees a purge never leaves a stale marker pinning the watermark.
///
/// Point deletes rather than a single `delete_range` tombstone: range deletions are not
/// officially supported on the transactional `OptimisticTransactionDB`.
fn purge_table(db: &RocksDB, id: &TableId) -> anyhow::Result<()> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let cf_deleted = db.cf_handle(CF_DELETED_TABLES).unwrap();
    let cf_dirty = db.cf_handle(CF_DIRTY_TABLES).unwrap();

    let mut start = TableKeyFactory::new(id);
    let mut end = TableKeyFactory::new(id);
    let start_key = start.start();
    let end_key = end.end();

    let mut batch = RocksWriteBatch::default();
    let mut it = db.raw_iterator_cf(cf_tables);
    it.seek(start_key);
    while it.valid() {
        let key = it.key().unwrap();
        if key >= end_key {
            break;
        }
        batch.delete_cf(cf_tables, key);
        it.next();
    }
    it.status()?;

    batch.delete_cf(cf_deleted, id);
    batch.delete_cf(cf_dirty, id);
    db.write(batch)?;

    Ok(())
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
/// controller/query exists. FUTURE: trigger under runtime disk pressure too, accepting that
/// read risk. That, not this, is the answer to a genuinely full disk -- getting here at all
/// requires the database to have opened, and opening replays the WAL and flushes it to L0.
pub(crate) fn reclaim_disk_space(db: &RocksDB) -> anyhow::Result<()> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let hi = reclaim_upper_bound(min_live_table_id(db)?);
    db.delete_file_in_range_cf(cf_tables, RECLAIM_LOWER_BOUND.as_slice(), hi.as_slice())?;
    Ok(())
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
pub(crate) fn purge_orphan_dirty_tables(db: &RocksDB) -> anyhow::Result<usize> {
    // Collect first, mutate after: writing while iterating disturbs the cursor.
    let (orphans, malformed) = scan_table_ids(db, CF_DIRTY_TABLES)?;
    drop_malformed_keys(db, CF_DIRTY_TABLES, &malformed)?;

    for id in &orphans {
        purge_table(db, id)?;
    }

    Ok(orphans.len())
}

/// See [`crate::db::reclaim::watermark`].
///
/// An undecodable `CF_CHUNKS` value aborts with `Err` rather than being skipped, since
/// skipping could lift the watermark over live data. The watermark is global, so one bad
/// chunk anywhere disables the unlink for all datasets -- the safe failure mode.
fn min_live_table_id(db: &RocksDB) -> anyhow::Result<Option<TableId>> {
    let mut min: Option<TableId> = None;
    {
        let cf_chunks = db.cf_handle(CF_CHUNKS).unwrap();
        let mut it = db.raw_iterator_cf(cf_chunks);
        it.seek_to_first();
        while it.valid() {
            let chunk: Chunk = borsh::from_slice(it.value().unwrap())?;
            min = watermark(min, chunk.tables().values().copied());
            it.next();
        }
        it.status()?;
    }

    // Malformed dirty keys pin no data, and the startup purge deletes them anyway. This
    // path must not write -- it serves reclaim on a disk with no room for writes.
    let (dirty, _) = scan_table_ids(db, CF_DIRTY_TABLES)?;

    Ok(watermark(min, dirty))
}
