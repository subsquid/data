use crate::{
    db::{
        db::{RocksDB, RocksWriteBatch, CF_CHUNKS, CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TABLES},
        table_id::TableId,
        Chunk
    },
    table::key::TableKeyFactory
};

/// Phase 1 -- logical, snapshot-safe purge of deleted tables.
///
/// Replaces each table routed to `CF_DELETED_TABLES` (by [`super::tx::Tx::delete_table`])
/// with one `CF_TABLES` range tombstone instead of millions of point deletes, then
/// drops its bookkeeping entry. Range tombstones respect snapshots, so in-flight
/// queries are unaffected and no grace period is needed; the space is freed later by
/// compaction -- the whole runtime reclaim path (the file unlink is startup-only).
///
/// Idempotent (a crash just replays the no-op range delete). Returns tables purged.
pub(crate) fn logical_cleanup(db: &RocksDB) -> anyhow::Result<usize> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let cf_deleted = db.cf_handle(CF_DELETED_TABLES).unwrap();

    // Collect first, then mutate: writing mid-iteration disturbs the cursor.
    let mut pending: Vec<TableId> = Vec::new();
    {
        let mut it = db.raw_iterator_cf(cf_deleted);
        it.seek_to_first();
        while it.valid() {
            // Skip malformed keys instead of panicking -- a panic re-fires every tick.
            if let Some(id) = TableId::try_from_slice(it.key().unwrap()) {
                pending.push(id);
            }
            it.next();
        }
        it.status()?;
    }

    if pending.is_empty() {
        return Ok(0);
    }

    // Range delete writes straight to the base DB (the txn batch has no range
    // delete), so it runs here, not in `delete_table`'s transaction. Dropping the
    // bookkeeping entry after a crash just replays a harmless no-op range delete.
    let mut batch = RocksWriteBatch::default();
    for id in &pending {
        let mut start = TableKeyFactory::new(id);
        let mut end = TableKeyFactory::new(id);
        db.delete_range_cf(cf_tables, start.start(), end.end())?;
        batch.delete_cf(cf_deleted, id);
    }
    db.write(batch)?;

    Ok(pending.len())
}

/// Physically reclaim disk by unlinking whole `CF_TABLES` SST files entirely below
/// the live watermark (smallest live `TableId`; see [`min_live_table_id`]). The only
/// reclaim that frees space *without writing*, so it works even at a full disk where
/// compaction deadlocks. Table ids are never-reused, time-ordered UUIDv7s, so dead
/// tables form a contiguous low range; boundary and above-watermark garbage are left
/// to compaction (which Phase-1 [`logical_cleanup`] tombstones let it drop).
///
/// SAFETY: the unlink IGNORES snapshots -- it can break an in-flight query reading a
/// just-deleted table below the watermark. So it runs only at STARTUP, before any
/// controller/query exists. FUTURE: also trigger under runtime disk pressure (the
/// emergency reclaim compaction can't do at a full disk), accepting that read risk.
pub(crate) fn reclaim_disk_space(db: &RocksDB) -> anyhow::Result<()> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let watermark = min_live_table_id(db)?;

    let lo = [0u8; 16];
    let hi: Vec<u8> = match watermark {
        Some(b) => b.as_ref().to_vec(),
        // No live tables: 17x 0xFF sorts above every `id(16B) ++ suffix` key, so
        // every dead file (including orphans) is unlinked.
        None => vec![0xFFu8; 17]
    };
    db.delete_file_in_range_cf(cf_tables, &lo[..], &hi[..])?;

    Ok(())
}

/// Crash recovery -- purge orphaned `CF_DIRTY_TABLES` markers.
///
/// A dirty marker is written when a build starts and removed when its chunk commits
/// (see [`super::tx::Tx::write_chunk`]). One still present with no build running is an
/// orphan from a build that died before commit; [`min_live_table_id`] counts it as
/// live, so a single orphan pins [`reclaim_disk_space`]'s watermark forever. We drop
/// the marker and range-tombstone its (unreferenced) `CF_TABLES` data.
///
/// MUST run only with no build in flight (e.g. startup before ingest): it treats
/// EVERY dirty marker as an orphan, so it would tombstone a live build's data. An
/// orphan from a mid-run crash survives until the next restart's purge -- it only
/// blunts the startup reclaim, never corrupts data. Returns orphans purged.
pub(crate) fn purge_orphan_dirty_tables(db: &RocksDB) -> anyhow::Result<usize> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let cf_dirty = db.cf_handle(CF_DIRTY_TABLES).unwrap();

    // Collect first, mutate after: writing while iterating disturbs the cursor.
    let mut orphans: Vec<TableId> = Vec::new();
    {
        let mut it = db.raw_iterator_cf(cf_dirty);
        it.seek_to_first();
        while it.valid() {
            // Skip a malformed key rather than panic (see logical_cleanup).
            if let Some(id) = TableId::try_from_slice(it.key().unwrap()) {
                orphans.push(id);
            }
            it.next();
        }
        it.status()?;
    }

    if orphans.is_empty() {
        return Ok(0);
    }

    let mut batch = RocksWriteBatch::default();
    for id in &orphans {
        let mut start = TableKeyFactory::new(id);
        let mut end = TableKeyFactory::new(id);
        db.delete_range_cf(cf_tables, start.start(), end.end())?;
        batch.delete_cf(cf_dirty, id);
    }
    db.write(batch)?;

    Ok(orphans.len())
}

/// Smallest `TableId` still live: referenced by a committed chunk (`CF_CHUNKS`) or
/// pending in `CF_DIRTY_TABLES` (built but not yet in a chunk). `None` if none exist.
/// Taken across ALL datasets, which also absorbs inter-dataset UUIDv7 clock skew.
///
/// A `CF_CHUNKS` value that fails to decode aborts with `Err` (not skipped): skipping
/// could omit a live table and lift the watermark over data a query needs, so the
/// caller ([`reclaim_disk_space`]) reclaims nothing this run -- the safe failure mode.
fn min_live_table_id(db: &RocksDB) -> anyhow::Result<Option<TableId>> {
    let mut min: Option<TableId> = None;

    {
        let cf_chunks = db.cf_handle(CF_CHUNKS).unwrap();
        let mut it = db.raw_iterator_cf(cf_chunks);
        it.seek_to_first();
        while it.valid() {
            let chunk: Chunk = borsh::from_slice(it.value().unwrap())?;
            for id in chunk.tables().values() {
                min = Some(min.map_or(*id, |m| m.min(*id)));
            }
            it.next();
        }
        it.status()?;
    }

    {
        let cf_dirty = db.cf_handle(CF_DIRTY_TABLES).unwrap();
        let mut it = db.raw_iterator_cf(cf_dirty);
        it.seek_to_first();
        while it.valid() {
            // Skip a malformed key rather than panic (see logical_cleanup).
            if let Some(id) = TableId::try_from_slice(it.key().unwrap()) {
                min = Some(min.map_or(id, |m| m.min(id)));
            }
            it.next();
        }
        it.status()?;
    }

    Ok(min)
}
