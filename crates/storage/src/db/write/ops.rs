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
/// For each table routed to `CF_DELETED_TABLES` (by [`super::tx::Tx::delete_table`]),
/// point-deletes every one of its `CF_TABLES` keys, then drops its bookkeeping entry.
/// The deletes are MVCC-versioned, so in-flight queries holding an older snapshot are
/// unaffected and no grace period is needed; the space is freed later by compaction
/// (the file unlink is startup-only).
///
/// Point deletes -- not one `delete_range` tombstone -- because range deletions are not
/// officially supported on the transactional `OptimisticTransactionDB`. The cost (one
/// tombstone per key) is bounded: cleanup runs only at startup and on a background
/// tick, never on a query path.
///
/// Idempotent (a crash just replays now-no-op point deletes). Returns tables purged.
pub(crate) fn logical_cleanup(db: &RocksDB) -> anyhow::Result<usize> {
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

    // One write batch per table (not one shared batch for all): a large multi-table
    // purge then never holds every table's point deletes in memory at once.
    for id in &pending {
        purge_table(db, CF_DELETED_TABLES, id)?;
    }

    Ok(pending.len())
}

/// Point-delete all of `id`'s `CF_TABLES` data and drop its `bookkeeping_cf` entry
/// (`CF_DELETED_TABLES` for [`logical_cleanup`], `CF_DIRTY_TABLES` for
/// [`purge_orphan_dirty_tables`]) in a single per-table write batch. Enumerates the
/// table's key range `[start, end)` and deletes each key.
///
/// Point deletes rather than one `delete_range` tombstone: range deletions are not
/// officially supported on the transactional `OptimisticTransactionDB`. One batch per
/// table (as the pre-tombstone code did) keeps a large purge's memory bounded.
fn purge_table(db: &RocksDB, bookkeeping_cf: &str, id: &TableId) -> anyhow::Result<()> {
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let cf_bookkeeping = db.cf_handle(bookkeeping_cf).unwrap();

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

    batch.delete_cf(cf_bookkeeping, id);
    db.write(batch)?;

    Ok(())
}

/// Physically reclaim disk by unlinking whole `CF_TABLES` SST files entirely below
/// the live watermark (smallest live `TableId`; see [`min_live_table_id`]). The only
/// reclaim that frees space *without writing*, so it works even at a full disk where
/// compaction deadlocks. Table ids are never-reused, time-ordered UUIDv7s, so dead
/// tables form a contiguous low range; boundary and above-watermark garbage are left
/// to compaction (which Phase-1 [`logical_cleanup`] point deletes let it drop).
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
/// the marker and point-delete its (unreferenced) `CF_TABLES` data.
///
/// MUST run only with no build in flight (e.g. startup before ingest): it treats
/// EVERY dirty marker as an orphan, so it would tombstone a live build's data. An
/// orphan from a mid-run crash survives until the next restart's purge -- it only
/// blunts the startup reclaim, never corrupts data. Returns orphans purged.
pub(crate) fn purge_orphan_dirty_tables(db: &RocksDB) -> anyhow::Result<usize> {
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

    for id in &orphans {
        purge_table(db, CF_DIRTY_TABLES, id)?;
    }

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
