//! [`MockDB`] -- a scratch temp-dir `Database` for driving the table cleanup
//! lifecycle in tests: logical purge (Phase 1, range tombstones) + physical
//! SST-file unlink below the live watermark.
//!
//! It hides the fiddly setup (auto-compaction off, small write buffers, UUIDv7 id
//! ordering, flush/compact to the bottom level) so a test reads as domain steps --
//! commit / delete / cleanup / reclaim / snapshot / purge -- asserting on observable
//! effects (rows read, files freed).

use std::{sync::Arc, time::Duration};

use arrow::datatypes::{DataType, Schema};
use sqd_storage::{
    db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind, ReadSnapshot, TableId},
    table::write::{use_small_buffers, RestoreBufferSizesGuard}
};
use tempfile::TempDir;

use crate::utils::{make_irregular_block, make_schema, read_chunk};

/// Generous upper bound on the total number of rows a single test builds across
/// all its tables (block numbers double as indices into the shared column data).
const CAPACITY: usize = 20_000;

fn two_u16_columns(n: usize) -> Vec<Vec<u16>> {
    let col: Vec<u16> = (0..n).map(|i| (i % 60_000) as u16).collect();
    vec![col.clone(), col]
}

/// A table [`MockDB`] built. Holds enough to delete it, read it back, and
/// reason about its watermark position (`id`).
pub struct Table {
    pub chunk: Chunk,
    pub id: TableId,
    pub rows: Vec<(u32, u32)>
}

/// A scratch `Database` wired for deterministic cleanup/reclaim tests: auto-compaction
/// off (only explicit `compact`/unlink move data) and small write buffers (so modest
/// row counts still produce real SST files).
///
/// Invariant: tables are created with strictly increasing ids in call order (a 2 ms gap
/// per build advances the UUIDv7 timestamp), so "created first" == "smaller id". Tests
/// rely on this to place an orphan/older table below a live one without reading raw ids.
pub struct MockDB {
    db: Database,
    ds: DatasetId,
    schema: Arc<Schema>,
    static_data: Vec<Vec<u16>>,
    next_block: usize,
    last_id: Option<TableId>,
    _small_buffers: RestoreBufferSizesGuard,
    // Keeps the temp dir alive: flush/reclaim create new WAL/SST files, which
    // fails once the directory is cleaned up.
    _dir: TempDir
}

impl MockDB {
    pub fn new() -> Self {
        Self::open(
            DatabaseSettings::default()
                .with_rocksdb_stats(true)
                .with_auto_compactions(false)
        )
    }

    /// Like [`MockDB::new`] but with the block cache disabled, so reads must
    /// hit the SST files. Lets a test observe data loss deterministically: once
    /// files are unlinked the data is genuinely gone, never served from cache.
    pub fn uncached() -> Self {
        Self::open(
            DatabaseSettings::default()
                .with_rocksdb_stats(true)
                .with_auto_compactions(false)
                .with_data_cache_size(0)
                .with_chunk_cache_size(0)
        )
    }

    fn open(settings: DatabaseSettings) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = settings.open(dir.path()).unwrap();
        let ds = DatasetId::from_str("solana");
        db.create_dataset(ds, DatasetKind::from_str("solana")).unwrap();
        Self {
            db,
            ds,
            schema: make_schema(DataType::UInt32, DataType::UInt32, false),
            static_data: two_u16_columns(CAPACITY),
            next_block: 0,
            last_id: None,
            _small_buffers: use_small_buffers(),
            _dir: dir
        }
    }

    /// Build (and `finish`) a table of `rows` rows over the next free block
    /// range. `finish` already persists the dirty marker and the table data;
    /// the caller decides whether to commit the chunk.
    fn build_table(&mut self, rows: usize) -> (Chunk, Vec<(u32, u32)>, TableId) {
        // Sleep so the next UUIDv7 timestamp advances, keeping creation order == id
        // order (the struct invariant the watermark tests rely on).
        std::thread::sleep(Duration::from_millis(2));

        let start = self.next_block;
        let end = start + rows;
        assert!(end <= self.static_data[0].len(), "harness data capacity exceeded");
        self.next_block = end;

        let (chunk, rows_data) =
            make_irregular_block(&self.static_data, start, end, Arc::clone(&self.schema), &self.db);
        let id = chunk.tables().get("block").copied().unwrap();
        if let Some(last) = self.last_id {
            assert!(id > last, "harness invariant: ids must increase with creation order");
        }
        self.last_id = Some(id);
        (chunk, rows_data, id)
    }

    /// Build a committed table: its chunk is inserted, so `write_chunk` removes
    /// its dirty marker.
    pub fn commit_table(&mut self, rows: usize) -> Table {
        let (chunk, rows, id) = self.build_table(rows);
        self.db.insert_chunk(self.ds, &chunk).unwrap();
        Table { chunk, id, rows }
    }

    /// Build a table but never commit its chunk -- `build_table`'s `finish` has
    /// already persisted the dirty marker and data, so nothing ever removes the
    /// marker. Exactly the orphan a crash-before-commit leaves behind.
    pub fn orphan_table(&mut self, rows: usize) -> Table {
        let (chunk, rows, id) = self.build_table(rows);
        Table { chunk, id, rows }
    }

    pub fn snapshot(&self) -> ReadSnapshot<'_> {
        self.db.snapshot()
    }

    pub fn read(&self, snapshot: &ReadSnapshot, table: &Table) -> Vec<(u32, u32)> {
        read_chunk(snapshot, table.chunk.clone())
    }

    /// Like [`MockDB::read`] but propagates errors instead of unwrapping --
    /// used to show that an unlink under a live snapshot makes the data
    /// unreadable rather than silently wrong.
    pub fn try_read(&self, snapshot: &ReadSnapshot, table: &Table) -> anyhow::Result<()> {
        let chunk_reader = snapshot.create_chunk_reader(table.chunk.clone());
        let table_reader = chunk_reader.get_table_reader("block")?;
        table_reader.read_column(0, None)?;
        table_reader.read_column(1, None)?;
        Ok(())
    }

    /// Whether a fresh snapshot still sees any committed chunk in the dataset.
    pub fn has_visible_chunk(&self) -> bool {
        self.db.snapshot().get_first_chunk(self.ds).unwrap().is_some()
    }

    pub fn delete(&self, table: &Table) {
        self.db
            .update_dataset(self.ds, |tx| tx.delete_chunk(&table.chunk))
            .unwrap();
    }

    /// Delete the whole dataset (runs Phase 1 synchronously), as production's
    /// `delete_dataset` does.
    pub fn delete_dataset(&self) {
        self.db.delete_dataset(self.ds).unwrap();
    }

    /// Whether the database has no datasets left.
    pub fn has_no_datasets(&self) -> bool {
        self.db.get_all_datasets().unwrap().is_empty()
    }

    pub fn cleanup(&self) -> usize {
        self.db.cleanup().unwrap()
    }

    /// Physically unlink dead SST files below the live watermark. The caller is
    /// responsible for there being no live pre-deletion reader (as at startup in
    /// production). Assert effects via [`MockDB::sst_size`].
    pub fn reclaim(&self) {
        self.db.reclaim_disk_space().unwrap()
    }

    pub fn purge_orphans(&self) -> usize {
        self.db.purge_orphan_dirty_tables().unwrap()
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    /// Land the table data in the bottom level, so a later reclaim is a real
    /// file unlink (`DeleteFilesInRange` skips L0): flush the memtable, then
    /// compact. In production the dead data being reclaimed lives there too.
    pub fn compact_to_bottom(&self) {
        self.db.flush().unwrap();
        self.db.compact_tables();
    }

    /// Total size of all SST files in the table-data column family, in bytes.
    pub fn sst_size(&self) -> u64 {
        self.db
            .get_property("TABLES", "rocksdb.total-sst-files-size")
            .unwrap()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }
}
