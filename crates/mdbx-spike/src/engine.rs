//! The two engines under test, behind one byte-KV surface.
//!
//! mdbx merge/delete run inside a single write transaction on purpose: that is
//! the worst case ADR 0002 worries about (writer lock held for a whole
//! chunk-merge), so the bench must charge for it.

use std::{fs, path::Path};

use anyhow::Result;
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, PageSize, ReadWriteOptions, SyncMode, WriteFlags};
use sqd_storage::kv::{KvRead, KvReadCursor, KvWrite};

use crate::kv_mdbx::{MdbxTableReader, MdbxTableWriter};

pub const TABLE_PREFIX_LEN: usize = 16;

pub fn table_prefix(id: u64) -> [u8; TABLE_PREFIX_LEN] {
    (id as u128).to_be_bytes()
}

pub fn meta_key(id: u64) -> Vec<u8> {
    let mut k = table_prefix(id).to_vec();
    k.push(0);
    k
}

pub fn page_key(id: u64, idx: u32) -> Vec<u8> {
    let mut k = table_prefix(id).to_vec();
    k.push(3);
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

pub fn is_page_key(key: &[u8]) -> bool {
    key.len() == TABLE_PREFIX_LEN + 5
}

pub struct DiskUsage {
    pub file_bytes: u64,
    pub live_bytes: u64
}

pub enum Engine {
    Mdbx(MdbxEngine),
    Rocks(RocksEngine)
}

impl Engine {
    pub fn write_chunk(&self, ds: usize, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        match self {
            Engine::Mdbx(e) => e.write_chunk(ds, entries),
            Engine::Rocks(e) => e.write_chunk(entries)
        }
    }

    pub fn merge_tables(&self, ds: usize, old: &[u64], new_id: u64) -> Result<usize> {
        match self {
            Engine::Mdbx(e) => e.merge_tables(ds, old, new_id),
            Engine::Rocks(e) => e.merge_tables(old, new_id)
        }
    }

    pub fn delete_tables(&self, ds: usize, ids: &[u64]) -> Result<usize> {
        match self {
            Engine::Mdbx(e) => e.delete_tables(ds, ids),
            Engine::Rocks(e) => e.delete_tables(ids)
        }
    }

    pub fn hold_reader(&self, secs: u64) -> Result<()> {
        match self {
            Engine::Mdbx(e) => {
                let txn = e.envs[0].begin_ro_txn()?;
                std::thread::sleep(std::time::Duration::from_secs(secs));
                drop(txn);
            }
            Engine::Rocks(e) => {
                let snap = e.db.snapshot();
                std::thread::sleep(std::time::Duration::from_secs(secs));
                drop(snap);
            }
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        match self {
            Engine::Mdbx(e) => {
                for env in &e.envs {
                    env.sync(true)?;
                }
            }
            Engine::Rocks(e) => e.db.flush()?
        }
        Ok(())
    }

    pub fn disk_usage(&self) -> Result<DiskUsage> {
        match self {
            Engine::Mdbx(e) => e.disk_usage(),
            Engine::Rocks(e) => e.disk_usage()
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Engine::Mdbx(e) => {
                let info = e.envs[0].info().map(|i| i.last_pgno()).unwrap_or(0);
                let freelist = e.envs[0].freelist().unwrap_or(0);
                format!(
                    "mdbx envs={} page_size={} env0: last_pgno={} freelist_pages={}",
                    e.envs.len(),
                    e.page_size,
                    info,
                    freelist
                )
            }
            Engine::Rocks(e) => {
                let total = e.int_prop("rocksdb.total-sst-files-size");
                let live = e.int_prop("rocksdb.live-sst-files-size");
                format!("rocks total_sst={total} live_sst={live}")
            }
        }
    }
}

pub struct MdbxEngine {
    envs: Vec<Database<NoWriteMap>>,
    page_size: usize,
    root: std::path::PathBuf
}

impl MdbxEngine {
    pub fn open(root: &Path, n_envs: usize, sync_mode: SyncMode, page_size: usize, max_bytes: usize) -> Result<Self> {
        let mut envs = Vec::with_capacity(n_envs);
        for i in 0..n_envs {
            let path = root.join(format!("env{i}"));
            fs::create_dir_all(&path)?;
            let options = DatabaseOptions {
                page_size: Some(PageSize::Set(page_size)),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    sync_mode,
                    min_size: Some(0),
                    max_size: Some(max_bytes as isize),
                    growth_step: Some(256 << 20),
                    shrink_threshold: None
                }),
                ..Default::default()
            };
            envs.push(Database::open_with_options(&path, options)?);
        }
        Ok(Self {
            envs,
            page_size,
            root: root.to_owned()
        })
    }

    fn env(&self, ds: usize) -> &Database<NoWriteMap> {
        &self.envs[ds % self.envs.len()]
    }

    fn write_chunk(&self, ds: usize, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let txn = self.env(ds).begin_rw_txn()?;
        let table = txn.open_table(None)?;
        {
            let mut writer = MdbxTableWriter::new(&txn, table);
            for (k, v) in entries {
                writer.put(k, v)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn merge_tables(&self, ds: usize, old: &[u64], new_id: u64) -> Result<usize> {
        let txn = self.env(ds).begin_rw_txn()?;
        let table = txn.open_table(None)?;

        let mut moved: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            let mut cur = txn.cursor(&table)?;
            for id in old {
                let prefix = table_prefix(*id);
                let mut kv = cur.set_range::<Vec<u8>, Vec<u8>>(&prefix)?;
                while let Some((k, v)) = kv {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    moved.push((k, v));
                    kv = cur.next::<Vec<u8>, Vec<u8>>()?;
                }
            }
        }

        let mut idx = 0u32;
        for (k, v) in &moved {
            if is_page_key(k) {
                txn.put(&table, page_key(new_id, idx), v, WriteFlags::UPSERT)?;
                idx += 1;
            }
        }
        txn.put(&table, meta_key(new_id), [0u8; 64], WriteFlags::UPSERT)?;
        for (k, _v) in &moved {
            txn.del(&table, k, None)?;
        }
        txn.commit()?;
        Ok(moved.len())
    }

    fn delete_tables(&self, ds: usize, ids: &[u64]) -> Result<usize> {
        let txn = self.env(ds).begin_rw_txn()?;
        let table = txn.open_table(None)?;

        let mut keys: Vec<Vec<u8>> = Vec::new();
        {
            let mut cur = txn.cursor(&table)?;
            for id in ids {
                let prefix = table_prefix(*id);
                let mut kv = cur.set_range::<Vec<u8>, ()>(&prefix)?;
                while let Some((k, ())) = kv {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    keys.push(k);
                    kv = cur.next::<Vec<u8>, ()>()?;
                }
            }
        }

        let deleted = keys.len();
        for k in &keys {
            txn.del(&table, k, None)?;
        }
        txn.commit()?;
        Ok(deleted)
    }

    /// Walks env0 through the `KvRead` seam and decompresses page values —
    /// proves the ADR 0002 read path end to end on real stored data.
    pub fn verify_scan(&self, compressed: bool) -> Result<(usize, u64)> {
        let txn = self.envs[0].begin_ro_txn()?;
        let table = txn.open_table(None)?;
        let reader = MdbxTableReader::new(&txn, table);
        let mut cur = reader.new_cursor();
        cur.seek_first()?;
        let mut entries = 0usize;
        let mut raw_bytes = 0u64;
        while cur.is_valid() {
            if is_page_key(cur.key()) && compressed {
                let raw = lz4_flex::decompress_size_prepended(cur.value())
                    .map_err(|e| anyhow::anyhow!("page decompress failed: {e}"))?;
                raw_bytes += raw.len() as u64;
            } else {
                raw_bytes += cur.value().len() as u64;
            }
            entries += 1;
            cur.next()?;
        }
        Ok((entries, raw_bytes))
    }

    fn disk_usage(&self) -> Result<DiskUsage> {
        let mut live = 0u64;
        for env in &self.envs {
            let info = env.info()?;
            let freelist = env.freelist()? as u64;
            let used_pages = (info.last_pgno() as u64 + 1).saturating_sub(freelist);
            live += used_pages * self.page_size as u64;
        }
        Ok(DiskUsage {
            file_bytes: dir_bytes(&self.root),
            live_bytes: live
        })
    }
}

pub struct RocksEngine {
    db: rocksdb::DB,
    root: std::path::PathBuf
}

const CF_TABLES: &str = "tables";

impl RocksEngine {
    /// Mirrors production `tables_cf_options`: LZ4, dynamic level bytes,
    /// compact-on-deletion collector, dedicated block cache.
    pub fn open(root: &Path, cache_mb: usize) -> Result<Self> {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let mut cf_opts = rocksdb::Options::default();
        let mut block = rocksdb::BlockBasedOptions::default();
        block.set_block_cache(&rocksdb::Cache::new_lru_cache(cache_mb << 20));
        cf_opts.set_block_based_table_factory(&block);
        cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        cf_opts.set_level_compaction_dynamic_level_bytes(true);
        cf_opts.add_compact_on_deletion_collector_factory(128 * 1024, 64 * 1024, 0.5);

        let db = rocksdb::DB::open_cf_descriptors(
            &db_opts,
            root,
            vec![rocksdb::ColumnFamilyDescriptor::new(CF_TABLES, cf_opts)]
        )?;
        Ok(Self {
            db,
            root: root.to_owned()
        })
    }

    fn cf(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_TABLES).expect("tables cf")
    }

    fn write_chunk(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for (k, v) in entries {
            batch.put_cf(self.cf(), k, v);
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn merge_tables(&self, old: &[u64], new_id: u64) -> Result<usize> {
        let mut moved: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut iter = self.db.raw_iterator_cf(self.cf());
        for id in old {
            let prefix = table_prefix(*id);
            iter.seek(prefix);
            while iter.valid() {
                let k = iter.key().expect("valid");
                if !k.starts_with(&prefix) {
                    break;
                }
                moved.push((k.to_vec(), iter.value().expect("valid").to_vec()));
                iter.next();
            }
        }

        let mut batch = rocksdb::WriteBatch::default();
        let mut idx = 0u32;
        for (k, v) in &moved {
            if is_page_key(k) {
                batch.put_cf(self.cf(), page_key(new_id, idx), v);
                idx += 1;
            }
        }
        batch.put_cf(self.cf(), meta_key(new_id), [0u8; 64]);
        // production purges with point deletes (no DeleteRange on the
        // transactional db), so the reference does too
        for (k, _v) in &moved {
            batch.delete_cf(self.cf(), k);
        }
        self.db.write(batch)?;
        Ok(moved.len())
    }

    fn delete_tables(&self, ids: &[u64]) -> Result<usize> {
        let mut batch = rocksdb::WriteBatch::default();
        let mut deleted = 0usize;
        let mut iter = self.db.raw_iterator_cf(self.cf());
        for id in ids {
            let prefix = table_prefix(*id);
            iter.seek(prefix);
            while iter.valid() {
                let k = iter.key().expect("valid");
                if !k.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(self.cf(), k);
                deleted += 1;
                iter.next();
            }
        }
        self.db.write(batch)?;
        Ok(deleted)
    }

    fn int_prop(&self, name: &str) -> u64 {
        self.db
            .property_int_value_cf(self.cf(), name)
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    fn disk_usage(&self) -> Result<DiskUsage> {
        Ok(DiskUsage {
            file_bytes: dir_bytes(&self.root),
            live_bytes: self.int_prop("rocksdb.live-sst-files-size")
        })
    }
}

pub fn dir_bytes(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_bytes(&p);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}
