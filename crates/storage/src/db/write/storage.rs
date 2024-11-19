use crate::db::db::{RocksDB, RocksWriteBatch, CF_DIRTY_TABLES, CF_TABLES};
use crate::db::table_id::TableId;
use crate::kv::KvWrite;
use rocksdb::ColumnFamily;


pub struct TableStorage<'a> {
    write_batch: RocksWriteBatch,
    db: &'a RocksDB,
    cf: &'a ColumnFamily,
}


impl <'a> TableStorage<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            write_batch: RocksWriteBatch::default(),
            db,
            cf: db.cf_handle(CF_TABLES).unwrap()
        }
    }

    pub fn mark_table_dirty(&mut self, table_id: TableId) {
        let cf_dirty = self.db.cf_handle(CF_DIRTY_TABLES).unwrap();
        self.write_batch.put_cf(cf_dirty, table_id, [])
    }

    pub fn byte_size(&self) -> usize {
        self.write_batch.size_in_bytes()
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        let batch = std::mem::take(&mut self.write_batch);
        self.db.write(batch)?;
        Ok(())
    }
    
    pub fn finish(self) -> anyhow::Result<()> {
        self.db.write(self.write_batch)?;
        Ok(())
    }
}


impl <'a> KvWrite for TableStorage<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.write_batch.put_cf(self.cf, key, value);
        if self.byte_size() > 8 * 1024 * 1024 {
            self.flush()?;
        }
        Ok(())
    }
}
