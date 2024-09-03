use rocksdb::{ColumnFamily, WriteBatchWithTransaction};

use crate::db::db::{CF_DIRTY_TABLES, CF_TABLES, RocksDB};
use crate::db::table_id::TableId;
use crate::kv::KvWrite;


pub struct TableStorage<'a> {
    write_batch: WriteBatchWithTransaction<true>,
    db: &'a RocksDB,
    cf: &'a ColumnFamily,
}


impl <'a> TableStorage<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            write_batch: WriteBatchWithTransaction::default(),
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
}


impl <'a> KvWrite for TableStorage<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.write_batch.put_cf(self.cf, key, value);
        Ok(())
    }
}
