use crate::db::db::{RocksDB, RocksWriteBatch, CF_DIRTY_TABLES, CF_TABLES};
use crate::db::table_id::TableId;
use crate::kv::KvReadCursor;
use crate::table::key::TableKeyFactory;


pub fn delete_table(db: &RocksDB, table_id: TableId) -> anyhow::Result<()> {
    let mut key1 = TableKeyFactory::new(table_id);
    let mut key2 = TableKeyFactory::new(table_id);
    let start = key1.start();
    let end = key2.end();
    
    let cf_tables = db.cf_handle(CF_TABLES).unwrap();
    let mut batch = RocksWriteBatch::default();
    let mut cursor = db.raw_iterator_cf(cf_tables);
    
    list_keys(&mut cursor, start, end, |key| {
        batch.delete_cf(cf_tables, key)
    })?;
    
    let cf_dirty_tables = db.cf_handle(CF_DIRTY_TABLES).unwrap();
    batch.delete_cf(cf_dirty_tables, table_id);

    db.write(batch)?;
    Ok(())
}


fn list_keys(
    cursor: &mut impl KvReadCursor, 
    from: &[u8],
    to: &[u8],
    mut cb: impl FnMut(&[u8])
) -> anyhow::Result<()>
{
    cursor.seek(from)?;
    while cursor.is_valid() && cursor.key() < to {
        cb(cursor.key());
        cursor.next()?;
    }
    Ok(())
}