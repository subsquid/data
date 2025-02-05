use crate::db::db::{RocksDB, RocksWriteBatch, CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TABLES};
use crate::kv::KvReadCursor;
use crate::table::key::TableKeyFactory;


pub fn deleted_deleted_tables(db: &RocksDB) -> anyhow::Result<()> {
    loop {
        let mut is_clean = true;
        let cf_deleted_tables = db.cf_handle(CF_DELETED_TABLES).unwrap();
        let mut it = db.raw_iterator_cf(cf_deleted_tables);
        for_each_key(&mut it, |key| {
            is_clean = false;
            delete_table(db, key)
        })?;
        if is_clean {
            return Ok(())
        }
    }
}


fn delete_table(db: &RocksDB, table_id: &[u8]) -> anyhow::Result<()> {
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

    let cf_deleted_tables = db.cf_handle(CF_DELETED_TABLES).unwrap();
    batch.delete_cf(cf_deleted_tables, table_id);

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


fn for_each_key(
    cursor: &mut impl KvReadCursor,
    mut cb: impl FnMut(&[u8]) -> anyhow::Result<()>
) -> anyhow::Result<()>
{
    cursor.seek_first()?;
    while cursor.is_valid() {
        cb(cursor.key())?;
        cursor.next()?;
    }
    Ok(())
}