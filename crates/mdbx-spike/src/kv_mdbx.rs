//! `sqd_storage::kv` traits over libmdbx — the seam ADR 0002 claims ports verbatim.
//!
//! The cursor copies the current pair out of the mmap; a production impl would
//! borrow (`Cow`) and be strictly faster. Conservative for read benchmarks.

use libmdbx::{Cursor, NoWriteMap, Table, Transaction, TransactionKind, WriteFlags, RO, RW};
use sqd_storage::kv::{KvRead, KvReadCursor, KvWrite};

pub struct MdbxTableWriter<'db, 'txn> {
    txn: &'txn Transaction<'db, RW, NoWriteMap>,
    table: Table<'txn>
}

impl<'db, 'txn> MdbxTableWriter<'db, 'txn> {
    pub fn new(txn: &'txn Transaction<'db, RW, NoWriteMap>, table: Table<'txn>) -> Self {
        Self { txn, table }
    }
}

impl KvWrite for MdbxTableWriter<'_, '_> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.txn.put(&self.table, key, value, WriteFlags::UPSERT)?;
        Ok(())
    }
}

pub struct MdbxTableReader<'db, 'txn> {
    txn: &'txn Transaction<'db, RO, NoWriteMap>,
    table: Table<'txn>
}

impl<'db, 'txn> MdbxTableReader<'db, 'txn> {
    pub fn new(txn: &'txn Transaction<'db, RO, NoWriteMap>, table: Table<'txn>) -> Self {
        Self { txn, table }
    }
}

impl<'db, 'txn> KvRead for MdbxTableReader<'db, 'txn> {
    type Cursor = MdbxKvCursor<'txn, RO>;

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<impl std::ops::Deref<Target = [u8]>>> {
        Ok(self.txn.get::<Vec<u8>>(&self.table, key)?)
    }

    fn new_cursor(&self) -> Self::Cursor {
        MdbxKvCursor {
            cur: self.txn.cursor(&self.table).expect("cursor open"),
            current: None
        }
    }
}

pub struct MdbxKvCursor<'txn, K: TransactionKind> {
    cur: Cursor<'txn, K>,
    current: Option<(Vec<u8>, Vec<u8>)>
}

impl<K: TransactionKind> KvReadCursor for MdbxKvCursor<'_, K> {
    // rocksdb iterator semantics: next/prev on an invalid cursor keep it invalid
    fn seek_first(&mut self) -> anyhow::Result<()> {
        self.current = self.cur.first::<Vec<u8>, Vec<u8>>()?;
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.current = self.cur.set_range::<Vec<u8>, Vec<u8>>(key)?;
        Ok(())
    }

    fn seek_prev(&mut self, key: &[u8]) -> anyhow::Result<()> {
        // last entry <= key: position at first >= key, then step back if overshot
        self.current = match self.cur.set_range::<Vec<u8>, Vec<u8>>(key)? {
            Some(kv) if kv.0.as_slice() > key => self.cur.prev::<Vec<u8>, Vec<u8>>()?,
            Some(kv) => Some(kv),
            None => self.cur.last::<Vec<u8>, Vec<u8>>()?
        };
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.current.is_some() {
            self.current = self.cur.next::<Vec<u8>, Vec<u8>>()?;
        }
        Ok(())
    }

    fn prev(&mut self) -> anyhow::Result<()> {
        if self.current.is_some() {
            self.current = self.cur.prev::<Vec<u8>, Vec<u8>>()?;
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> &[u8] {
        &self.current.as_ref().expect("cursor position is not valid").0
    }

    fn value(&self) -> &[u8] {
        &self.current.as_ref().expect("cursor position is not valid").1
    }
}

#[cfg(test)]
mod tests {
    use libmdbx::{Database, DatabaseOptions, NoWriteMap};
    use sqd_storage::kv::{KvRead, KvReadCursor, KvWrite};

    use super::*;

    #[test]
    fn cursor_matches_rocksdb_iterator_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let db: Database<NoWriteMap> = Database::open_with_options(dir.path(), DatabaseOptions::default()).unwrap();

        let txn = db.begin_rw_txn().unwrap();
        let table = txn.open_table(None).unwrap();
        let mut writer = MdbxTableWriter::new(&txn, table);
        for key in [b"b", b"d", b"f"] {
            writer.put(key, key).unwrap();
        }
        drop(writer);
        txn.commit().unwrap();

        let txn = db.begin_ro_txn().unwrap();
        let table = txn.open_table(None).unwrap();
        let reader = MdbxTableReader::new(&txn, table);

        assert_eq!(reader.get(b"d").unwrap().as_deref(), Some(b"d".as_slice()));
        assert_eq!(reader.get(b"c").unwrap().as_deref(), None);

        let mut cur = reader.new_cursor();

        cur.seek_first().unwrap();
        assert_eq!(cur.key(), b"b");

        cur.seek(b"c").unwrap();
        assert_eq!(cur.key(), b"d"); // first >= c

        cur.seek_prev(b"c").unwrap();
        assert_eq!(cur.key(), b"b"); // last <= c

        cur.seek_prev(b"d").unwrap();
        assert_eq!(cur.key(), b"d"); // exact hit stays

        cur.seek_prev(b"z").unwrap();
        assert_eq!(cur.key(), b"f"); // past the end -> last

        cur.next().unwrap();
        assert!(!cur.is_valid());
        cur.next().unwrap(); // stays invalid, no panic
        assert!(!cur.is_valid());

        cur.seek(b"a").unwrap();
        assert_eq!(cur.key(), b"b");
        cur.prev().unwrap();
        assert!(!cur.is_valid());
    }
}
