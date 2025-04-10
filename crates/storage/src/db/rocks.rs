use crate::db::db::RocksIterator;
use crate::kv::KvReadCursor;


impl <'a, DB: rocksdb::DBAccess> KvReadCursor for RocksIterator<'a, DB> {
    fn seek_first(&mut self) -> anyhow::Result<()> {
        self.seek_to_first();
        self.status()?;
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.seek(key);
        self.status()?;
        Ok(())
    }

    fn seek_prev(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.seek_for_prev(key);
        self.status()?;
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.next();
        self.status()?;
        Ok(())
    }

    fn prev(&mut self) -> anyhow::Result<()> {
        self.prev();
        self.status()?;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.valid()
    }

    fn key(&self) -> &[u8] {
        self.key().expect("cursor position is not valid")
    }

    fn value(&self) -> &[u8] {
        self.value().expect("cursor position is not valid")
    }
}