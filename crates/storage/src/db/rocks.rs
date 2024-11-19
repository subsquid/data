use crate::db::db::{RocksSnapshotIterator, RocksTransactionIterator};
use crate::kv::KvReadCursor;


macro_rules! impl_read_cursor {
    ($t:ident) => {
        impl<'a> KvReadCursor for $t<'a> {
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
                self.key().unwrap()
            }

            fn value(&self) -> &[u8] {
                self.value().unwrap()
            }
        }
    };
}


impl_read_cursor!(RocksTransactionIterator);
impl_read_cursor!(RocksSnapshotIterator);