use std::sync::Arc;

use sqd_primitives::Name;
use sqd_storage::db::{ChunkReader, SnapshotTableReader};

use crate::scan::Chunk;
use crate::scan::scan::Scan;


pub struct StorageChunk<'a> {
    reader: &'a ChunkReader<'a>,
    cache: dashmap::DashMap<Name, Arc<SnapshotTableReader<'a>>>
}


impl <'a> StorageChunk<'a> {
    pub fn new(reader: &'a ChunkReader<'a>) -> Self {
        Self {
            reader,
            cache: dashmap::DashMap::new()
        }
    }
}


impl <'a> Chunk for StorageChunk<'a> {
    fn scan_table(&self, name: Name) -> anyhow::Result<Scan<'a>> {
        let entry = self.cache.entry(name);
        let table = entry.or_try_insert_with(|| {
            self.reader.get_table_reader(name).map(Arc::new)
        }).map(|t| {
            t.clone()
        })?;
        Ok(Scan::new(table))
    }
}