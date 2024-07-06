use std::sync::Arc;
use sqd_primitives::Name;
use crate::scan::chunk::Chunk;
use crate::scan::parquet::file::ParquetFile;
use crate::scan::scan::Scan;


pub struct ParquetChunk {
    path: String,
    tables: dashmap::DashMap<Name, Arc<ParquetFile>>
}


impl ParquetChunk {
    pub fn new<P: Into<String>>(path: P) -> Self {
        Self {
            path: path.into(),
            tables: dashmap::DashMap::new()
        }
    }
}


impl Chunk for ParquetChunk {
    fn scan_table(&self, name: Name) -> anyhow::Result<Scan<'_>> {
        let entry = self.tables.entry(name);
        let file = entry.or_try_insert_with(|| {
            ParquetFile::open(format!("{}/{}.parquet", self.path, name)).map(Arc::new)
        }).map(|r| {
            r.value().clone()
        })?;
        Ok(Scan::new(file))
    }
}