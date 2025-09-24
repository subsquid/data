use crate::scan::chunk::Chunk;
use crate::scan::parquet::file::ParquetFile;
use crate::scan::scan::Scan;
use crate::TableDoesNotExist;
use anyhow::anyhow;
use sqd_primitives::Name;
use std::sync::Arc;


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
            let file_path = format!("{}/{}.parquet", self.path, name);
            ParquetFile::open(file_path)
                .map_err(|err| {
                    if let Some(err) = err.downcast_ref::<std::io::Error>() {
                        if err.kind() == std::io::ErrorKind::NotFound {
                            return anyhow!(TableDoesNotExist::new(name))
                        }
                    }
                    err
                })
                .map(Arc::new)
        }).map(|r| {
            r.value().clone()
        })?;
        Ok(Scan::new(file))
    }
}