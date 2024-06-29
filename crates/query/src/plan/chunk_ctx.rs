use crate::scan::ParquetScan;
use crate::primitives::Name;


pub struct ChunkCtx {
    path: String,
    tables: dashmap::DashMap<Name, ParquetScan>
}


impl ChunkCtx {
    pub fn new(path: String) -> Self {
        ChunkCtx {
            path,
            tables: dashmap::DashMap::new()
        }
    }

    pub fn get_parquet(&self, table: Name) -> anyhow::Result<ParquetScan> {
        let entry = self.tables.entry(table);
        entry.or_try_insert_with(|| {
            ParquetScan::new(&format!("{}/{}.parquet", self.path, table))
        }).map(|r| {
            r.value().clone()
        })
    }
}