use crate::db::db::RocksDB;
use crate::db::{ChunkReader, DatasetId, ReadSnapshot};


pub struct DatasetCompaction<'a> {
    db: &'a RocksDB,
    snapshot: ReadSnapshot<'a>,
    dataset_id: DatasetId,
    merge: Vec<ChunkReader<'a>>
}


impl<'a> DatasetCompaction<'a> {
    pub fn perform(db: &'a RocksDB, dataset_id: DatasetId) -> anyhow::Result<()> {
        let op = Self {
            db,
            snapshot: ReadSnapshot::new(db),
            dataset_id,
            merge: Vec::new()
        };
        
        Ok(())
    }
}