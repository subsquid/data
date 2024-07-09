use anyhow::ensure;
use rocksdb::Options as RocksOptions;
use sqd_dataset::DatasetDescriptionRef;

use crate::db::data::{DatasetId, DatasetKind, DatasetLabel};
use crate::db::read::snapshot::ReadSnapshot;
use crate::db::write::{ChunkBuilder, NewChunk, Tx};
use sqd_primitives::Name;


pub(super) const CF_DATASETS: Name = "DATASETS";
pub(super) const CF_DATASET_VERSIONS: Name = "DATASET_VERSIONS";
pub(super) const CF_CHUNKS: Name = "CHUNKS";
pub(super) const CF_TABLES: Name = "TABLES";
pub(super) const CF_DIRTY_TABLES: Name = "DIRTY_TABLES";


pub(super) type RocksDB = rocksdb::OptimisticTransactionDB;
pub(super) type RocksTransaction<'a> = rocksdb::Transaction<'a, RocksDB>;
pub(super) type RocksTransactionIterator<'a> = rocksdb::DBRawIteratorWithThreadMode<'a, RocksTransaction<'a>>;
pub(super) type RocksTransactionOptions = rocksdb::OptimisticTransactionOptions;
pub(super) type RocksSnapshot<'a> = rocksdb::SnapshotWithThreadMode<'a, RocksDB>;
pub(super) type RocksSnapshotIterator<'a> = rocksdb::DBRawIteratorWithThreadMode<'a, RocksDB>;


pub struct Database {
    db: RocksDB
}


impl Database {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = RocksDB::open_cf(&options, path, [
            CF_DATASETS,
            CF_CHUNKS,
            CF_TABLES,
            CF_DIRTY_TABLES
        ])?;
        Ok(Self {
            db
        })
    }

    pub fn create_dataset(&self, id: DatasetId, kind: DatasetKind) -> anyhow::Result<()> {
        let tx = Tx::new(&self.db);
        let label = tx.find_label_for_update(id)?;
        ensure!(label.is_none(), "dataset {} already exists", id);
        tx.write_label(id, &DatasetLabel {
            kind,
            version: 0
        })?;
        tx.commit()
    }

    pub fn new_chunk_builder(&self, ds: DatasetDescriptionRef) -> ChunkBuilder<'_> {
        ChunkBuilder::new(&self.db, ds)
    }

    pub fn insert_chunk(&self, dataset_id: DatasetId, new_chunk: NewChunk) -> anyhow::Result<()> {
        let tx = Tx::new_with_snapshot(&self.db);
        let mut label = tx.get_label_for_update(dataset_id)?;
        label.version += 1;
        tx.write_label(dataset_id, &label)?;
        tx.validate_new_chunk_insertion(dataset_id, &new_chunk)?;
        tx.write_new_chunk(dataset_id, new_chunk)?;
        tx.commit()
    }

    pub fn get_snapshot(&self) -> ReadSnapshot<'_> {
        ReadSnapshot::new(&self.db)
    }
}