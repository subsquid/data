use crate::db::db::RocksDB;
use crate::db::ops::table_merge::TableMerge;
use crate::db::table_id::TableId;
use crate::db::write::tx::Tx;
use crate::db::{Chunk, ChunkReader, DatasetId, ReadSnapshot, TableBuilder};
use std::collections::BTreeMap;


pub enum CompactionStatus {
    Ok,
    Canceled
}


pub fn perform_dataset_compaction(db: &RocksDB, dataset_id: DatasetId) -> anyhow::Result<CompactionStatus> {
    DatasetCompaction {
        db,
        _snapshot: &ReadSnapshot::new(db),
        dataset_id,
        merge: Vec::new()
    }.execute()
}


struct DatasetCompaction<'a> {
    db: &'a RocksDB,
    _snapshot: &'a ReadSnapshot<'a>,
    dataset_id: DatasetId,
    merge: Vec<ChunkReader<'a>>
}


impl<'a> DatasetCompaction<'a> {
    fn execute(mut self) -> anyhow::Result<CompactionStatus> {
        self.prepare_merge_plan()?;

        let new_chunk = {
            let mut tables = BTreeMap::new();
            self.merge_all_tables(&mut tables)?;
            self.make_chunk(tables)
        };

        let status = Tx::new(self.db).run(|tx| {
            let mut label = match tx.find_label_for_update(self.dataset_id)? {
                Some(label) => label,
                None => return Ok(CompactionStatus::Canceled)
            };

            if self.data_was_changed(tx)? {
                return Ok(CompactionStatus::Canceled)
            }

            self.delete_merged_chunks(tx)?;
            tx.write_chunk(self.dataset_id, &new_chunk)?;

            label.bump_version();
            tx.write_label(self.dataset_id, &label)?;
            Ok(CompactionStatus::Ok)
        })?;
        
        Ok(status)
    }

    fn data_was_changed(&self, tx: &Tx) -> anyhow::Result<bool> {
        let current_chunks = tx.list_chunks(
            self.dataset_id,
            self.merge[0].first_block(),
            Some(self.merge.last().unwrap().last_block())
        );

        let mut compared = 0;
        for (current, merged) in current_chunks.zip(self.merge.iter()) {
            let current = current?;
            if &current != merged.chunk() {
                return Ok(true)
            }
            compared += 1;
        }

        Ok(compared != self.merge.len())
    }

    fn delete_merged_chunks(&self, tx: &Tx) -> anyhow::Result<()> {
        for c in self.merge.iter() {
            tx.delete_chunk(self.dataset_id, c.chunk())?;
        }
        Ok(())
    }

    fn make_chunk(&self, tables: BTreeMap<String, TableId>) -> Chunk {
        let first_chunk = &self.merge[0];
        let last_chunk = self.merge.last().unwrap();
        Chunk::V0 {
            first_block: first_chunk.first_block(),
            last_block: last_chunk.last_block(),
            last_block_hash: last_chunk.last_block_hash().to_string(),
            base_block_hash: first_chunk.base_block_hash().to_string(),
            tables
        }
    }

    fn merge_all_tables(&self, tables: &mut BTreeMap<String, TableId>) -> anyhow::Result<()> {
        for name in self.merge[0].tables().keys() {
            self.merge_table(name, tables)?
        }
        Ok(())
    }

    fn merge_table(&self, name: &str, tables: &mut BTreeMap<String, TableId>) -> anyhow::Result<()> {
        let chunks = self.merge.iter()
            .map(|ch| ch.get_table_reader(name))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let src = TableMerge::prepare(&chunks)?;
        let mut table_builder = TableBuilder::new(self.db, src.schema());
        table_builder.set_stats(src.columns_with_stats().iter().copied())?;
        src.write(&mut table_builder)?;
        let table_id = table_builder.finish()?;
        
        tables.insert(name.to_string(), table_id);
        Ok(())
    }

    fn prepare_merge_plan(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}