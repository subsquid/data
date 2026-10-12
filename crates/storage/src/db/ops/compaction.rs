use std::{cmp::max, collections::BTreeMap};

use sqd_primitives::BlockNumber;

use self::plan::{MergePlanner, RunBoundary};
use crate::db::{
    db::RocksDB, ops::table_merge::TableMerge, table_id::TableId, write::tx::Tx, Chunk, ChunkReader, DatasetId,
    ReadSnapshot, TableBuilder
};

mod plan;

pub const MAX_CHUNK_SIZE: usize = 200_000;
pub const WA_LIMIT: f64 = 1.9;
pub const MERGE_HORIZON: usize = 500;
pub const COMPACTION_LEN_LIMIT: usize = 50;

pub enum CompactionStatus {
    Ok(Vec<MergedChunk>),
    Canceled,
    NotingToCompact
}

#[derive(Debug)]
pub struct MergedChunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub size: usize
}

pub fn perform_dataset_compaction(
    db: &RocksDB,
    dataset_id: DatasetId,
    max_chunk_size: Option<usize>,
    write_amplification_limit: Option<f64>,
    compaction_len_limit: Option<usize>
) -> anyhow::Result<CompactionStatus> {
    DatasetCompaction {
        db,
        snapshot: &ReadSnapshot::new(db),
        dataset_id,
        merge: Vec::new(),
        max_chunk_size: max_chunk_size.unwrap_or(MAX_CHUNK_SIZE),
        write_amplification_limit: write_amplification_limit.unwrap_or(WA_LIMIT),
        compaction_len_limit: compaction_len_limit.unwrap_or(COMPACTION_LEN_LIMIT)
    }
    .execute()
}

struct DatasetCompaction<'a> {
    db: &'a RocksDB,
    snapshot: &'a ReadSnapshot<'a>,
    dataset_id: DatasetId,
    merge: Vec<ChunkReader<'a>>,
    max_chunk_size: usize,
    write_amplification_limit: f64,
    compaction_len_limit: usize
}

impl<'a> DatasetCompaction<'a> {
    fn execute(mut self) -> anyhow::Result<CompactionStatus> {
        self.prepare_merge_plan()?;

        if self.merge.len() < 2 {
            return Ok(CompactionStatus::NotingToCompact);
        }

        let new_chunk = {
            let mut tables = BTreeMap::new();
            self.merge_all_tables(&mut tables)?;
            self.make_chunk(tables)
        };

        Tx::new(self.db).run(|tx| {
            let mut label = match tx.find_label_for_update(self.dataset_id)? {
                Some(label) => label,
                None => return Ok(CompactionStatus::Canceled)
            };

            if self.data_was_changed(tx)? {
                return Ok(CompactionStatus::Canceled);
            }

            self.delete_merged_chunks(tx)?;
            tx.write_chunk(self.dataset_id, &new_chunk)?;

            label.bump_version();
            tx.write_label(self.dataset_id, &label)?;

            let merged_chunks = self
                .merge
                .iter()
                .map(|c| {
                    let size = c
                        .tables()
                        .keys()
                        .map(|name| c.get_table_reader(name).map(|r| r.num_rows()))
                        .fold(Ok::<_, anyhow::Error>(0), |acc, size| {
                            let acc = acc?;
                            let size = size?;
                            Ok(max(acc, size))
                        })?;
                    Ok(MergedChunk {
                        first_block: c.first_block(),
                        last_block: c.last_block(),
                        size
                    })
                })
                .collect::<anyhow::Result<_>>()?;

            Ok(CompactionStatus::Ok(merged_chunks))
        })
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
                return Ok(true);
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
        Chunk::V1 {
            first_block: first_chunk.first_block(),
            last_block: last_chunk.last_block(),
            last_block_hash: last_chunk.last_block_hash().to_string(),
            parent_block_hash: first_chunk.base_block_hash().to_string(),
            first_block_time: first_chunk.chunk().first_block_time(),
            last_block_time: last_chunk.chunk().last_block_time(),
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
        let chunks = self
            .merge
            .iter()
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
        let reversed_chunk_iterator = self.snapshot.list_chunks(self.dataset_id, 0, None).into_reversed();
        let mut planner = MergePlanner::new(
            self.max_chunk_size,
            self.write_amplification_limit,
            self.compaction_len_limit
        );
        let mut boundary = RunBoundary::default();
        let mut chunks = Vec::new();
        for chunk in reversed_chunk_iterator.take(MERGE_HORIZON) {
            let chunk = chunk?;

            let mut max_rows = 0;
            let mut schemas = Vec::with_capacity(chunk.tables().len());
            for (name, table_id) in chunk.tables() {
                let reader = self.snapshot.create_table_reader(*table_id)?;
                max_rows = max(reader.num_rows(), max_rows);
                schemas.push((name.as_str(), reader.schema()));
            }
            if max_rows == 0 {
                max_rows = chunk.blocks_count() as usize;
            }

            let joins_next = boundary.joins_next(chunk.first_block(), chunk.last_block(), &schemas);
            planner.push_reversed(max_rows, joins_next);
            chunks.push(chunk);
        }

        chunks.reverse();
        for chunk in chunks.drain(planner.finish()) {
            self.merge.push(self.snapshot.create_chunk_reader(chunk));
        }
        Ok(())
    }
}
