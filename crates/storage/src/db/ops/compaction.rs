use crate::db::db::RocksDB;
use crate::db::ops::schema_merge::can_merge_schemas;
use crate::db::ops::table_merge::TableMerge;
use crate::db::table_id::TableId;
use crate::db::write::tx::Tx;
use crate::db::{Chunk, ChunkReader, DatasetId, ReadSnapshot, TableBuilder};
use arrow::datatypes::SchemaRef;
use sqd_primitives::BlockNumber;
use std::cmp::{max, min};
use std::collections::BTreeMap;


pub const MAX_CHUNK_SIZE: usize = 200_000;
pub const WA_LIMIT: f64 = 1.9;
pub const MERGE_HORIZON: usize = 500;
pub const COMPACTION_LEN_LIMIT: usize = 10;

pub enum CompactionStatus {
    Ok(Vec<MergedChunk>),
    Canceled,
    NotingToCompact,
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
    compaction_len_limit: Option<usize>,
) -> anyhow::Result<CompactionStatus>
{
    DatasetCompaction {
        db,
        snapshot: &ReadSnapshot::new(db),
        dataset_id,
        merge: Vec::new(),
        max_chunk_size: max_chunk_size.unwrap_or(MAX_CHUNK_SIZE),
        write_amplification_limit: write_amplification_limit.unwrap_or(WA_LIMIT),
        compaction_len_limit: compaction_len_limit.unwrap_or(COMPACTION_LEN_LIMIT),
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
    compaction_len_limit: usize,
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
                None => return Ok(CompactionStatus::Canceled),
            };

            if self.data_was_changed(tx)? {
                return Ok(CompactionStatus::Canceled);
            }

            self.delete_merged_chunks(tx)?;
            tx.write_chunk(self.dataset_id, &new_chunk)?;

            label.bump_version();
            tx.write_label(self.dataset_id, &label)?;

            let merged_chunks = self.merge.iter().map(|c| {
                let size = c.tables()
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
            }).collect::<anyhow::Result<_>>()?;

            Ok(CompactionStatus::Ok(merged_chunks))
        })
    }

    fn data_was_changed(&self, tx: &Tx) -> anyhow::Result<bool> {
        let current_chunks = tx.list_chunks(
            self.dataset_id,
            self.merge[0].first_block(),
            Some(self.merge.last().unwrap().last_block()),
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
            tables,
        }
    }

    fn merge_all_tables(&self, tables: &mut BTreeMap<String, TableId>) -> anyhow::Result<()> {
        for name in self.merge[0].tables().keys() {
            self.merge_table(name, tables)?
        }
        Ok(())
    }

    fn merge_table(
        &self,
        name: &str,
        tables: &mut BTreeMap<String, TableId>,
    ) -> anyhow::Result<()> {
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

    fn score_merge(chunk_sizes: &[usize], max_wa: Option<f64>) -> usize {
        let max_size = chunk_sizes.iter().max().unwrap_or(&0).clone() as f64;
        let full_size = chunk_sizes.iter().sum();
        if max_size == 0.0 {
            return 1;
        }
        if let Some(wa_limit) = max_wa {
            let wa = full_size as f64 / max_size;
            if wa < wa_limit {
                return 0;
            }
        }
        full_size
    }

    fn find_range(
        chunk_sizes: &[usize],
        start: usize,
        end: usize,
        chunk_size_threshold: usize,
        wa_threshold: f64,
        len_limit: usize
    ) -> Option<(usize, usize, usize)> {
        if start + 1 >= end {
            return None;
        }
        let left = start;
        let mut right = left + 1;
        let right_limit = min(end, start + len_limit);
        loop {
            let score = Self::score_merge(&chunk_sizes[left..right], Some(wa_threshold));
            if score >= chunk_size_threshold {
                return Some((left, right, score));
            }
            if right < right_limit {
                right += 1;
                continue;
            }
            if score > 0 {
                return Some((left, right, score));
            }
            break;
        }
        let max_el = chunk_sizes[left..right].iter().max().unwrap();
        let max_idx = chunk_sizes[left..right]
            .iter()
            .position(|element| element == max_el)
            .unwrap();
        let left_range =
            Self::find_range(chunk_sizes, start, start + max_idx, *max_el, wa_threshold, len_limit);
        if left_range.is_some() {
            return left_range;
        }
        Self::find_range(chunk_sizes, start + max_idx + 1, end, *max_el, wa_threshold, len_limit)
    }

    fn prepare_merge_plan(&mut self) -> anyhow::Result<()> {
        let mut reversed_chunk_iterator = self
            .snapshot
            .list_chunks(self.dataset_id, 0, None)
            .into_reversed();
        let mut first_applicable_block = u64::MAX;
        let mut chunk_data_sizes: Vec<Vec<usize>> = Default::default();
        let mut last_schema_map: BTreeMap<String, SchemaRef> = Default::default();
        let mut first_block: Option<u64> = None;
        chunk_data_sizes.push(Default::default());
        let mut chunk_ctr = 0;
        while let Some(el) = reversed_chunk_iterator.next().transpose()? {
            if chunk_ctr < MERGE_HORIZON {
                chunk_ctr += 1;
            } else {
                break;
            }
            let tables = el.tables();
            let mut max_rows = 0;
            let mut schema_compatible = true;
            let mut data_continuous = true;
            if let Some(first_block) = first_block {
                data_continuous = (el.last_block() + 1 == first_block);
            }
            first_block = Some(el.first_block());
            for (key, v) in tables {
                let reader = self.snapshot.create_table_reader(*v)?;
                max_rows = max(reader.num_rows(), max_rows);
                let this_schema = reader.schema();
                if let Some(last_schema) = last_schema_map.get(key) {
                    schema_compatible &= can_merge_schemas(&this_schema, last_schema);
                }
                last_schema_map.insert(key.to_string(), this_schema);
            }
            if max_rows == 0 {
                max_rows = el.blocks_count() as usize;
            }
            if schema_compatible && data_continuous && max_rows < self.max_chunk_size {
                chunk_data_sizes.last_mut().unwrap().push(max_rows);
            } else {
                chunk_data_sizes.push(vec![max_rows; 1]);
            }
            if max_rows >= self.max_chunk_size {
                chunk_data_sizes.push(Default::default());
            }
            first_applicable_block = el.first_block();
        }
        chunk_data_sizes.iter_mut().for_each(|v| v.reverse());
        chunk_data_sizes.reverse();

        let mut skip_chunks = 0;
        let mut take_chunks = 0;

        for (idx, continous_run) in chunk_data_sizes.iter().enumerate() {
            if continous_run.len() < 2 {
                // skip unmergable run
                skip_chunks += continous_run.len();
                continue;
            }
            if idx < chunk_data_sizes.len() - 1 {
                // there will be no more chunks in this run, we can just merge disregarding write amplification as each chunk would be merged at most once
                let left = 0;
                let mut right = 1;
                while Self::score_merge(&continous_run[left..right], None) < self.max_chunk_size {
                    if right < continous_run.len() {
                        right += 1;
                    } else {
                        break;
                    }
                }
                skip_chunks += left;
                take_chunks = right - left;
                break;
            }
            // we should find appropriate range to merge respecting write amplification
            let range_option = Self::find_range(
                continous_run,
                0,
                continous_run.len(),
                self.max_chunk_size,
                self.write_amplification_limit,
                self.compaction_len_limit
            );
            if let Some((left, right, score)) = range_option {
                skip_chunks += left;
                take_chunks = right - left;
                break;
            }
            skip_chunks += continous_run.len();
        }

        let mut chunk_iterator = self.snapshot.list_chunks(self.dataset_id, first_applicable_block, None);
        let mut new_chunk_size: u64 = 0;
        let mut expected_first_block = first_applicable_block;

        for chunk_result in chunk_iterator.skip(skip_chunks).take(take_chunks) {
            let chunk = chunk_result?;
            let reader = self.snapshot.create_chunk_reader(chunk);
            self.merge.push(reader);
        }
        Ok(())
    }
}
