use std::{ops::Range, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{RecordBatch, RecordBatchOptions},
    datatypes::{Schema, SchemaRef}
};
use rayon::prelude::*;
use sqd_primitives::range::RangeList;
use sqd_query_engine::scan::{ColumnarChunk, ColumnarTable, WindowStats};
use sqd_storage::db::{ChunkReader, SnapshotTableReader};

/// A storage chunk as the new engine reads it.
pub struct HotChunk<'r, 's> {
    pub reader: &'r ChunkReader<'s>
}

impl<'s> ColumnarChunk for HotChunk<'_, 's> {
    type Table = HotTable<'s>;

    fn has_table(&self, name: &str) -> bool {
        self.reader.has_table(name)
    }

    fn open_table(&self, name: &str) -> anyhow::Result<Option<HotTable<'s>>> {
        if !self.reader.has_table(name) {
            return Ok(None);
        }
        let table = self.reader.get_table_reader(name)?;
        Ok(Some(HotTable(table)))
    }
}

pub struct HotTable<'s>(Arc<SnapshotTableReader<'s>>);

impl ColumnarTable for HotTable<'_> {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn read(&self, columns: &[usize], ranges: Option<&[Range<usize>]>) -> anyhow::Result<RecordBatch> {
        let ranges = ranges.map(|ranges| {
            RangeList::new(
                ranges
                    .iter()
                    .map(|range| range.start as u32..range.end as u32)
                    .collect()
            )
        });

        if columns.is_empty() {
            let rows = ranges
                .as_ref()
                .map_or(self.0.num_rows(), |ranges| ranges.iter().map(|r| r.len()).sum());
            let options = RecordBatchOptions::new().with_row_count(Some(rows));
            return Ok(RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &options
            )?);
        }

        let arrays = columns
            .par_iter()
            .map(|&index| self.0.read_column(index, ranges.as_ref()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let schema = self.0.schema().project(columns)?;
        RecordBatch::try_new(Arc::new(schema), arrays).context("assembling the columns of a storage table")
    }

    fn stats(&self, column: usize) -> anyhow::Result<Option<WindowStats>> {
        let Some(stats) = self.0.get_column_stats(column)? else {
            return Ok(None);
        };
        Ok(Some(WindowStats {
            offsets: stats.offsets.iter().map(|&offset| offset as usize).collect(),
            min: stats.min,
            max: stats.max
        }))
    }
}
