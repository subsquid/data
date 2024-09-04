use std::collections::HashSet;
use anyhow::anyhow;
use arrow::array::{RecordBatch, UInt32Array};
use sqd_primitives::RowRangeList;
use sqd_storage2::db::ChunkTableReader;
use crate::primitives::Name;
use crate::scan::array_predicate::ArrayStats;
use crate::scan::reader::TableReader;
use crate::scan::row_predicate::RowStats;
use crate::scan::RowPredicateRef;
use crate::scan::util::{add_row_index, build_row_index_array};


impl <'a> TableReader for ChunkTableReader<'a> {
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool
    ) -> anyhow::Result<Vec<RecordBatch>>
    {
        let mut maybe_new_row_selection = None;
        let mut maybe_new_projection = None;

        if let Some(predicate) = predicate.as_ref() {
            if predicate.can_evaluate_stats() {
                let maybe_selection = predicate.evaluate_stats(self)?;
                maybe_new_row_selection = maybe_selection.map(|ranges| {
                    if let Some(prev) = row_selection {
                        prev.intersection(&ranges)
                    } else {
                        ranges
                    }
                });
            }
            
            if let Some(columns) = projection {
                let new_columns = predicate.projection()
                    .iter()
                    .filter(|col| !columns.contains(*col))
                    .count();

                if new_columns > 0 {
                    let mut new_projection = HashSet::<Name>::with_capacity(columns.len() + new_columns);
                    new_projection.extend(columns);
                    new_projection.extend(predicate.projection());
                    maybe_new_projection = Some(new_projection);
                }
            }
        }

        let row_selection = maybe_new_row_selection.as_ref().or(row_selection);

        let mut record_batch = self.read_table(
            maybe_new_projection.as_ref().or(projection),
            row_selection
        )?;

        if with_row_index {
            let row_index = build_row_index_array(
                0,
                record_batch.num_rows(),
                row_selection
            );
            record_batch = add_row_index(&record_batch, row_index)
        }

        if let Some(predicate) = predicate {
            let mask = predicate.evaluate(&record_batch)?;

            if maybe_new_projection.is_some() {
                let projected_columns = projection.unwrap();

                let indexes: Vec<usize> = record_batch.schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, f)| {
                        if projected_columns.contains(&f.name().as_str()) {
                            Some(idx)
                        } else {
                            None
                        }
                    }).collect();

                record_batch = record_batch.project(&indexes)?;
            }
            
            record_batch = arrow::compute::filter_record_batch(&record_batch, &mask)?;
        }

        Ok(vec![record_batch])
    }
}


impl <'a> RowStats for ChunkTableReader<'a> {
    fn get_column_offsets(&self, column: Name) -> anyhow::Result<UInt32Array> {
        let index = self.schema().index_of(column)?;
        let stats = self.get_column_stats(index)?.ok_or_else(|| {
           anyhow!("column {} doesn't have stats", column)
        })?;
        Ok(UInt32Array::new(stats.offsets, None))
    }

    fn get_column_stats(&self, column: Name) -> anyhow::Result<Option<ArrayStats>> {
        let index = self.schema().index_of(column)?;
        let stats = self.get_column_stats(index)?;
        Ok(stats.map(|stats| {
            ArrayStats {
                min: stats.min,
                max: stats.max
            }
        }))
    }
}