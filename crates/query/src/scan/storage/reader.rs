use std::collections::HashSet;
use std::ops::Not;

use arrow::array::{RecordBatch, UInt32Array};
use rayon::prelude::*;

use sqd_primitives::RowRangeList;
use sqd_storage::db::ChunkTableReader;
use sqd_storage::table::read::ProjectionMask;

use crate::primitives::Name;
use crate::scan::array_predicate::ArrayStats;
use crate::scan::reader::TableReader;
use crate::scan::row_predicate::RowStats;
use crate::scan::RowPredicateRef;
use crate::scan::util::{add_row_index, apply_predicate, build_row_index_array};


impl <'a> TableReader for ChunkTableReader<'a> {
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut maybe_new_row_selection = None;

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
        }

        let row_selection = maybe_new_row_selection.as_ref().or(row_selection);

        let mut row_groups = if let Some(row_ranges) = row_selection {
            row_ranges.paginate(
                self.get_row_group_offsets().values().as_ref()
            ).collect::<Vec<_>>()
        } else {
            (0..self.num_row_groups())
                .map(|idx| (idx, None))
                .collect()
        };

        if let Some(predicate) = predicate.as_ref() {
            if predicate.can_evaluate_stats() {
                for (row_group_idx, sel_ptr) in row_groups.iter_mut() {
                    let stats = RowGroupStats {
                        reader: self,
                        row_group: *row_group_idx
                    };

                    if let Some(row_ranges) = predicate.evaluate_stats(&stats)? {
                        let new_selection = if let Some(prev) = sel_ptr.as_ref() {
                            prev.intersection(&row_ranges)
                        } else {
                            row_ranges
                        };
                        let _ = std::mem::replace(sel_ptr, Some(new_selection));
                    }
                }
                row_groups.retain(|rg| {
                    rg.1.as_ref().map(|ranges| !ranges.is_empty()).unwrap_or(true)
                })
            }
        }

        let (projection_mask, predicate_columns) = if let Some(columns) = projection {
            let mut mask = ProjectionMask::new(self.num_columns());

            let predicate_columns = predicate.as_ref()
                .map_or([].as_slice(), |p| p.projection())
                .iter()
                .filter_map(|col| {
                    columns.contains(col).not().then(|| *col)
                })
                .collect::<HashSet<_>>();

            for name in columns.iter().chain(predicate_columns.iter()).copied() {
                let idx = self.schema().index_of(name)?;
                mask.include_column(idx);
            }

            (Some(mask), predicate_columns)
        } else {
            (None, HashSet::new())
        };

        row_groups.into_par_iter().map(|(row_group_idx, maybe_row_selection)| {
            read_row_group(
                self,
                row_group_idx,
                projection_mask.as_ref(),
                predicate.clone().map(|p| (p, &predicate_columns)),
                maybe_row_selection,
                with_row_index,
            )
        }).collect()
    }
}


fn read_row_group<'a>(
    reader: &'a ChunkTableReader<'a>,
    row_group_idx: usize,
    projection: Option<&ProjectionMask>,
    maybe_predicate: Option<(RowPredicateRef, &HashSet<Name>)>,
    maybe_row_selection: Option<RowRangeList>,
    with_row_index: bool
) -> anyhow::Result<RecordBatch>
{
    let mut record_batch = reader.read_row_group(
        row_group_idx,
        maybe_row_selection.as_ref(),
        projection
    )?;
    
    if with_row_index {
        let offsets = reader.get_row_group_offsets();
        let row_group_offset = offsets.value(row_group_idx);
        let row_group_len = offsets.value(row_group_idx + 1) - offsets.value(row_group_idx);
        let row_index = build_row_index_array(
            row_group_offset,
            row_group_len as usize,
            maybe_row_selection.as_ref()
        );
        record_batch = add_row_index(&record_batch, row_index)
    }

    if let Some((predicate, predicate_columns)) = maybe_predicate {
        record_batch = apply_predicate(record_batch, predicate.as_ref(), predicate_columns)?;
    }

    Ok(record_batch)
}


macro_rules! option_or_ok {
    ($exp:expr) => {
        match $exp {
            Some(x) => x,
            None => return Ok(None)
        }
    };
}


impl <'a> RowStats for ChunkTableReader<'a> {
    fn get_column_offsets(&self, _column: Name) -> anyhow::Result<UInt32Array> {
        Ok(self.get_row_group_offsets().clone())
    }

    fn get_column_stats(&self, column: Name) -> anyhow::Result<Option<ArrayStats>> {
        let col_idx = self.schema().index_of(column)?;
        let min = option_or_ok!(self.get_per_row_group_min(col_idx)?);
        let max = option_or_ok!(self.get_per_row_group_max(col_idx)?);
        Ok(Some(ArrayStats {
            min,
            max
        }))
    }
}


struct RowGroupStats<'a> {
    reader: &'a ChunkTableReader<'a>,
    row_group: usize
}


impl <'a> RowStats for RowGroupStats<'a> {
    fn get_column_offsets(&self, column: Name) -> anyhow::Result<UInt32Array> {
        let col_idx = self.reader.schema().index_of(column)?;
        self.reader.get_page_offsets(self.row_group, col_idx)
    }

    fn get_column_stats(&self, column: Name) -> anyhow::Result<Option<ArrayStats>> {
        let col_idx = self.reader.schema().index_of(column)?;
        let min = option_or_ok!(self.reader.get_per_page_min(self.row_group, col_idx)?);
        let max = option_or_ok!(self.reader.get_per_page_max(self.row_group, col_idx)?);
        Ok(Some(ArrayStats {
            min,
            max
        }))
    }
}