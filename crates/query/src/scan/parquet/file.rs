use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Not;
use std::sync::Arc;

use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use rayon::prelude::*;

use sqd_primitives::RowRangeList;

use crate::primitives::{Name, RowIndex};
use crate::scan::parquet::io::MmapIO;
use crate::scan::parquet::metadata::ParquetMetadata;
use crate::scan::reader::TableReader;
use crate::scan::row_predicate::RowPredicateRef;
use crate::scan::util::{add_row_index, apply_predicate, build_row_index_array};


#[derive(Clone)]
pub struct ParquetFile {
    io: MmapIO,
    metadata: Arc<ParquetMetadata>,
    filename: Arc<String>
}


impl ParquetFile {
    pub fn open<P: Into<String>>(file: P) -> anyhow::Result<Self> {
        let filename = file.into();

        let io = MmapIO::open(&filename)?;

        let metadata = ArrowReaderMetadata::load(
            &io,
            ArrowReaderOptions::new().with_page_index(true)
        )?;

        Ok(Self {
            io,
            metadata: Arc::new(ParquetMetadata::new(metadata)),
            filename: Arc::new(filename)
        })
    }
}


impl TableReader for ParquetFile {
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool
    ) -> anyhow::Result<Vec<RecordBatch>>
    {
        let mut maybe_new_row_selection = None;

        if let Some(predicate) = predicate.as_ref() {
            if predicate.can_evaluate_stats() {
                let stats = self.metadata.row_group_stats();

                let maybe_selection = predicate.evaluate_stats(stats)?;

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

        let parquet_metadata = self.metadata.metadata().metadata();

        let mut maybe_row_group_offsets: Option<Vec<RowIndex>> = None;

        let mut row_groups = if let Some(row_ranges) = row_selection {
            let row_group_offsets = build_row_group_offsets(parquet_metadata.row_groups());
            let selected_row_groups = row_ranges.filter_groups(&row_group_offsets).collect::<Vec<_>>();
            maybe_row_group_offsets = Some(row_group_offsets);
            selected_row_groups
        } else {
            (0..parquet_metadata.num_row_groups())
                .map(|idx| (idx, None))
                .collect()
        };

        if let Some(predicate) = predicate.as_ref() {
            if predicate.can_evaluate_stats() {
                for (row_group_idx, sel_ptr) in row_groups.iter_mut() {
                    let stats = self.metadata.page_stats(*row_group_idx);
                    if let Some(row_ranges) = predicate.evaluate_stats(stats)? {
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
            let predicate_columns = predicate.as_ref()
                .map_or([].as_slice(), |p| p.projection())
                .iter()
                .filter_map(|col| {
                    columns.contains(col).not().then(|| *col)
                })
                .collect::<HashSet<_>>();

            let mut indices = Vec::with_capacity(
                columns.len() + predicate_columns.len()
            );

            let fields = self.metadata.metadata().parquet_schema().root_schema().get_fields();

            for name in columns.iter().chain(predicate_columns.iter()).copied() {
                let idx = fields.iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| {
                        anyhow::format_err!("column '{}' is not found in {}", name, self.filename)
                    })?;
                indices.push(idx);
            }

            let projection_mask = ProjectionMask::roots(
                parquet_metadata.file_metadata().schema_descr(),
                indices
            );

            (projection_mask, predicate_columns)
        } else {
            (ProjectionMask::all(), HashSet::new())
        };

        let maybe_row_index_offsets = with_row_index.then(|| {
            maybe_row_group_offsets.unwrap_or_else(|| {
                build_row_group_offsets(parquet_metadata.row_groups())
            })
        });

        let results: Vec<_> = row_groups.into_par_iter().map(|(row_group_idx, maybe_row_selection)| {
            read_row_group(
                self.io.clone(),
                self.metadata.metadata(),
                row_group_idx,
                projection_mask.clone(),
                predicate.clone().map(|p| (p, &predicate_columns)),
                maybe_row_selection,
                maybe_row_index_offsets.as_ref().map(|offsets| offsets[row_group_idx]),
                1_000_000_000
            )
        }).collect();

        let mut record_batches = Vec::with_capacity(
            results.iter().map(|r| r.as_ref().map_or(0, |bs| bs.len())).sum()
        );

        for r in results {
            record_batches.extend(r?)
        }
        
        Ok(record_batches)
    }
}


fn read_row_group(
    io: MmapIO,
    metadata: &ArrowReaderMetadata,
    row_group_idx: usize,
    projection: ProjectionMask,
    maybe_predicate: Option<(RowPredicateRef, &HashSet<Name>)>,
    maybe_row_selection: Option<RowRangeList>,
    maybe_row_index_offset: Option<RowIndex>,
    record_batch_size: usize
) -> anyhow::Result<Vec<RecordBatch>> {

    let mut reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        io,
        metadata.clone()
    );

    reader = reader.with_row_groups(vec![row_group_idx]);
    reader = reader.with_batch_size(record_batch_size);
    reader = reader.with_projection(projection);

    let maybe_row_index = maybe_row_index_offset.map(|offset| {
        let len = metadata.metadata().row_group(row_group_idx).num_rows();
        build_row_index_array(offset, len as usize, &maybe_row_selection)
    });

    if let Some(row_ranges) = maybe_row_selection {
        let row_selection = to_parquet_row_selection(&row_ranges);
        reader = reader.with_row_selection(row_selection);
    }

    let mut result = Vec::new();
    let mut batch_offset = 0;

    for batch_result in reader.build()? {
        let mut batch = batch_result?;

        if let Some(row_index) = &maybe_row_index {
            let row_index = row_index.slice(batch_offset, batch.num_rows());
            batch = add_row_index(&batch, row_index)
        }

        batch_offset += batch.num_rows();

        if let Some((predicate, predicate_columns)) = &maybe_predicate {
            batch = apply_predicate(batch, predicate.as_ref(), *predicate_columns)?;
        }

        if batch.num_rows() > 0 {
            result.push(batch)
        }
    }

    Ok(result)
}


fn build_row_group_offsets(row_groups: &[RowGroupMetaData]) -> Vec<RowIndex> {
    let mut offsets = Vec::with_capacity(row_groups.len() + 1);
    offsets.push(0);
    let mut current = 0;
    for rg in row_groups.iter() {
        current += rg.num_rows() as RowIndex;
        offsets.push(current)
    }
    offsets
}


fn to_parquet_row_selection(ranges: &RowRangeList) -> RowSelection {
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut last_end = 0u32;

    for range in ranges.iter() {
        let len = (range.end - range.start) as usize;
        match range.start.cmp(&last_end) {
            Ordering::Equal => match selectors.last_mut() {
                Some(last) => last.row_count = last.row_count.checked_add(len).unwrap(),
                None => selectors.push(RowSelector::select(len)),
            },
            Ordering::Greater => {
                selectors.push(RowSelector::skip((range.start - last_end) as usize));
                selectors.push(RowSelector::select(len))
            }
            Ordering::Less => panic!("out of order"),
        }
        last_end = range.end;
    }

    RowSelection::from(selectors)
}