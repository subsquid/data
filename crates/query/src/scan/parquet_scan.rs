use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray, PrimitiveBuilder, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use rayon::prelude::*;

use crate::primitives::{Name, RowIndex, RowIndexArrowType};
use crate::scan::io::MmapIO;
use crate::scan::parquet_metadata::ParquetMetadata;
use crate::scan::row_predicate::{RowPredicateRef};
use crate::scan::row_selection;
use crate::util::record_batch_vec_to_lazy_polars_df;


#[derive(Clone)]
pub struct ParquetScan {
    io: MmapIO,
    metadata: Arc<ParquetMetadata>,
    predicate: Option<RowPredicateRef>,
    projection: Option<HashSet<Name>>,
    row_selection: Option<RowSelection>,
    row_index: bool
}


impl ParquetScan {
    pub fn new(filename: &str) -> anyhow::Result<Self> {
        let io = MmapIO::open(filename)?;

        let metadata = ArrowReaderMetadata::load(
            &io,
            ArrowReaderOptions::new().with_page_index(true)
        )?;

        Ok(Self {
            io,
            metadata: Arc::new(ParquetMetadata::new(metadata)),
            predicate: None,
            projection: None,
            row_selection: None,
            row_index: false
        })
    }

    pub fn filename(&self) -> &str {
        self.io.filename()
    }

    pub fn with_predicate<P: Into<Option<RowPredicateRef>>>(mut self, predicate: P) -> Self {
        self.predicate = predicate.into();
        self
    }

    pub fn with_projection(mut self, projection: HashSet<Name>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_column(self, column: Name) -> Self {
        self.with_columns([column])
    }

    pub fn with_columns<I>(mut self, columns: I) -> Self
        where I: IntoIterator<Item = Name>
    {
        if let Some(projection) = self.projection.as_mut() {
            projection.extend(columns);
            self
        } else {
            self.with_projection(columns.into_iter().collect())
        }
    }

    pub fn with_row_index(mut self, yes: bool) -> Self {
        self.row_index = yes;
        self
    }

    pub fn with_row_selection<S: Into<Option<RowSelection>>>(mut self, row_selection: S) -> Self {
        self.row_selection = row_selection.into();
        self
    }

    pub fn to_lazy_df(self) -> anyhow::Result<polars::prelude::LazyFrame> {
        let batches = self.execute()?;
        record_batch_vec_to_lazy_polars_df(&batches)
    }

    pub fn execute(self) -> anyhow::Result<Vec<RecordBatch>> {
        let mut maybe_row_selection = self.row_selection;

        if let Some(predicate) = self.predicate.as_ref() {
            if predicate.can_evaluate_stats() {
                let stats = self.metadata.row_group_stats();

                let selection = predicate.evaluate_stats(stats)?
                    .map(row_selection::from_row_ranges);

                maybe_row_selection = row_selection::intersect(
                    maybe_row_selection,
                    selection
                )
            }
        }

        let parquet_metadata = self.metadata.metadata().metadata();

        let mut row_groups = if let Some(selection) = maybe_row_selection {
            row_selection::select_row_groups(
                parquet_metadata.row_groups(),
                &selection
            )
        } else {
            (0..parquet_metadata.num_row_groups())
                .map(|idx| (idx, None))
                .collect()
        };

        if let Some(predicate) = self.predicate.as_ref() {
            if predicate.can_evaluate_stats() {
                for (row_group_idx, sel_ptr) in row_groups.iter_mut() {
                    let stats = self.metadata.page_stats(*row_group_idx);

                    let selection = predicate.evaluate_stats(stats)?
                        .map(row_selection::from_row_ranges);

                    let prev_selection = std::mem::replace(sel_ptr, None);

                    let _ = std::mem::replace(sel_ptr, row_selection::intersect(
                        prev_selection,
                        selection
                    ));
                }
            }
        }

        let (projection_mask, predicate_columns) = if let Some(columns) = self.projection {
            let mut predicate_columns = self.predicate.as_ref()
                .map_or([].as_slice(), |p| p.projection())
                .to_vec();

            predicate_columns.retain(|name| !columns.contains(name));

            let mut indices = Vec::with_capacity(
                columns.len() + predicate_columns.len()
            );

            let fields = self.metadata.metadata().parquet_schema().root_schema().get_fields();

            for name in columns.iter().chain(predicate_columns.iter()).copied() {
                let idx = fields.iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| {
                        anyhow::format_err!("column '{}' is not found in {}", name, self.io.filename())
                    })?;
                indices.push(idx);
            }

            let projection_mask = ProjectionMask::roots(
                parquet_metadata.file_metadata().schema_descr(),
                indices
            );

            (projection_mask, predicate_columns)
        } else {
            (ProjectionMask::all(), vec![])
        };

        let maybe_row_group_offsets = self.row_index.then(|| {
            build_row_group_offsets(parquet_metadata.row_groups())
        });

        let results: Vec<_> = row_groups.into_par_iter().map(|(row_group_idx, maybe_row_selection)| {
            read_row_group(
                self.io.clone(),
                self.metadata.metadata(),
                row_group_idx,
                projection_mask.clone(),
                self.predicate.clone().map(|p| (p, predicate_columns.as_slice())),
                maybe_row_selection,
                maybe_row_group_offsets.as_ref().map(|offsets| offsets[row_group_idx]),
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
    maybe_predicate: Option<(RowPredicateRef, &[Name])>,
    maybe_row_selection: Option<RowSelection>,
    maybe_row_index_offset: Option<i64>,
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
        build_row_index_array(offset as usize, len as usize, &maybe_row_selection)
    });

    if let Some(row_selection) = maybe_row_selection {
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
            let mask = predicate.evaluate(&batch)?;

            if predicate_columns.len() > 0 {
                let projection: Vec<usize> = batch.schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, f)| {
                        if predicate_columns.contains(&f.name().as_str()) {
                            None
                        } else {
                            Some(idx)
                        }
                    }).collect();

                batch = batch.project(&projection)?;
            }

            batch = arrow::compute::filter_record_batch(&batch, &mask)?;
        }

        result.push(batch)
    }

    Ok(result)
}


fn build_row_group_offsets(row_groups: &[RowGroupMetaData]) -> Vec<i64> {
    let mut offsets = vec![0; row_groups.len()];
    for i in 1..offsets.len() {
        offsets[i] = offsets[i-1] + row_groups[i-1].num_rows();
    }
    offsets
}


fn build_row_index_array(
    mut offset: usize,
    len: usize,
    maybe_row_selection: &Option<RowSelection>
) -> PrimitiveArray<RowIndexArrowType> {
    if let Some(row_selection) = maybe_row_selection {
        let mut selection_span = 0;
        let mut row_count = 0;
        for sel in row_selection.iter() {
            selection_span += sel.row_count;
            if !sel.skip {
                row_count += sel.row_count;
            }
        }
        assert!(selection_span <= len);
        let mut array = PrimitiveBuilder::with_capacity(row_count);
        for sel in row_selection.iter() {
            if !sel.skip {
                for i in offset..offset + sel.row_count {
                    array.append_value(i as RowIndex)
                }
            }
            offset += sel.row_count;
        }
        array.finish()
    } else {
        (offset as RowIndex..(offset + len) as RowIndex).collect()
    }
}


fn add_row_index(batch: &RecordBatch, index: PrimitiveArray<RowIndexArrowType>) -> RecordBatch {
    let mut schema_builder = SchemaBuilder::from(batch.schema().as_ref());
    schema_builder.reverse();
    schema_builder.push(Field::new("row_index", DataType::UInt32, false));
    schema_builder.reverse();
    let schema = schema_builder.finish();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
    columns.push(Arc::new(index));
    columns.extend(batch.columns().iter().cloned());

    RecordBatch::try_new_with_options(
        SchemaRef::new(schema),
        columns,
        &RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(batch.num_rows()))
    ).unwrap()
}