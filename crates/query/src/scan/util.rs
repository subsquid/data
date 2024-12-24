use crate::primitives::{RowIndex, RowIndexArrowType, RowRangeList};
use arrow::array::{ArrayRef, PrimitiveArray, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use std::sync::Arc;


pub fn build_row_index_array(
    offset: RowIndex,
    len: usize,
    maybe_row_selection: Option<&RowRangeList>
) -> PrimitiveArray<RowIndexArrowType> {
    if let Some(row_ranges) = maybe_row_selection {
        let num_rows = row_ranges.iter()
            .map(|r| r.end - r.start)
            .sum::<RowIndex>() as usize;

        assert!(num_rows <= len);
        let mut array = UInt32Array::builder(num_rows);

        for range in row_ranges.iter() {
            for i in range {
                array.append_value(offset + i)
            }
        }

        array.finish()
    } else {
        (offset..(offset + len as RowIndex)).collect()
    }
}


pub fn add_row_index(batch: &RecordBatch, index: PrimitiveArray<RowIndexArrowType>) -> RecordBatch {
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