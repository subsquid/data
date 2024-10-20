use crate::builder::AnyStructBuilder;
use crate::slice::{AsSlice, Slice};
use crate::sort::sort_table_to_indexes;
use arrow::array::RecordBatch;


pub fn sort_record_batch<'a>(
    records: &'a RecordBatch,
    by_columns: impl IntoIterator<Item=&'a str>
) -> anyhow::Result<RecordBatch>
{
    let columns = by_columns.into_iter().map(|name| {
        Ok(records.schema_ref().index_of(name)?)
    }).collect::<anyhow::Result<Vec<_>>>()?;
    
    sort_record_batch_impl(records, &columns)
}


#[inline(never)]
fn sort_record_batch_impl(records: &RecordBatch, columns: &[usize]) -> anyhow::Result<RecordBatch> {
    let slice = records.as_slice();
    
    let indexes = sort_table_to_indexes(&slice, &columns);
    
    let mut builder = AnyStructBuilder::new(records.schema_ref().fields().clone());
    slice.write_indexes(&mut builder, indexes.iter().copied())?;
    
    let array = unsafe {
        builder.finish_unchecked()
    };
    
    Ok(array.into())
}