use arrow::array::{ArrayRef, RecordBatch};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Series, UnionArgs};
use crate::primitives::RowIndex;


pub fn record_batch_to_polars_df(batch: &RecordBatch) -> anyhow::Result<DataFrame> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        columns.push(Series::from_arrow(
            &schema.fields().get(i).unwrap().name(),
            Box::<dyn polars_arrow::array::Array>::from(&**column),
        )?);
    }
    Ok(DataFrame::from_iter(columns))
}


pub fn record_batch_vec_to_lazy_polars_df(batch_vec: &Vec<RecordBatch>) -> anyhow::Result<LazyFrame> {
    if batch_vec.len() == 0 {
        return Ok(DataFrame::empty().lazy());
    }

    let batches = batch_vec.iter().map(|record_batch| {
        let df = record_batch_to_polars_df(record_batch)?;
        Ok(df.lazy())
    }).collect::<anyhow::Result<Vec<_>>>()?;

    let df = polars::prelude::concat(
        batches.as_slice(),
        UnionArgs::default()
    )?;

    Ok(df)
}


pub fn polars_series_to_row_index_iter(series: &Series) -> impl Iterator<Item = RowIndex> + '_ {
    series.u32().unwrap().into_no_null_iter()
}


pub fn polars_series_to_arrow_array(series: &Series) -> ArrayRef {
    let series = series.rechunk();
    assert_eq!(series.chunks().len(), 1);
    let polars_array = series.to_arrow(0, false);
    ArrayRef::from(polars_array)
}


/// Safety: Call it in the main at the very beginning.
/// See https://doc.rust-lang.org/std/env/fn.set_var.html for more details
pub unsafe fn set_polars_thread_pool_size(n_threads: usize) {
    std::env::set_var("POLARS_MAX_THREADS", format!("{}", n_threads))
}