use arrow::array::{Array, ArrayRef, RecordBatch};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, Series, UnionArgs};
use polars_core::prelude::{CompatLevel, SortMultipleOptions};


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


pub fn record_batch_vec_to_lazy_polars_df(batch_vec: &[RecordBatch]) -> anyhow::Result<LazyFrame> {
    Ok(match batch_vec.len() {
        0 => DataFrame::empty().lazy(),
        1 => {
            let b = &batch_vec[0];
            let df = record_batch_to_polars_df(b)?;
            df.lazy()
        },
        _ => {
            let batches = batch_vec.iter().map(|record_batch| {
                let df = record_batch_to_polars_df(record_batch)?;
                Ok(df.lazy())
            }).collect::<anyhow::Result<Vec<_>>>()?;

            polars::prelude::concat(
                batches.as_slice(),
                UnionArgs::default()
            )?
        }
    })
}


pub fn polars_series_to_row_index_iter(series: &Series) -> impl Iterator<Item = u32> + '_ {
    series.u32().unwrap().into_no_null_iter()
}


pub fn polars_series_to_arrow_array(series: &Series) -> ArrayRef {
    let series = series.rechunk();
    assert_eq!(series.chunks().len(), 1);
    let polars_array = series.to_arrow(0, CompatLevel::oldest());
    ArrayRef::from(polars_array)
}


pub fn sort_record_batch(record_batch: &RecordBatch, by: Vec<String>) -> anyhow::Result<RecordBatch> {
    let df = record_batch_to_polars_df(record_batch)?;
    
    let sorted_df = df.sort(
        by, 
        SortMultipleOptions::default().with_multithreaded(false)
    )?;
    
    let schema = record_batch.schema();
    
    let columns: Vec<ArrayRef> = sorted_df.iter().enumerate().map(|(i, s)| {
        let array = polars_series_to_arrow_array(s);
        if array.data_type() == schema.field(i).data_type() {
            array
        } else {
            arrow::compute::cast(&array, schema.field(i).data_type()).unwrap()
        }
    }).collect();
    
    let sorted_batch = RecordBatch::try_new(schema, columns)?;
    
    Ok(sorted_batch)
}