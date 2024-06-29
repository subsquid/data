use std::collections::BTreeSet;

use arrow::array::{Array, AsArray, PrimitiveArray, RecordBatch};

use crate::primitives::{RowIndex, RowIndexArrowType};
use crate::util::polars_series_to_row_index_iter;


pub struct RowList {
    row_indexes: parking_lot::Mutex<BTreeSet<RowIndex>>
}


impl RowList {
    pub fn new() -> Self {
        Self {
            row_indexes: parking_lot::Mutex::new(BTreeSet::new())
        }
    }

    pub fn extend<I>(&self, rows: I)
        where I: IntoIterator<Item = RowIndex>
    {
        self.row_indexes.lock().extend(rows)
    }

    pub fn extend_from_record_batch_vec(&self, batches: &Vec<RecordBatch>) {
        let row_index_iter = batches.iter().flat_map(|b| {
            let array: &PrimitiveArray<RowIndexArrowType> = b.column_by_name("row_index")
                .expect("No row_index column in the batch")
                .as_primitive();
            assert_eq!(array.null_count(), 0);
            array.values().iter().copied()
        });
        self.extend(row_index_iter)
    }

    pub fn extend_from_polars_df(&self, df: &polars::prelude::DataFrame) {
        let series = df.column("row_index").expect("No row_index column in the data frame");
        let row_index_iter = polars_series_to_row_index_iter(series);
        self.extend(row_index_iter)
    }

    pub fn into_inner(self) -> BTreeSet<RowIndex> {
        self.row_indexes.into_inner()
    }
}