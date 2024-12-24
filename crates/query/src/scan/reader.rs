use crate::primitives::{Name, RowRangeList};
use crate::scan::RowPredicateRef;
use arrow::array::RecordBatch;
use std::collections::HashSet;


pub trait TableReader {
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool
    ) -> anyhow::Result<Vec<RecordBatch>>;
}
