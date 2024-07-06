use std::collections::HashSet;
use arrow::array::RecordBatch;
use sqd_primitives::RowRangeList;
use crate::primitives::Name;
use crate::scan::RowPredicateRef;


pub trait TableReader {
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool
    ) -> anyhow::Result<Vec<RecordBatch>>;
}
