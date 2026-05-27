use std::collections::HashSet;

use arrow::{array::RecordBatch, datatypes::SchemaRef};

use crate::{
    primitives::{Name, RowRangeList},
    scan::RowPredicateRef
};

pub trait TableReader {
    /// Reads record batches with optional filtering, projection, and row selection.
    ///
    /// When `default_null_columns` is provided, columns listed there that are
    /// missing from the underlying data source should be treated as all-null
    /// columns rather than causing an error. This applies to both projection
    /// and predicate columns.
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool,
        default_null_columns: Option<&HashSet<Name>>
    ) -> anyhow::Result<Vec<RecordBatch>>;

    fn schema(&self) -> SchemaRef;
}
