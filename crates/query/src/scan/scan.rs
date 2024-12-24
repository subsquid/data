use crate::primitives::{Name, RowRangeList};
use crate::scan::reader::TableReader;
use crate::scan::RowPredicateRef;
use arrow::array::RecordBatch;
use sqd_polars::arrow::record_batch_vec_to_lazy_polars_df;
use std::collections::HashSet;
use std::sync::Arc;


pub struct Scan<'a> {
    reader: Arc<dyn TableReader + 'a>,
    predicate: Option<RowPredicateRef>,
    projection: Option<HashSet<Name>>,
    row_selection: Option<RowRangeList>,
    row_index: bool
}


impl <'a> Scan<'a> {
    pub fn new(reader: Arc<dyn TableReader + 'a>) -> Self {
        Self {
            reader,
            predicate: None,
            projection: None,
            row_selection: None,
            row_index: false
        }
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

    pub fn with_row_selection<S: Into<Option<RowRangeList>>>(mut self, row_selection: S) -> Self {
        self.row_selection = row_selection.into();
        self
    }

    pub fn execute(&self) -> anyhow::Result<Vec<RecordBatch>> {
        self.reader.read(
            self.predicate.clone(),
            self.projection.as_ref(),
            self.row_selection.as_ref(),
            self.row_index
        )
    }

    pub fn to_lazy_df(&self) -> anyhow::Result<sqd_polars::prelude::LazyFrame> {
        let batches = self.execute()?;
        record_batch_vec_to_lazy_polars_df(&batches)
    }
}