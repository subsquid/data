use std::collections::HashSet;

use arrow::array::RecordBatch;

use sqd_primitives::RowRangeList;
use sqd_storage::db::ChunkTableReader;

use crate::primitives::Name;
use crate::scan::reader::TableReader;
use crate::scan::RowPredicateRef;


impl <'a> TableReader for ChunkTableReader<'a> {
    fn read(
        &self,
        predicate: Option<RowPredicateRef>,
        projection: Option<&HashSet<Name>>,
        row_selection: Option<&RowRangeList>,
        with_row_index: bool
    ) -> anyhow::Result<Vec<RecordBatch>> {
        todo!()
    }
}