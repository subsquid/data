use std::collections::HashSet;
use std::sync::Arc;

use anyhow::bail;
use arrow::array::{Array, BooleanArray, RecordBatch, UInt32Array};

use sqd_primitives::RowRangeList;

use crate::primitives::Name;
use crate::scan::array_predicate;
use crate::scan::array_predicate::{ArrayPredicateRef, ArrayStats};


pub type RowPredicateRef = Arc<dyn RowPredicate>;


pub trait RowPredicate: Sync + Send {
    fn projection(&self) -> &[Name];

    fn evaluate(&self, batch: &RecordBatch) -> anyhow::Result<BooleanArray>;

    fn can_evaluate_stats(&self) -> bool {
        false
    }

    fn evaluate_stats(&self, _stats: &dyn RowStats) -> anyhow::Result<Option<RowRangeList>> {
        Ok(None)
    }
}


pub trait RowStats {
    fn get_column_offsets(&self, column: Name) -> anyhow::Result<UInt32Array>;

    fn get_column_stats(&self, column: Name) -> anyhow::Result<Option<ArrayStats>>;

    fn get_num_rows(&self) -> usize;
}


pub struct ColumnPredicate {
    column: [Name; 1],
    array_predicate: ArrayPredicateRef
}


impl ColumnPredicate {
    pub fn new(column_name: Name, array_predicate: ArrayPredicateRef) -> Self {
        Self {
            column: [column_name],
            array_predicate
        }
    }
}


impl RowPredicate for ColumnPredicate {
    fn projection(&self) -> &[Name] {
        &self.column
    }

    fn evaluate(&self, batch: &RecordBatch) -> anyhow::Result<BooleanArray> {
        if let Some(array) = batch.column_by_name(self.column[0]) {
            self.array_predicate.evaluate(array)
        } else {
            bail!("missing column: {}", self.column[0])
        }
    }

    fn can_evaluate_stats(&self) -> bool {
        self.array_predicate.can_evaluate_stats()
    }

    fn evaluate_stats(&self, row_stats: &dyn RowStats) -> anyhow::Result<Option<RowRangeList>> {
        row_stats.get_column_stats(self.column[0])?.map(|column_stats| {
            let mask = self.array_predicate.evaluate_stats(&column_stats)?;

            let offsets = row_stats.get_column_offsets(self.column[0])?;

            let ranges = (0..offsets.len() - 1)
                .filter(|&i| {
                    mask.value(i) && !mask.is_null(i)
                })
                .map(|i| offsets.value(i)..offsets.value(i+1));

            Ok(RowRangeList::seal(ranges))
        }).transpose()
    }
}


pub struct AndPredicate {
    predicates: Vec<RowPredicateRef>,
    projection: Vec<Name>
}


impl AndPredicate {
    pub fn new(predicates: Vec<RowPredicateRef>) -> Self {
        let projection = predicates_projection(&predicates);
        Self {
            predicates,
            projection
        }
    }
}


impl RowPredicate for AndPredicate {
    fn projection(&self) -> &[Name] {
        &self.projection
    }

    fn evaluate(&self, batch: &RecordBatch) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
           return Ok(array_predicate::zero_mask(batch.num_rows(), true))
        }
        let mut result_mask = self.predicates[0].evaluate(batch)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate(batch)?;
            result_mask = arrow::compute::and(&result_mask, &m)?;
        }
        Ok(result_mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        self.predicates.iter().any(|p| p.can_evaluate_stats())
    }

    fn evaluate_stats(&self, stats: &dyn RowStats) -> anyhow::Result<Option<RowRangeList>> {
        let mut selection: Option<RowRangeList> = None;
        for p in self.predicates.iter() {
            if p.can_evaluate_stats() {
                if let Some(sel) = p.evaluate_stats(stats)? {
                    selection = Some(if let Some(prev) = selection {
                        prev.intersection(&sel)
                    } else {
                        sel
                    })
                }
            }
        }
        Ok(selection)
    }
}


pub struct OrPredicate {
    predicates: Vec<RowPredicateRef>,
    projection: Vec<Name>
}


impl OrPredicate {
    pub fn new(predicates: Vec<RowPredicateRef>) -> Self {
        let projection = predicates_projection(&predicates);
        Self {
            predicates,
            projection
        }
    }
}


impl RowPredicate for OrPredicate {
    fn projection(&self) -> &[Name] {
        &self.projection
    }

    fn evaluate(&self, batch: &RecordBatch) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(array_predicate::zero_mask(batch.num_rows(), false))
        }
        let mut result_mask = self.predicates[0].evaluate(batch)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate(batch)?;
            result_mask = arrow::compute::or(&result_mask, &m)?;
        }
        Ok(result_mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        self.predicates.iter().all(|p| p.can_evaluate_stats())
    }

    fn evaluate_stats(&self, stats: &dyn RowStats) -> anyhow::Result<Option<RowRangeList>> {
        let mut selection: Option<RowRangeList> = None;
        for p in self.predicates.iter() {
            if p.can_evaluate_stats() {
                if let Some(sel) = p.evaluate_stats(stats)? {
                    selection = Some(if let Some(prev) = selection {
                        prev.union(&sel)
                    } else {
                        sel
                    })
                } else {
                    return Ok(None)
                }
            } else {
                return Ok(None)
            }
        }
        Ok(selection)
    }
}


fn predicates_projection(predicates: &[RowPredicateRef]) -> Vec<Name> {
    let n_columns = predicates.iter().map(|p| p.projection().len()).sum();
    let mut projected_set: HashSet<Name> = HashSet::with_capacity(n_columns);
    let mut projection: Vec<Name> = Vec::with_capacity(n_columns);
    for name in predicates.iter().flat_map(|p| p.projection()) {
        if !projected_set.contains(name) {
            projection.push(*name);
            projected_set.insert(*name);
        }
    }
    projection
}