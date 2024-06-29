use std::collections::HashSet;
use std::sync::Arc;

use anyhow::bail;
use arrow::array::{Array, BooleanArray, RecordBatch};

use crate::scan::{array_predicate, row_range};
use crate::scan::array_predicate::{ArrayPredicateRef, ArrayStats};
use crate::scan::row_range::{RowRange, RowRangeList};
use crate::primitives::Name;


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
    fn get_column_stats(&self, column: Name) -> Option<ColumnStatsRef>;
    fn get_num_rows(&self) -> usize;
}


pub trait ColumnStats: ArrayStats {
    fn get_ranges(&self) -> &[RowRange];
    fn as_array_stats(&self) -> &dyn ArrayStats;
}


pub type ColumnStatsRef = Arc<dyn ColumnStats>;


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

    fn evaluate_stats(&self, stats: &dyn RowStats) -> anyhow::Result<Option<RowRangeList>> {
        stats.get_column_stats(self.column[0]).map(|column_stats| {
            let mask = self.array_predicate.evaluate_stats(column_stats.as_array_stats())?;

            let ranges = column_stats.get_ranges()
                .iter()
                .cloned()
                .enumerate()
                .filter(|(i, _r)| {
                    mask.value(*i) && !mask.is_null(*i)
                })
                .map(|(_i, r)| r);

            Ok(row_range::seal(ranges).collect())
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
        let mut selections: Vec<Vec<RowRange>> = Vec::with_capacity(self.predicates.len());
        for p in self.predicates.iter() {
            if p.can_evaluate_stats() {
                if let Some(sel) = p.evaluate_stats(stats)? {
                    selections.push(sel);
                }
            }
        }
        Ok(selections.into_iter().reduce(|a, b| row_range::intersection(a, b).collect()))
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
        let mut selections = Vec::with_capacity(self.predicates.len());
        for p in self.predicates.iter() {
            if p.can_evaluate_stats() {
                if let Some(sel) = p.evaluate_stats(stats)? {
                    selections.push(sel);
                } else {
                    return Ok(None)
                }
            } else {
                return Ok(None)
            }
        }
        Ok(selections.into_iter().reduce(|a, b| row_range::union(a, b).collect()))
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