use std::sync::Arc;

use crate::scan::array_predicate;
use crate::scan::array_predicate::ArrayPredicateRef;
use crate::scan::row_predicate::{AndPredicate, ColumnPredicate, OrPredicate, RowPredicateRef};
use crate::scan::arrow::IntoScalar;
use crate::primitives::Name;


macro_rules! make_column_predicate {
    ($col:expr, $arr_predicate:expr) => {
        Arc::new(
            ColumnPredicate::new(
                $col,
                Arc::new($arr_predicate)
            )
        )
    };
}


pub fn col_eq<T: IntoScalar>(name: Name, value: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::Eq::new(value))
}


pub fn col_in_list<T: IntoScalar>(name: Name, values: Vec<T>) -> RowPredicateRef {
    match values.len() {
        1 => col_eq(name, values.into_iter().next().unwrap()),
        _ => {
            make_column_predicate!(
                name,
                array_predicate::Or::new(
                    values.into_iter().map(|v| {
                        Arc::new(array_predicate::Eq::new(v)) as ArrayPredicateRef
                    }).collect()
                )
            )
        }
    }
}


/// column <= value
pub fn col_lt_eq<T: IntoScalar>(name: Name, value: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::GtEq::new(value))
}


/// column >= value
pub fn col_gt_eq<T: IntoScalar>(name: Name, value: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::LtEq::new(value))
}


// low <= column <= high
pub fn col_between<T: IntoScalar>(name: Name, low: T, high: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::And::new(vec![
        Arc::new(array_predicate::LtEq::new(low)),
        Arc::new(array_predicate::GtEq::new(high))
    ]))
}


pub fn and(predicates: Vec<RowPredicateRef>) -> RowPredicateRef {
    if predicates.len() == 1 {
        predicates.into_iter().next().unwrap()
    } else {
        Arc::new(
            AndPredicate::new(predicates)
        )
    }
}


pub fn or(predicates: Vec<RowPredicateRef>) -> RowPredicateRef {
    Arc::new(
        OrPredicate::new(predicates)
    )
}