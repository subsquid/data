use crate::primitives::Name;
use crate::scan::array_predicate::ArrayPredicateRef;
use crate::scan::arrow::IntoArrowScalar;
use crate::scan::row_predicate::{AndPredicate, ColumnPredicate, OrPredicate, RowPredicateRef};
use crate::scan::{array_predicate, IntoArrowArray};
use arrow::array::{Array, Scalar};
use std::hash::Hash;
use std::sync::Arc;


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


pub fn col_eq<T: IntoArrowScalar>(name: Name, value: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::Eq::new(value))
}


pub fn col_in_list<L: IntoArrowArray>(name: Name, values: L) -> RowPredicateRef {
    let values = values.into_array();
    match values.len() {
        1 => col_eq(name, Scalar::new(values)),
        0 | 2..10 => {
            make_column_predicate!(
                name,
                array_predicate::Or::new(
                    (0..values.len()).map(|i| {
                        let val = Scalar::new(values.slice(i, 1));
                        Arc::new(array_predicate::Eq::new(val)) as ArrayPredicateRef
                    }).collect()
                )
            )
        },
        _ => {
            make_column_predicate!(
                name,
                array_predicate::InList::new(values)
            )
        }
    }
}


/// column <= value
pub fn col_lt_eq<T: IntoArrowScalar>(name: Name, value: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::GtEq::new(value))
}


/// column >= value
pub fn col_gt_eq<T: IntoArrowScalar>(name: Name, value: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::LtEq::new(value))
}


/// low <= column <= high
pub fn col_between<T: IntoArrowScalar>(name: Name, low: T, high: T) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::And::new(vec![
        Arc::new(array_predicate::LtEq::new(low)),
        Arc::new(array_predicate::GtEq::new(high))
    ]))
}


pub fn bloom_filter<L>(
    name: Name, 
    bytes_size: usize, 
    num_hashes: usize, 
    values: L
) -> RowPredicateRef
where 
    L: IntoIterator<Item: Hash>
{
    let array_pred = array_predicate::or(
        values.into_iter().map(|val| {
            Arc::new(array_predicate::BloomFilter::new(
                bytes_size,
                num_hashes,
                val
            )) as ArrayPredicateRef
        }).collect()
    );
    Arc::new(ColumnPredicate::new(name, array_pred))
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
    if predicates.len() == 1 {
        predicates.into_iter().next().unwrap()
    } else {
        Arc::new(
            OrPredicate::new(predicates)
        )
    }
}


pub fn col_primitive_list_contains_any<T>(name: Name, values: &[T::Native]) -> RowPredicateRef
where
    T: arrow::datatypes::ArrowPrimitiveType,
    T::Native: Eq + std::hash::Hash,
{
    make_column_predicate!(name, array_predicate::PrimitiveListContainsAny::<T>::new(values))
}


pub fn col_string_list_contains_any<S: AsRef<str>>(name: Name, values: &[S]) -> RowPredicateRef {
    make_column_predicate!(name, array_predicate::StringListContainsAny::new(values))
}
