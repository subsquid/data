use std::sync::Arc;

use anyhow::bail;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Datum, PrimitiveArray, Scalar};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{ArrowNativeTypeOp, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};

use crate::scan::arrow::IntoArrow;


pub trait ArrayPredicate: Sync + Send {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray>;

    fn can_evaluate_stats(&self) -> bool {
        false
    }

    fn evaluate_stats(&self, _stats: &dyn ArrayStats) -> anyhow::Result<BooleanArray> {
        bail!("Stats evaluation is not supported by this predicate")
    }
}


pub trait ArrayStats: Sync + Send {
    fn get_min(&self) -> ArrayRef;
    fn get_max(&self) -> ArrayRef;
}


pub type ArrayPredicateRef = Arc<dyn ArrayPredicate>;


pub struct And {
    predicates: Vec<ArrayPredicateRef>
}


impl And {
    pub fn new(predicates: Vec<ArrayPredicateRef>) -> Self {
        Self {
            predicates
        }
    }
}


impl ArrayPredicate for And {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(zero_mask(arr.len(), true))
        }
        let mut result_mask = self.predicates[0].evaluate(arr)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate(arr)?;
            result_mask = arrow::compute::and(&result_mask, &m)?;
        }
        Ok(result_mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        self.predicates.iter().any(|p| p.can_evaluate_stats())
    }

    fn evaluate_stats(&self, stats: &dyn ArrayStats) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(zero_mask(stats.get_max().len(), true))
        }
        let mut result_mask = self.predicates[0].evaluate_stats(stats)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate_stats(stats)?;
            result_mask = arrow::compute::and(&result_mask, &m)?;
        }
        Ok(result_mask)
    }
}


pub struct Or {
    predicates: Vec<ArrayPredicateRef>
}


impl Or {
    pub fn new(predicates: Vec<ArrayPredicateRef>) -> Self {
        Or {
            predicates
        }
    }
}


impl ArrayPredicate for Or {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(zero_mask(arr.len(), false))
        }
        let mut result_mask = self.predicates[0].evaluate(arr)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate(arr)?;
            result_mask = arrow::compute::or(&result_mask, &m)?;
        }
        Ok(result_mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        self.predicates.iter().all(|p| p.can_evaluate_stats())
    }

    fn evaluate_stats(&self, stats: &dyn ArrayStats) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(zero_mask(stats.get_max().len(), false))
        }
        let mut result_mask = self.predicates[0].evaluate_stats(stats)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate_stats(stats)?;
            result_mask = arrow::compute::or(&result_mask, &m)?;
        }
        Ok(result_mask)
    }
}


macro_rules! cast_scalar {
    ($value:ident, $scalar:expr, $arr:ident, Less: $less:literal, Greater: $greater:literal) => {
        let scalar = $scalar;
        let cast_result = cast_scalar(scalar, $arr.data_type())?;
        let $value = match &cast_result {
            CastResult::Same => scalar,
            CastResult::Cast(value) => value,
            CastResult::Less => return Ok(zero_mask($arr.len(), $less)),
            CastResult::Greater => return Ok(zero_mask($arr.len(), $greater)),
        };
    };
}


pub struct Eq {
    value: Scalar<ArrayRef>
}


impl Eq {
    pub fn new<T: IntoArrow>(value: T) -> Self {
        Self {
            value: value.into_scalar()
        }
    }
}


impl ArrayPredicate for Eq {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        cast_scalar!(value, &self.value, arr, Less: false, Greater: false);
        let mask = arrow::compute::kernels::cmp::eq(&arr, value)?;
        Ok(mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        true
    }

    fn evaluate_stats(&self, stats: &dyn ArrayStats) -> anyhow::Result<BooleanArray> {
        let min_array = &stats.get_min();
        cast_scalar!(value, &self.value, min_array, Less: false, Greater: false);
        let min_boundary = arrow::compute::kernels::cmp::gt_eq(value, min_array)?;
        let max_boundary = arrow::compute::kernels::cmp::lt_eq(value, &stats.get_max())?;
        Ok(arrow::compute::and(&min_boundary, &max_boundary)?)
    }
}


/// value >= item
pub struct GtEq {
    value: Scalar<ArrayRef>
}


impl GtEq {
    pub fn new<T: IntoArrow>(value: T) -> Self {
        Self {
            value: value.into_scalar()
        }
    }
}


impl ArrayPredicate for GtEq {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        cast_scalar!(value, &self.value, arr, Less: false, Greater: true);
        let result_mask = arrow::compute::kernels::cmp::gt_eq(value, &arr)?;
        Ok(result_mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        true
    }

    fn evaluate_stats(&self, stats: &dyn ArrayStats) -> anyhow::Result<BooleanArray> {
        let min = &stats.get_min();
        cast_scalar!(value, &self.value, min, Less: false, Greater: true);
        let result_mask = arrow::compute::kernels::cmp::gt_eq(value, min)?;
        Ok(result_mask)
    }
}


/// value <= item
pub struct LtEq {
    value: Scalar<ArrayRef>
}


impl LtEq {
    pub fn new<T: IntoArrow>(value: T) -> Self {
        Self {
            value: value.into_scalar()
        }
    }
}


impl ArrayPredicate for LtEq {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        cast_scalar!(value, &self.value, arr, Less: true, Greater: false);
        let result_mask = arrow::compute::kernels::cmp::lt_eq(value, &arr)?;
        Ok(result_mask)
    }

    fn can_evaluate_stats(&self) -> bool {
        true
    }

    fn evaluate_stats(&self, stats: &dyn ArrayStats) -> anyhow::Result<BooleanArray> {
        let max = &stats.get_max();
        cast_scalar!(value, &self.value, max, Less: true, Greater: false);
        let result_mask = arrow::compute::kernels::cmp::lt_eq(value, max)?;
        Ok(result_mask)
    }
}


pub fn zero_mask(len: usize, is_set: bool) -> BooleanArray {
    let buf = if is_set {
        BooleanBuffer::new_set(len)
    } else {
        BooleanBuffer::new_unset(len)
    };
    BooleanArray::from(buf)
}


enum CastResult {
    Less,
    Greater,
    Same,
    Cast(Scalar<ArrayRef>)
}


fn cast_scalar(scalar: &Scalar<ArrayRef>, target_domain: &DataType) -> anyhow::Result<CastResult> {
    let array = scalar.get().0;

    if array.data_type() == target_domain {
        return Ok(CastResult::Same)
    }

    if array.data_type().is_integer() && target_domain.is_integer() {
        return Ok(tower_cast(array, target_domain))
    }

    let new_array = cast_with_options(array, target_domain, &CastOptions {
        safe: false,
        ..CastOptions::default()
    })?;

    Ok(CastResult::Cast(Scalar::new(new_array)))
}


fn tower_cast(array: &dyn Array, target_domain: &DataType) -> CastResult {
    macro_rules! cast {
        ($from:ty, $to:ty, $common:ty) => {
            tower_cast_impl::<$from, $to, $common>(array)
        };
    }
    
    match (array.data_type(), target_domain) {
        (DataType::UInt64, DataType::UInt32) => cast!(UInt64Type, UInt32Type, u64),
        (DataType::UInt64, DataType::UInt16) => cast!(UInt64Type, UInt16Type, u64),
        (DataType::UInt64, DataType::UInt8) => cast!(UInt64Type, UInt8Type, u64),
        (DataType::UInt64, DataType::Int64) => cast!(UInt64Type, Int64Type, i128),
        (DataType::UInt64, DataType::Int32) => cast!(UInt64Type, Int32Type, i128),
        (DataType::UInt64, DataType::Int16) => cast!(UInt64Type, Int16Type, i128),
        (DataType::UInt64, DataType::Int8) => cast!(UInt64Type, Int8Type, i128),

        (DataType::UInt32, DataType::UInt64) => cast!(UInt32Type, UInt64Type, u64),
        (DataType::UInt32, DataType::UInt16) => cast!(UInt32Type, UInt16Type, u32),
        (DataType::UInt32, DataType::UInt8) => cast!(UInt32Type, UInt8Type, u32),
        (DataType::UInt32, DataType::Int64) => cast!(UInt32Type, Int64Type, i64),
        (DataType::UInt32, DataType::Int32) => cast!(UInt32Type, Int32Type, i64),
        (DataType::UInt32, DataType::Int16) => cast!(UInt32Type, Int16Type, i64),
        (DataType::UInt32, DataType::Int8) => cast!(UInt32Type, Int8Type, i64),

        (DataType::UInt16, DataType::UInt64) => cast!(UInt16Type, UInt64Type, u64),
        (DataType::UInt16, DataType::UInt32) => cast!(UInt16Type, UInt32Type, u32),
        (DataType::UInt16, DataType::UInt8) => cast!(UInt16Type, UInt8Type, u16),
        (DataType::UInt16, DataType::Int64) => cast!(UInt16Type, Int64Type, i64),
        (DataType::UInt16, DataType::Int32) => cast!(UInt16Type, Int32Type, i32),
        (DataType::UInt16, DataType::Int16) => cast!(UInt16Type, Int16Type, i32),
        (DataType::UInt16, DataType::Int8) => cast!(UInt16Type, Int8Type, i32),

        (DataType::UInt8, DataType::UInt64) => cast!(UInt8Type, UInt64Type, u64),
        (DataType::UInt8, DataType::UInt32) => cast!(UInt8Type, UInt32Type, u32),
        (DataType::UInt8, DataType::UInt16) => cast!(UInt8Type, UInt16Type, u16),
        (DataType::UInt8, DataType::Int64) => cast!(UInt8Type, Int64Type, i64),
        (DataType::UInt8, DataType::Int32) => cast!(UInt8Type, Int32Type, i32),
        (DataType::UInt8, DataType::Int16) => cast!(UInt8Type, Int16Type, i16),
        (DataType::UInt8, DataType::Int8) => cast!(UInt8Type, Int8Type, i16),

        (DataType::Int64, DataType::UInt64) => cast!(Int64Type, UInt64Type, i128),
        (DataType::Int64, DataType::UInt32) => cast!(Int64Type, UInt32Type, i64),
        (DataType::Int64, DataType::UInt16) => cast!(Int64Type, UInt16Type, i64),
        (DataType::Int64, DataType::UInt8) => cast!(Int64Type, UInt8Type, i64),
        (DataType::Int64, DataType::Int32) => cast!(Int64Type, Int32Type, i64),
        (DataType::Int64, DataType::Int16) => cast!(Int64Type, Int16Type, i64),
        (DataType::Int64, DataType::Int8) => cast!(Int64Type, Int8Type, i64),

        (DataType::Int32, DataType::UInt64) => cast!(Int32Type, UInt64Type, i128),
        (DataType::Int32, DataType::UInt32) => cast!(Int32Type, UInt32Type, i64),
        (DataType::Int32, DataType::UInt16) => cast!(Int32Type, UInt16Type, i32),
        (DataType::Int32, DataType::UInt8) => cast!(Int32Type, UInt8Type, i32),
        (DataType::Int32, DataType::Int64) => cast!(Int32Type, Int64Type, i64),
        (DataType::Int32, DataType::Int16) => cast!(Int32Type, Int16Type, i32),
        (DataType::Int32, DataType::Int8) => cast!(Int32Type, Int8Type, i32),

        (DataType::Int16, DataType::UInt64) => cast!(Int16Type, UInt64Type, i128),
        (DataType::Int16, DataType::UInt32) => cast!(Int16Type, UInt32Type, i64),
        (DataType::Int16, DataType::UInt16) => cast!(Int16Type, UInt16Type, i32),
        (DataType::Int16, DataType::UInt8) => cast!(Int16Type, UInt8Type, i16),
        (DataType::Int16, DataType::Int64) => cast!(Int16Type, Int64Type, i64),
        (DataType::Int16, DataType::Int32) => cast!(Int16Type, Int32Type, i32),
        (DataType::Int16, DataType::Int8) => cast!(Int16Type, Int8Type, i16),

        (DataType::Int8, DataType::UInt64) => cast!(Int8Type, UInt64Type, i128),
        (DataType::Int8, DataType::UInt32) => cast!(Int8Type, UInt32Type, i64),
        (DataType::Int8, DataType::UInt16) => cast!(Int8Type, UInt16Type, i32),
        (DataType::Int8, DataType::UInt8) => cast!(Int8Type, UInt8Type, i16),
        (DataType::Int8, DataType::Int64) => cast!(Int8Type, Int64Type, i64),
        (DataType::Int8, DataType::Int32) => cast!(Int8Type, Int32Type, i32),
        (DataType::Int8, DataType::Int16) => cast!(Int8Type, Int16Type, i16),

        (from, to) => panic!("unexpected cast from {} to {}", from, to)
    }
}


fn tower_cast_impl<FROM, TO, C>(array: &dyn Array) -> CastResult
    where FROM: ArrowPrimitiveType,
          TO: ArrowPrimitiveType,
          TO::Native: TryFrom<FROM::Native>,
          C: From<FROM::Native>,
          C: From<TO::Native>,
          C: Ord
{
    let value = array.as_primitive::<FROM>().value(0);
    let target_value = if let Some(val) = TO::Native::try_from(value).ok() {
        val
    } else {
        let value = C::from(value);
        let max = C::from(TO::Native::MAX_TOTAL_ORDER);
        let min = C::from(TO::Native::MIN_TOTAL_ORDER);
        return if value > max {
            CastResult::Greater
        } else {
            assert!(value < min);
            CastResult::Less
        }
    };
    let scalar = Scalar::new(
        Arc::new(
            PrimitiveArray::<TO>::from_value(target_value, 1)
        ) as Arc<dyn Array>
    );
    CastResult::Cast(scalar)
}