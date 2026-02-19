use crate::scan::arrow::IntoArrowScalar;
use anyhow::{anyhow, bail, ensure};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Datum, PrimitiveArray, Scalar};
use arrow::buffer::{BooleanBuffer, Buffer};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{ArrowNativeType, ArrowNativeTypeOp, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use std::hash::Hash;
use std::ops::BitAnd;
use std::sync::Arc;
use crate::scan::IntoArrowArray;


pub type ArrayPredicateRef = Arc<dyn ArrayPredicate>;


pub trait ArrayPredicate: Sync + Send {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray>;

    fn can_evaluate_stats(&self) -> bool {
        false
    }

    fn evaluate_stats(&self, _stats: &ArrayStats) -> anyhow::Result<BooleanArray> {
        bail!("Stats evaluation is not supported by this predicate")
    }
}


#[derive(Clone)]
pub struct ArrayStats {
    pub min: ArrayRef,
    pub max: ArrayRef
}


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

    fn evaluate_stats(&self, stats: &ArrayStats) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(zero_mask(stats.min.len(), true))
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

    fn evaluate_stats(&self, stats: &ArrayStats) -> anyhow::Result<BooleanArray> {
        if self.predicates.len() == 0 {
            return Ok(zero_mask(stats.min.len(), false))
        }
        let mut result_mask = self.predicates[0].evaluate_stats(stats)?;
        for i in 1..self.predicates.len() {
            let m = self.predicates[i].evaluate_stats(stats)?;
            result_mask = arrow::compute::or(&result_mask, &m)?;
        }
        Ok(result_mask)
    }
}


pub fn or(predicates: Vec<ArrayPredicateRef>) -> ArrayPredicateRef {
    if predicates.len() == 1 {
        predicates.into_iter().next().unwrap()
    } else {
        Arc::new(Or::new(predicates))
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
    pub fn new<T: IntoArrowScalar>(value: T) -> Self {
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

    fn evaluate_stats(&self, stats: &ArrayStats) -> anyhow::Result<BooleanArray> {
        let min_array = &stats.min;
        cast_scalar!(value, &self.value, min_array, Less: false, Greater: false);
        let min_boundary = arrow::compute::kernels::cmp::gt_eq(value, min_array)?;
        let max_boundary = arrow::compute::kernels::cmp::lt_eq(value, &stats.max)?;
        Ok(arrow::compute::and(&min_boundary, &max_boundary)?)
    }
}


/// value >= item
pub struct GtEq {
    value: Scalar<ArrayRef>
}


impl GtEq {
    pub fn new<T: IntoArrowScalar>(value: T) -> Self {
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

    fn evaluate_stats(&self, stats: &ArrayStats) -> anyhow::Result<BooleanArray> {
        let min = &stats.min;
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
    pub fn new<T: IntoArrowScalar>(value: T) -> Self {
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

    fn evaluate_stats(&self, stats: &ArrayStats) -> anyhow::Result<BooleanArray> {
        let max = &stats.max;
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
    let target_value = if let Ok(val) = TO::Native::try_from(value) {
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


pub struct InList {
    list: sqd_polars::prelude::Series
}


impl InList {
    pub fn new<L: IntoArrowArray>(values: L) -> Self {
        let arr = values.into_array();
        let list = sqd_polars::arrow::array_series("value_list", &arr).unwrap();
        Self {
            list
        }
    }
}


impl ArrayPredicate for InList {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        let series = sqd_polars::arrow::array_series("values", arr)?;
        let polars_mask = sqd_polars::prelude::is_in(&series, &self.list)?;
        let mask = sqd_polars::arrow::polars_boolean_to_arrow_boolean(&polars_mask);
        Ok(mask)
    }
}


fn bitwise_and<const N: usize>(value: &[u8; N], other: &[u8; N]) -> [u8; N] {
    let mut arr = [0; N];
    for i in 0..N {
        arr[i] = value[i] & other[i];
    }
    arr
}


pub struct BloomFilter {
    value: Buffer
}


impl BloomFilter {
    pub fn new<T: Hash>(byte_size: usize, num_hashes: usize, value: T) -> Self {
        let mut bloom = sqd_bloom_filter::BloomFilter::new(byte_size, num_hashes);
        bloom.insert(&value);
        Self {
            value: Buffer::from(bloom.bytes())
        }
    }

    #[inline(never)]
    fn eval_static<T, const N: usize>(&self, blooms: &[u8]) -> BooleanBuffer
    where
        T: ArrowNativeType + BitAnd<Output=T>
    {
        let value = to_typed_fixed_slice::<T, N>(&self.value);
        let blooms = to_typed_slice::<T>(blooms);
        assert_eq!(blooms.len() % N, 0);
        let len = blooms.len() / N;
        BooleanBuffer::collect_bool(len, |i| unsafe {
            let bloom_ptr = blooms.as_ptr().add(i * N) as *const [T; N];
            let bloom = &*bloom_ptr;
            let mut and: [T; N] = [T::default(); N];
            for i in 0..N {
                and[i] = value[i] & bloom[i];
            }
            &and == value
        })
    }

    #[inline(never)]
    fn eval_dynamic<T>(&self, blooms: &[u8]) -> BooleanBuffer
    where
        T: ArrowNativeType + BitAnd<Output=T>
    {
        let value = to_typed_slice::<T>(&self.value);
        let blooms = to_typed_slice::<T>(blooms);
        assert_eq!(blooms.len() % value.len(), 0);
        let len = blooms.len() / value.len();
        BooleanBuffer::collect_bool(len, |i| unsafe {
            let bloom = blooms.get_unchecked(i..i + value.len());
            value.into_iter().zip(bloom.into_iter()).all(|(&v, &b)| v & b == v)
        })
    }
}


fn to_typed_slice<T: ArrowNativeType>(value: &[u8]) -> &[T] {
    let (prefix, offsets, suffix) = unsafe { value.align_to::<T>() };
    assert!(prefix.is_empty() && suffix.is_empty());
    offsets
}


fn to_typed_fixed_slice<T: ArrowNativeType, const N: usize>(value: &[u8]) -> &[T; N] {
    let slice = to_typed_slice::<T>(value);
    slice.try_into().unwrap()
}


impl ArrayPredicate for BloomFilter {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        let arr = arr.as_fixed_size_binary_opt().ok_or_else(|| {
            anyhow!("expected fixed sized binary array, but got {}", arr.data_type())
        })?;

        let size = self.value.len();

        ensure!(
            arr.value_length() as usize == size,
            "this bloom filter is {} bytes, but array item is {} bytes",
            size,
            arr.value_length()
        );

        let value_range = arr.value_offset(0) as usize..arr.value_offset(arr.len()) as usize;
        let values = &arr.value_data()[value_range];

        let mask = match size {
            64 => self.eval_static::<u128, 4>(values),
            _ => if size % 16 == 0 {
                self.eval_dynamic::<u128>(values)
            } else if size % 8 == 0 {
                self.eval_dynamic::<u64>(values)
            } else {
                self.eval_dynamic::<u8>(values)
            }
        };

        Ok(BooleanArray::new(mask, arr.nulls().cloned()))
    }
}


pub struct PrimitiveListContainsAny<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
}


impl<T: ArrowPrimitiveType> PrimitiveListContainsAny<T> {
    pub fn new(values: &[T::Native]) -> Self {
        Self {
            values: values.iter().copied().collect(),
        }
    }
}


impl<T: ArrowPrimitiveType> ArrayPredicate for PrimitiveListContainsAny<T> {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        let list_array = arr.as_list_opt::<i32>().ok_or_else(|| {
            anyhow!("expected List array, but got {}", arr.data_type())
        })?;

        let values_array = list_array.values().as_primitive_opt::<T>().ok_or_else(|| {
            anyhow!(
                "expected List of primitive values, but got List of {}",
                list_array.values().data_type()
            )
        })?;

        let offsets = list_array.offsets();
        let mask = BooleanBuffer::collect_bool(list_array.len(), |i| {
            if list_array.is_null(i) {
                return false;
            }
            let start = offsets[i].as_usize();
            let end = offsets[i + 1].as_usize();
            for j in start..end {
                if self.values.contains(&values_array.value(j)) {
                    return true;
                }
            }
            false
        });

        Ok(BooleanArray::from(mask))
    }
}


pub struct StringListContainsAny {
    values: Vec<String>,
}


impl StringListContainsAny {
    pub fn new<S: AsRef<str>>(values: &[S]) -> Self {
        Self {
            values: values.iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }
}


impl ArrayPredicate for StringListContainsAny {
    fn evaluate(&self, arr: &dyn Array) -> anyhow::Result<BooleanArray> {
        let list_array = arr.as_list_opt::<i32>().ok_or_else(|| {
            anyhow!("expected List array, but got {}", arr.data_type())
        })?;

        let values_array = list_array.values().as_string_opt::<i32>().ok_or_else(|| {
            anyhow!(
                "expected List of string values, but got List of {}",
                list_array.values().data_type()
            )
        })?;

        let offsets = list_array.offsets();
        let mask = BooleanBuffer::collect_bool(list_array.len(), |i| {
            if list_array.is_null(i) {
                return false;
            }
            let start = offsets[i].as_usize();
            let end = offsets[i + 1].as_usize();
            for j in start..end {
                if self.values.iter().any(|v| v.as_str() == values_array.value(j)) {
                    return true;
                }
            }
            false
        });

        Ok(BooleanArray::from(mask))
    }
}


#[cfg(feature = "_bench")]
mod bench {
    use crate::scan::array_predicate::{ArrayPredicate, BloomFilter, PrimitiveListContainsAny, StringListContainsAny};
    use arrow::array::{FixedSizeBinaryArray, ListArray, ListBuilder, StringBuilder};
    use arrow::buffer::MutableBuffer;
    use arrow::datatypes::UInt32Type;


    #[divan::bench]
    fn bloom_filter(bench: divan::Bencher) {
        let pred = BloomFilter::new(64, 7, "hello");
        let array = FixedSizeBinaryArray::new(
            64,
            MutableBuffer::from_len_zeroed(64 * 200_000).into(),
            None
        );
        bench.bench(|| {
            pred.evaluate(&array).unwrap()
        })
    }


    #[divan::bench]
    fn primitive_list_contains(bench: divan::Bencher) {
        let pred: PrimitiveListContainsAny<UInt32Type> = PrimitiveListContainsAny::new(&vec![0]);
        let array = ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(0), Some(1)]),
            Some(vec![Some(100), Some(200), Some(300), Some(400), Some(500)]),
        ]);
        bench.bench(|| {
            pred.evaluate(&array).unwrap()
        })
    }


    #[divan::bench]
    fn string_list_contains(bench: divan::Bencher) {
        let pred = StringListContainsAny::new(&vec!["0x00000000000000000000000000000000"]);

        let mut list_builder = ListBuilder::new(StringBuilder::new());

        list_builder.values().append_value("0x11111111111111111111111111111111");
        list_builder.values().append_value("0x22222222222222222222222222222222");
        list_builder.values().append_value("0x33333333333333333333333333333333");
        list_builder.append(true);

        list_builder.values().append_value("0x00000000000000000000000000000000");
        list_builder.values().append_value("0x11111111111111111111111111111111");
        list_builder.append(true);

        list_builder.values().append_value("0x11111111111111111111111111111111");
        list_builder.values().append_value("0x22222222222222222222222222222222");
        list_builder.values().append_value("0x33333333333333333333333333333333");
        list_builder.values().append_value("0x44444444444444444444444444444444");
        list_builder.values().append_value("0x55555555555555555555555555555555");
        list_builder.append(true);

        let array = list_builder.finish();

        bench.bench(|| {
            pred.evaluate(&array).unwrap()
        })
    }
}