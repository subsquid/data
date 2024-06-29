use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, Scalar, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};


pub trait IntoArrow {
    fn into_scalar(self) -> Scalar<ArrayRef>;

    fn vec_into_array(values: Vec<Self>) -> ArrayRef
        where Self: Sized;
}


macro_rules! impl_scalar {
    ($t:ty, $arr_type:ty) => {
        impl IntoArrow for $t {
            fn into_scalar(self) -> Scalar<ArrayRef> {
                let arr = <$arr_type>::from(vec![self]);
                Scalar::new(Arc::new(arr))
            }

            fn vec_into_array(values: Vec<Self>) -> ArrayRef {
                let arr = <$arr_type>::from(values);
                Arc::new(arr)
            }
        }
    };
}


impl_scalar!(bool, BooleanArray);

impl_scalar!(u8, UInt8Array);
impl_scalar!(u16, UInt16Array);
impl_scalar!(u32, UInt32Array);
impl_scalar!(u64, UInt64Array);

impl_scalar!(i8, Int8Array);
impl_scalar!(i16, Int16Array);
impl_scalar!(i32, Int32Array);
impl_scalar!(i64, Int64Array);

impl_scalar!(&str, StringArray);
impl_scalar!(String, StringArray);