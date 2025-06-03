use arrow::array::{ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, Scalar, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use std::sync::Arc;


pub trait IntoArrowScalar: Sized {
    fn into_scalar(self) -> Scalar<ArrayRef>;
}


impl IntoArrowScalar for Scalar<ArrayRef> {
    fn into_scalar(self) -> Scalar<ArrayRef> {
        self
    }
}


pub trait IntoArrowArray {
    fn into_array(self) -> ArrayRef;
}


impl IntoArrowArray for ArrayRef {
    fn into_array(self) -> ArrayRef {
        self
    }
}


macro_rules! imp {
    ($t:ty, $arr_type:ty) => {
        impl IntoArrowScalar for $t {
            fn into_scalar(self) -> Scalar<ArrayRef> {
                let arr = <$arr_type>::from(vec![self]);
                Scalar::new(Arc::new(arr))
            }
        }

        impl IntoArrowArray for Vec<$t> {
            fn into_array(self) -> ArrayRef {
                let arr = <$arr_type>::from(self);
                Arc::new(arr)
            }
        }
    };
}


imp!(bool, BooleanArray);
imp!(u8, UInt8Array);
imp!(u16, UInt16Array);
imp!(u32, UInt32Array);
imp!(u64, UInt64Array);
imp!(i8, Int8Array);
imp!(i16, Int16Array);
imp!(i32, Int32Array);
imp!(i64, Int64Array);
imp!(&[u8], BinaryArray);
imp!(&str, StringArray);
imp!(String, StringArray);


impl IntoArrowArray for &[String] {
    fn into_array(self) -> ArrayRef {
        let arr = StringArray::from_iter(self.iter().map(Some));
        Arc::new(arr)
    }
}


macro_rules! impl_into_ref {
    ($($t:ty),*) => {
        $(
        impl IntoArrowArray for $t {
            fn into_array(self) -> ArrayRef {
                Arc::new(self)
            }
        }
        )*
    };
}


impl_into_ref!(
    UInt8Array,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    BooleanArray,
    StringArray,
    FixedSizeBinaryArray
);