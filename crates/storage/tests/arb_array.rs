use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, StringArray, StructArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow_buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Field};
use proptest::bool::Any;
use proptest::collection::SizeRange;
use proptest::prelude::*;
use proptest::string::string_regex;
use std::sync::Arc;
use arrow::datatypes::Int32Type;


pub fn bitmask(len: impl Into<SizeRange>) -> impl Strategy<Value=BooleanBuffer> {
    prop::collection::vec(any::<bool>(), len).prop_map(BooleanBuffer::from)
}

pub fn boolean(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<bool>(), len).prop_map(|vec| {
        let array = BooleanArray::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn binary(len: impl Into<SizeRange> + Clone) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), len.clone()), len).prop_map(|vecs| {
        let res = vecs.iter().map(Vec::as_slice).collect();
        let array = BinaryArray::from_vec(res);
        Arc::new(array) as ArrayRef
    })
}

pub fn list(len: impl Into<SizeRange> + Clone) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(prop::collection::vec(any::<Option<i32>>(), len.clone()), len).prop_map(|vecs| {
        let res = vecs.iter().map(|x| Some(x.clone())).collect::<Vec<_>>();
        let array = ListArray::from_iter_primitive::<Int32Type, _, _>(res);
        Arc::new(array) as ArrayRef
    })
}

pub fn string(len: impl Into<SizeRange>) -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(string_regex("\\PC*").unwrap(), len).prop_map(|strings| {
        let array = StringArray::from(strings);
        Arc::new(array) as ArrayRef
    })
}

pub fn uint8(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<u8>(), len).prop_map(|vec| {
        let array = UInt8Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn uint16(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<u16>(), len).prop_map(|vec| {
        let array = UInt16Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn uint32(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<u32>(), len).prop_map(|vec| {
        let array = UInt32Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn uint64(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<u64>(), len).prop_map(|vec| {
        let array = UInt64Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn int8(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<i8>(), len).prop_map(|vec| {
        let array = Int8Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn int16(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<i16>(), len).prop_map(|vec| {
        let array = Int16Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn int32(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<i32>(), len).prop_map(|vec| {
        let array = Int32Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn int64(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<i64>(), len).prop_map(|vec| {
        let array = Int64Array::from(vec);
        Arc::new(array) as ArrayRef
    })
}

pub fn timestamp(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<i64>(), len).prop_map(|vec| {
        let array = TimestampSecondArray::from(vec);
        Arc::new(array) as ArrayRef
    })
}


pub fn structs(len: impl Into<SizeRange>) -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(string_regex("\\PC*").unwrap(), len).prop_map(|strings| {
        let arrow_strings = Arc::new(StringArray::from(strings));
        let array = StructArray::from(vec![
            (
                Arc::new(Field::new("s", DataType::Utf8, false)),
                arrow_strings as ArrayRef,
            ),
        ]);
        Arc::new(array) as ArrayRef
    })
}


pub fn with_nullmask(array: impl Strategy<Value=ArrayRef>) -> impl Strategy<Value=ArrayRef> {
    array.prop_flat_map(|arr| {
        bitmask(arr.len()).prop_map(move |nulls| {
            arr.to_data()
                .into_builder()
                .nulls(Some(nulls.into()))
                .build()
                .map(arrow::array::make_array)
                .unwrap()
        })
    })
}