use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow_buffer::BooleanBuffer;
use proptest::collection::SizeRange;
use proptest::prelude::*;
use std::sync::Arc;


pub fn bitmask(len: impl Into<SizeRange>) -> impl Strategy<Value=BooleanBuffer> {
    prop::collection::vec(any::<bool>(), len).prop_map(BooleanBuffer::from)
}


pub fn boolean(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    bitmask(len).prop_map(|mask| Arc::new(BooleanArray::new(mask, None)) as ArrayRef)
}


pub fn uint32(len: impl Into<SizeRange>) -> impl Strategy<Value=ArrayRef> {
    prop::collection::vec(any::<u32>(), len).prop_map(|vec| {
        let array = UInt32Array::from(vec);
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