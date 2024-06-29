use std::cmp::max;
use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use crate::util;


pub fn get_byte_size(array: &dyn Array) -> usize {
    macro_rules! prim {
        ($t:ty) => {
            array.len() * std::mem::size_of::<$t>() + bit_mask_size(array)
        };
    }

    match array.data_type() {
        DataType::Null => 0,
        DataType::Boolean => array.len() / 8 + bit_mask_size(array),
        DataType::Int8 => prim!(i8),
        DataType::Int16 => prim!(i16),
        DataType::Int32 => prim!(i32),
        DataType::Int64 => prim!(i64),
        DataType::UInt8 => prim!(u8),
        DataType::UInt16 => prim!(u16),
        DataType::UInt32 => prim!(u32),
        DataType::UInt64 => prim!(u64),
        DataType::Timestamp(_, _) => prim!(i64),
        DataType::Binary => {
            let binary = array.as_binary::<i32>();
            binary.values().len() + std::mem::size_of::<i32>() * (array.len() + 1) + bit_mask_size(array)
        },
        DataType::Utf8 => {
            let string_array = array.as_string::<i32>();
            string_array.values().len() + std::mem::size_of::<i32>() * (array.len() + 1) + bit_mask_size(array)
        },
        DataType::List(_) => {
            let list_array = array.as_list::<i32>();
            get_byte_size(list_array.values().as_ref()) + std::mem::size_of::<i32>() * (array.len() + 1) + bit_mask_size(array)
        },
        DataType::Struct(_) => {
            let struct_array = array.as_struct();
            struct_array.columns().iter().map(|c| get_byte_size(c.as_ref())).sum::<usize>() + bit_mask_size(array)
        },
        t => panic!("unsupported array type - {}", t)
    }
}


fn bit_mask_size(array: &dyn Array) -> usize {
    if array.nulls().is_some() {
        array.len() / 8
    } else {
        0
    }
}


pub fn bisect_at_byte_size(array: &dyn Array, size: usize, tol: usize) -> usize {
    let total = get_byte_size(array);
    if total <= size + size * tol / 100 {
        return array.len()
    }
    match array.data_type() {
        DataType::Boolean |
        DataType::Int8 |
        DataType::Int16 |
        DataType::Int32 |
        DataType::Int64 |
        DataType::UInt8 |
        DataType::UInt16 |
        DataType::UInt32 |
        DataType::UInt64 |
        DataType::Timestamp(_, _) => {
            let len = size * array.len() / total;
            max(len, 1)
        },
        _ => {
            let mut max_len = array.len();
            let mut max_size = total;
            let mut min_len = 0;
            let mut min_size = 0;
            while max_len - min_len > 1 {
                let guess = min_len + max(1, (max_len - min_len) * (size - min_size) / (max_size - min_size));
                let actual_size = get_byte_size(&array.slice(0, guess));
                if util::is_within_tolerance(actual_size, size, tol) {
                    return guess
                }
                if actual_size < size {
                    min_len = guess;
                    min_size = actual_size;
                } else {
                    max_len = guess;
                    max_size = actual_size;
                }
            }
            max_len
        }
    }
}