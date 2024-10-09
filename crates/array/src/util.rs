
#[inline]
pub fn validate_offsets<I: Ord + Default + Copy>(offsets: &[I]) -> Result<(), &'static str> {
    if offsets.len() == 0 {
        return Err("offsets slice can't be empty")
    }

    let mut prev = offsets[0];
    if prev < I::default() {
        return Err("found negative offset value")
    }

    for &val in offsets[1..].iter() {
        if val < prev {
            return Err("offset values are not monotonically increasing")
        }
        prev = val
    }

    Ok(())
}


macro_rules! invalid_buffer_access {
    () => {
        panic!("invalid arrow buffer access")
    };
}

use arrow::datatypes::DataType;
pub(crate) use invalid_buffer_access;


pub(crate) mod bit_tools {
    use std::ops::Range;
    use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
    use arrow_buffer::bit_util;
    
    pub fn all_valid(data: &[u8], offset: usize, len: usize) -> bool {
        // TODO: optimize
        UnalignedBitChunk::new(data, offset, len).count_ones() == len
    }
    
    pub fn all_indexes_valid(data: &[u8], indexes: impl Iterator<Item = usize>) -> Option<usize> {
        let mut len = 0;
        for i in indexes {
            if !bit_util::get_bit(data, i) {
                return None;
            }
            len += 1
        }
        Some(len)
    }
    
    pub fn all_ranges_valid(data: &[u8], ranges: impl Iterator<Item = Range<usize>>) -> Option<usize> {
        let mut len = 0;
        for r in ranges {
            if !all_valid(data, r.start, r.len()) {
                return None;
            }
            len += r.len()
        }
        Some(len)
    }
}


pub(crate) fn get_num_buffers(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean |
        DataType::Int8 |
        DataType::Int16 |
        DataType::Int32 |
        DataType::Int64 |
        DataType::UInt8 |
        DataType::UInt16 |
        DataType::UInt32 |
        DataType::UInt64 |
        DataType::Float16 |
        DataType::Float32 |
        DataType::Float64 |
        DataType::Timestamp(_, _) |
        DataType::Date32 |
        DataType::Date64 |
        DataType::Time32(_) |
        DataType::Time64(_) |
        DataType::Duration(_) |
        DataType::Interval(_) => {
            2
        }
        DataType::Binary |
        DataType::Utf8 => {
            3
        }
        DataType::List(f) => {
            2 + get_num_buffers(f.data_type())
        }
        DataType::Struct(fields) => {
            1 + fields.iter().map(|f| get_num_buffers(f.data_type())).sum::<usize>()
        }
        ty => panic!("unsupported arrow data type - {}", ty)
    }
}