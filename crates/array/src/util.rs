use arrow::datatypes::{DataType, Fields};
use std::cmp::Ordering;
use std::ops::AddAssign;


#[inline]
pub fn validate_offsets<I: Ord + Default + Copy>(
    offsets: &[I], 
    mut prev: I
) -> Result<(), &'static str> 
{
    if offsets.len() == 0 {
        return Err("offsets slice can't be empty")
    }
    
    if offsets[0] < I::default() {
        return Err("found negative offset value")
    }

    for &val in offsets.iter() {
        if val < prev {
            return Err(
                "offset values are not monotonically increasing"
            )
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
pub(crate) use invalid_buffer_access;


pub mod bit_tools {
    use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
    use arrow_buffer::bit_util;
    use std::ops::Range;


    pub fn all_valid(data: &[u8], offset: usize, len: usize) -> bool {
        // TODO: optimize
        UnalignedBitChunk::new(data, offset, len).count_ones() == len
    }
    
    pub fn all_indexes_valid(data: &[u8], indexes: impl Iterator<Item=usize>) -> Option<usize> {
        let mut len = 0;
        for i in indexes {
            if !bit_util::get_bit(data, i) {
                return None;
            }
            len += 1
        }
        Some(len)
    }
    
    pub fn all_ranges_valid(data: &[u8], ranges: impl Iterator<Item=Range<usize>>) -> Option<usize> {
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


pub fn bisect_offsets<I: Ord + Copy>(offsets: &[I], idx: I) -> Option<usize> {
    let mut beg = 0;
    let mut end = offsets.len() - 1;
    while end - beg > 1 { 
        let mid = beg + (end - beg) / 2;
        match offsets[mid].cmp(&idx) {
            Ordering::Equal => return Some(mid),
            Ordering::Less => {
                beg = mid
            },
            Ordering::Greater => {
                end = mid
            }
        }
    }
    if offsets[beg] <= idx && idx < offsets[end] {
        Some(beg)
    } else {
        None
    }
}


pub fn get_offset_position<I: Ord + Copy>(offsets: &[I], index: I, first_to_try: usize) -> usize {
    let beg = offsets[first_to_try];
    if beg <= index {
        if index < offsets[first_to_try + 1] {
            first_to_try
        } else {
            first_to_try + bisect_offsets(&offsets[first_to_try..], index)
                .expect("index is out of bounds")
        }
    } else {
        bisect_offsets(&offsets[0..first_to_try + 1], index)
            .expect("index is out of bounds")
    }
}


pub fn build_offsets<I: Copy + AddAssign>(first: I, lengths: impl Iterator<Item=I>) -> Vec<I> {
    let mut vec = Vec::with_capacity(1 + lengths.size_hint().0);
    let mut last = first;
    vec.push(last);
    vec.extend(lengths.map(|len| {
        last += len;
        last
    }));
    vec
}


pub fn build_field_offsets(start_pos: usize, fields: &Fields) -> Vec<usize> {
    build_offsets(start_pos, fields.iter().map(|f| {
        get_num_buffers(f.data_type())
    }))
}


pub fn get_num_buffers(data_type: &DataType) -> usize {
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
        DataType::FixedSizeBinary(_) => {
            2
        }
        DataType::List(f) => {
            2 + get_num_buffers(f.data_type())
        }
        DataType::FixedSizeList(f, _) => {
            1 + get_num_buffers(f.data_type())
        }
        DataType::Struct(fields) => {
            1 + fields.iter().map(|f| get_num_buffers(f.data_type())).sum::<usize>()
        }
        ty => panic!("unsupported arrow data type - {}", ty)
    }
}
