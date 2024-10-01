
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
