use crate::index::RangeList;
use crate::offsets::Offsets;
use crate::writer::OffsetsWriter;
use arrow_buffer::{MutableBuffer, OffsetBuffer, ScalarBuffer};


pub struct OffsetsBuilder {
    buffer: MutableBuffer,
    last_offset: i32
}


impl OffsetsBuilder {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = MutableBuffer::new(capacity + 1);
        buffer.push(0i32);
        Self {
            buffer,
            last_offset: 0
        }
    }
    
    pub fn byte_size(&self) -> usize {
        self.buffer.len()
    }
    
    pub fn clear(&mut self) {
        self.buffer.truncate(size_of::<i32>());
        self.last_offset = 0
    }

    pub fn append_slice(&mut self, offsets: Offsets<'_>) {
        let beg = offsets.first_offset();

        self.buffer.extend(offsets.values()[1..].iter().map(|o| {
            *o - beg + self.last_offset
        }));

        self.last_offset += offsets.last_offset() - beg;
    }
    
    pub fn append_slice_indexes(&mut self, offsets: Offsets<'_>, indexes: impl Iterator<Item=usize>) {
        self.buffer.reserve(indexes.size_hint().0 * size_of::<i32>());
        
        for i in indexes {
            let len = offsets.values()[i + 1] - offsets.values()[i];
            self.last_offset += len;
            self.buffer.push(self.last_offset)
        }
    }
    
    pub fn append_slice_ranges(&mut self, offsets: Offsets<'_>, ranges: &mut impl RangeList) {
        self.buffer.reserve(ranges.span() * size_of::<i32>());
        
        for r in ranges.iter() {
            self.append_slice(offsets.slice(r.start, r.len()))
        }
    }

    #[inline]
    pub fn append_len(&mut self, len: usize) {
        self.last_offset += len as i32;
        self.buffer.push(self.last_offset)
    }

    #[inline]
    pub fn append(&mut self, offset: i32) {
        assert!(self.last_offset <= offset);
        self.last_offset = offset;
        self.buffer.push(offset)
    }

    pub fn finish(self) -> OffsetBuffer<i32> {
        let scalar = ScalarBuffer::from(self.buffer);
        unsafe {
            // SAFETY: monotonicity and non-emptiness are guaranteed by construction
            OffsetBuffer::new_unchecked(scalar)
        }
    }
    
    pub fn as_slice(&self) -> Offsets<'_> {
        unsafe {
            // SAFETY: monotonicity and non-emptiness are guaranteed by construction
            Offsets::new_unchecked(self.buffer.typed_data())
        }
    }
}


impl OffsetsWriter for OffsetsBuilder {
    #[inline]
    fn write_slice(&mut self, offsets: Offsets<'_>) -> anyhow::Result<()> {
        self.append_slice(offsets);
        Ok(())
    }

    #[inline]
    fn write_slice_indexes(&mut self, offsets: Offsets<'_>, indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        self.append_slice_indexes(offsets, indexes);
        Ok(())
    }

    #[inline]
    fn write_slice_ranges(&mut self, offsets: Offsets<'_>, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.append_slice_ranges(offsets, ranges);
        Ok(())
    }

    #[inline]
    fn write_len(&mut self, len: usize) -> anyhow::Result<()> {
        self.append_len(len);
        Ok(())
    }
}