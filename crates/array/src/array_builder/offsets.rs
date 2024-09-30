use crate::writer::{OffsetsWriter, RangeList};
use arrow_buffer::MutableBuffer;


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

    pub fn append_slice(&mut self, offsets: &[i32]) {
        let beg = offsets[0];

        self.buffer.extend(offsets[1..].iter().map(|o| {
            *o - beg + self.last_offset
        }));

        self.last_offset += offsets[offsets.len() - 1] - beg;
    }
    
    pub fn append_slice_indexes(&mut self, offsets: &[i32], indexes: impl Iterator<Item=usize>) {
        self.buffer.reserve(indexes.size_hint().0 * size_of::<i32>());
        
        for i in indexes {
            let len = offsets[i + 1] - offsets[i];
            self.last_offset += len;
            self.buffer.push(self.last_offset)
        }
    }
    
    pub fn append_slice_ranges(&mut self, offsets: &[i32], ranges: &mut impl RangeList) {
        self.buffer.reserve(ranges.size() * size_of::<i32>());
        
        for r in ranges.iter() {
            self.append_slice(&offsets[r.start..r.end + 1])
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
}


impl OffsetsWriter for OffsetsBuilder {
    #[inline]
    fn write_slice(&mut self, offsets: &[i32]) -> anyhow::Result<()> {
        self.append_slice(offsets);
        Ok(())
    }

    #[inline]
    fn write_slice_indexes(&mut self, offsets: &[i32], indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        self.append_slice_indexes(offsets, indexes);
        Ok(())
    }

    #[inline]
    fn write_slice_ranges(&mut self, offsets: &[i32], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.append_slice_ranges(offsets, ranges);
        Ok(())
    }

    #[inline]
    fn write_len(&mut self, len: usize) -> anyhow::Result<()> {
        self.append_len(len);
        Ok(())
    }

    #[inline]
    fn write(&mut self, offset: i32) -> anyhow::Result<()> {
        self.append(offset);
        Ok(())
    }
}