use arrow_buffer::MutableBuffer;
use crate::writer::OffsetsWriter;
use crate::util::validate_offsets;


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
        validate_offsets(offsets).expect("invalid offsets slice");
        unsafe {
            self.append_valid_slice(offsets)
        }
    }

    pub unsafe fn append_valid_slice(&mut self, offsets: &[i32]) {
        let beg = offsets[0];

        self.buffer.extend(offsets[1..].iter().map(|o| {
            *o - beg + self.last_offset
        }));

        self.last_offset += offsets[offsets.len() - 1] - beg;
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