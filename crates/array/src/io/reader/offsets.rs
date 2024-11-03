use crate::io::reader::byte_reader::ByteReader;
use crate::offsets::Offsets;
use crate::reader::OffsetsReader;
use crate::writer::OffsetsWriter;
use anyhow::{anyhow, ensure};
use arrow_buffer::MutableBuffer;
use std::ops::Range;


const OS: usize = size_of::<i32>();


pub struct OffsetsIOReader<R> {
    byte_reader: R,
    len: usize,
    buf: MutableBuffer
}


impl<R: ByteReader> OffsetsIOReader<R> {
    pub fn new(byte_reader: R) -> anyhow::Result<Self> {
        let len = byte_reader.len();
        ensure!(len > 0, "offsets buffer can't be empty");
        ensure!(
            len % OS == 0,
            "invalid offsets buffer length: {} is not a multiple of {}",
            len,
            OS
        );
        Ok(Self {
            byte_reader,
            len: len / OS - 1,
            buf: MutableBuffer::new(256)
        })
    }

    fn buffered_offsets(&self) -> &[i32] {
        let len = self.buf.len() / OS;
        unsafe {
            std::slice::from_raw_parts(self.buf.as_ptr().cast(), len)
        }
    }
    
    fn read(&mut self, byte_offset: &mut usize, byte_len: &mut usize) -> anyhow::Result<()> {
        let buf = self.byte_reader.read(*byte_offset, *byte_len)?;
        self.buf.extend_from_slice(buf);
        *byte_offset += buf.len();
        *byte_len -= buf.len();
        Ok(())
    }
    
    fn shift(&mut self) {
        let len = self.buf.len() / OS;
        if len > 1 {
            let shift = (len - 1) * OS;
            let new_byte_len = self.buf.len() - shift;
            self.buf.as_slice_mut().copy_within(shift.., 0);
            self.buf.truncate(new_byte_len)
        }
    }
}


impl <R: ByteReader> OffsetsReader for OffsetsIOReader<R> {
    fn len(&self) -> usize {
        self.len
    }
    
    fn read_slice(
        &mut self, 
        dst: &mut impl OffsetsWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<Range<usize>> 
    {
        ensure!(offset + len <= self.len);
        self.buf.clear();
        
        let mut byte_offset = offset * OS;
        let mut byte_len = (len + 1) * OS;
        
        loop {
            self.read(&mut byte_offset, &mut byte_len)?;
            if self.buf.len() >= OS {
                break
            }
        }
        
        let first_offset = self.buffered_offsets()[0] as usize;

        while byte_len > 0 {
            let data = self.buffered_offsets();
            if data.len() > 1 {
                let offsets = Offsets::try_new(data).map_err(|msg| anyhow!(msg))?;
                dst.write_slice(offsets)?;
                self.shift()
            }
            self.read(&mut byte_offset, &mut byte_len)?;
        }

        let offsets = Offsets::try_new(self.buffered_offsets()).map_err(|msg| anyhow!(msg))?;
        dst.write_slice(offsets)?;
        
        Ok(first_offset..offsets.last_index())
    }
}