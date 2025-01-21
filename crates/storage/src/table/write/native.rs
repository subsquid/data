use std::sync::LazyLock;
use crate::table::write::page::PageWriter;
use anyhow::ensure;
use arrow_buffer::{bit_util, ArrowNativeType, MutableBuffer, ToByteSlice};
use parking_lot::RwLock;
use sqd_array::index::RangeList;
use sqd_array::writer::NativeWriter;


pub static PAGE_SIZE: LazyLock<RwLock<usize>> = LazyLock::new(|| {
    RwLock::new(64 * 1024)
});


pub struct NativePageWriter<P> {
    page_writer: P,
    buffer: MutableBuffer,
    item_size: usize,
    page_size: usize
}


impl<P: PageWriter> NativePageWriter<P> {
    pub fn new(page_writer: P, item_size: usize) -> Self {
        assert!(item_size > 0);
        let page_size = std::cmp::max(PAGE_SIZE.read().clone(), item_size);
        Self {
            page_writer,
            buffer: MutableBuffer::new(page_size * 2),
            item_size,
            page_size
        }
    }

    #[inline]
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.len() > self.page_size * 3 / 2 {
            self.do_flush()
        } else {
            Ok(())
        }
    }

    #[inline(never)]
    fn do_flush(&mut self) -> anyhow::Result<()> {
        let mut byte_offset = 0;
        while self.buffer.len() - byte_offset > self.page_size * 3 / 2 
            && self.buffer.len() >= self.item_size 
        {
            let len = bit_util::ceil(self.page_size, self.item_size);
            let byte_end = byte_offset + len * self.item_size;
            let bytes = &self.buffer[byte_offset..byte_end];
            self.page_writer.write_page(len, bytes)?;
            byte_offset = byte_end;
        }
        let bytes_left = self.buffer.len() - byte_offset;
        if bytes_left > 0 {
            self.buffer.as_slice_mut().copy_within(byte_offset.., 0);
        }
        self.buffer.truncate(bytes_left);
        Ok(())
    }
    
    pub fn finish(mut self) -> anyhow::Result<P> {
        ensure!(
            self.buffer.len() % self.item_size == 0,
            "got partially written item"
        );
        
        let mut byte_offset = 0;
        let byte_end = self.buffer.len();
        
        if byte_end > self.page_size {
            let len = bit_util::ceil(self.page_size, self.item_size);
            let split_byte = len * self.item_size;
            let bytes = &self.buffer[0..split_byte];
            self.page_writer.write_page(len, bytes)?;
            byte_offset = split_byte;
        }
        
        if byte_end > byte_offset {
            self.page_writer.write_page(
                (byte_end - byte_offset) / self.item_size,
                &self.buffer[byte_offset..]
            )?;
        }
        
        Ok(self.page_writer)
    }
}


impl<P: PageWriter> NativeWriter for NativePageWriter<P> {
    #[inline]
    fn write<T: ToByteSlice>(&mut self, value: T) -> anyhow::Result<()> {
        self.buffer.push(value);
        self.flush()
    }

    #[inline]
    fn write_iter<T: ArrowNativeType>(&mut self, values: impl Iterator<Item=T>) -> anyhow::Result<()> {
        for v in values {
            self.write(v)?;
        }
        Ok(())
    }

    #[inline]
    fn write_slice<T: ArrowNativeType>(&mut self, values: &[T]) -> anyhow::Result<()> {
        self.buffer.extend_from_slice(values);
        self.flush()
    }

    #[inline]
    fn write_slice_indexes<T: ArrowNativeType>(&mut self, values: &[T], indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        self.buffer.write_slice_indexes(values, indexes)?;
        self.flush()
    }

    #[inline]
    fn write_slice_ranges<T: ArrowNativeType>(&mut self, values: &[T], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.buffer.write_slice_ranges(values, ranges)?;
        self.flush()
    }
}