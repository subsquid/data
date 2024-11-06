use super::page::PageWriter;
use sqd_array::builder::bitmask::BitmaskBuilder;
use sqd_array::index::RangeList;
use sqd_array::writer::BitmaskWriter;


pub struct BitmaskPageWriter<P> {
    page_writer: P,
    builder: BitmaskBuilder,
    page_size: usize
}


impl<P: PageWriter> BitmaskPageWriter<P> {
    pub fn new(page_writer: P) -> Self {
        let page_size = 16 * 1024;
        Self {
            page_writer,
            builder: BitmaskBuilder::new(page_size * 2),
            page_size
        }
    }

    #[inline]
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.builder.len() / 8 > self.page_size * 3 / 2 {
            self.do_flush()
        } else {
            Ok(())
        }
    }
    
    #[inline(never)]
    fn do_flush(&mut self) -> anyhow::Result<()> {
        let mut byte_offset = 0;
        while self.builder.bytes_size() - byte_offset > self.page_size * 3 / 2 {
            let bytes = &self.builder.data()[byte_offset..byte_offset + self.page_size];
            self.page_writer.write_page(self.page_size * 8, bytes)?;
            byte_offset += self.page_size;
        }
        self.builder.shift(byte_offset * 8);
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<P> {
        let mut byte_offset = 0;
        let byte_end = self.builder.bytes_size();
        
        if byte_end > self.page_size {
            let split_byte = byte_end / 2;
            let bytes = &self.builder.data()[0..split_byte];
            self.page_writer.write_page(split_byte * 8, bytes)?;
            byte_offset = split_byte;
        }

        if byte_end > byte_offset {
            self.page_writer.write_page(
                self.builder.len() - byte_offset * 8,
                &self.builder.data()[byte_offset..byte_end]
            )?;
        }
        
        Ok(self.page_writer)
    }
}


impl<P: PageWriter> BitmaskWriter for BitmaskPageWriter<P> {
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        self.builder.append_slice(data, offset, len);
        self.flush()
    }

    fn write_slice_indexes(&mut self, data: &[u8], indexes: impl Iterator<Item=usize> + Clone) -> anyhow::Result<()> {
        self.builder.append_slice_indexes(data, indexes);
        self.flush()
    }

    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.builder.append_slice_ranges(data, ranges);
        self.flush()
    }

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        // FIXME: We have unbounded memory usage, that is possible to optimize.
        // It is also possible to optimize `.write_slice()` and `.write_slice_ranges()`
        // to avoid extra coping of large ranges at the cost of extra if statement.
        // But, likely nothing of the above will affect us much at the moment...
        self.builder.append_many(val, count);
        self.flush()
    }
}