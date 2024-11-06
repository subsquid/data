use crate::table::write::page::PageWriter;
use arrow_buffer::ToByteSlice;
use sqd_array::builder::offsets::OffsetsBuilder;
use sqd_array::index::RangeList;
use sqd_array::offsets::Offsets;
use sqd_array::writer::OffsetsWriter;


pub struct OffsetPageWriter<P> {
    page_writer: P,
    builder: OffsetsBuilder,
    page_len: usize
}


impl<P: PageWriter> OffsetPageWriter<P> {
    pub fn new(page_writer: P) -> Self {
        let page_len = 16 * 1024;
        Self {
            page_writer,
            builder: OffsetsBuilder::new(page_len * 2),
            page_len
        }
    }

    #[inline]
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.builder.len() > self.page_len * 3 / 2 {
            self.do_flush()
        } else {
            Ok(())
        }
    }

    #[inline(never)]
    fn do_flush(&mut self) -> anyhow::Result<()> {
        let mut offset = 0;
        while self.builder.len() - offset > self.page_len * 3 / 2 {
            let end = offset + self.page_len;
            self.page_writer.write_page(
                self.page_len,
                self.builder.as_slice().values()[offset..end].to_byte_slice()
            )?;
            offset = end;
        }
        self.builder.shift(offset);
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<P> {
        let slice = self.builder.as_slice();
        let data = slice.values();
        let mut offset = 0;

        if data.len() > self.page_len {
            let split = data.len() / 2;
            self.page_writer.write_page(
                split,
                data[0..split].to_byte_slice()
            )?;
            offset = split;
        }

        let data = &data[offset..];
        if !data.is_empty() {
            self.page_writer.write_page(data.len(), data.to_byte_slice())?;
        }

        Ok(self.page_writer)
    }
}


impl<P: PageWriter> OffsetsWriter for OffsetPageWriter<P> {
    fn write_slice(&mut self, offsets: Offsets<'_>) -> anyhow::Result<()> {
        self.builder.append_slice(offsets);
        self.flush()
    }

    fn write_slice_indexes(&mut self, offsets: Offsets<'_>, indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
        self.builder.append_slice_indexes(offsets, indexes);
        self.flush()
    }

    fn write_slice_ranges(&mut self, offsets: Offsets<'_>, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.builder.append_slice_ranges(offsets, ranges);
        self.flush()
    }

    fn write_len(&mut self, len: usize) -> anyhow::Result<()> {
        self.builder.append_len(len);
        self.flush()
    }
}