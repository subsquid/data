use super::array::{read_array, Storage};
use super::pagination::Pagination;
use crate::kv::{KvRead, KvReadCursor};
use crate::table::key::TableKeyFactory;
use crate::table::read::cursor_byte_reader::CursorByteReader;
use crate::table::stats::Stats;
use anyhow::{anyhow, ensure, Context};
use arrow::array::{ArrayRef, BooleanBufferBuilder, RecordBatch};
use arrow::buffer::{BooleanBuffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowNativeType, SchemaRef};
use arrow::util::bit_util;
use arrow_buffer::NullBuffer;
use parking_lot::Mutex;
use rayon::prelude::*;
use sqd_array::io::reader::{BitmaskIOReader, IOReader, NativeIOReader, NullmaskIOReader, OffsetsIOReader};
use sqd_array::reader::{AnyReader, ArrayReader, Reader, ReaderFactory};
use sqd_array::util::{build_field_offsets, validate_offsets};
use sqd_primitives::range::RangeList;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;


pub struct TableReader<S> {
    storage: S,
    key: TableKeyFactory,
    schema: SchemaRef,
    column_positions: Vec<usize>,
    offsets: Mutex<HashMap<usize, OffsetBuffer<u32>>>,
}


impl<S: KvRead + Sync> TableReader<S> {
    pub fn new(storage: S, table_name: &[u8]) -> anyhow::Result<Self> {
        let mut key = TableKeyFactory::new(table_name);

        let schema = {
            let bytes = storage.get(key.schema())?.ok_or_else(|| {
                anyhow!("schema key not found")
            })?;
            arrow::ipc::root_as_schema(&bytes).map(arrow::ipc::convert::fb_to_schema)
                .map_err(|_| anyhow!("failed to deserialize table schema"))?
        };

        let column_positions = build_field_offsets(schema.fields(), 0);

        Ok(Self {
            storage,
            key,
            schema: Arc::new(schema),
            column_positions,
            offsets: Mutex::new(HashMap::new()),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn get_column_stats(&self, _column_index: usize) -> anyhow::Result<Option<Stats>> {
        Ok(None)
    }

    pub fn read_table(
        &self,
        projection: Option<&HashSet<&str>>,
        row_ranges: Option<&RangeList<u32>>,
    ) -> anyhow::Result<RecordBatch>
    {
        let column_indexes = if let Some(projection) = projection {
            let mut columns = Vec::with_capacity(projection.len());
            for (i, f) in self.schema.fields().iter().enumerate() {
                if projection.contains(f.name().as_str()) {
                    columns.push(i)
                }
            }
            columns
        } else {
            (0..self.schema.fields().len()).collect()
        };

        let columns = column_indexes.par_iter().map(|i| {
            self.read_column(*i, row_ranges)
        }).collect::<anyhow::Result<Vec<_>>>()?;

        let schema = if column_indexes.len() == self.schema.fields().len() {
            self.schema.clone()
        } else {
            self.schema.project(&column_indexes)?.into()
        };

        let record_batch = RecordBatch::try_new(schema, columns)?;
        Ok(record_batch)
    }

    pub fn read_column(&self, index: usize, ranges: Option<&RangeList<u32>>) -> anyhow::Result<ArrayRef> {
        read_array(
            self,
            self.column_positions[index],
            ranges,
            self.schema.field(index).data_type(),
        )
    }

    pub fn create_column_reader(&self, column_index: usize) -> anyhow::Result<impl ArrayReader> {
        let mut factory = CursorReaderFactory {
            table: self,
            buffer: self.column_positions[column_index]
        };
        AnyReader::from_factory(
            &mut factory,
            self.schema.field(column_index).data_type()
        )
    }

    fn get_buffer_pages(
        &self,
        buffer: usize,
    ) -> anyhow::Result<OffsetBuffer<u32>>
    {
        let mut bag = self.offsets.lock();
        if let Some(buf) = bag.get(&buffer) {
            return Ok(buf.clone());
        }

        let mut key = self.key.clone();
        let page = self.storage.get(
            key.offsets(buffer)
        )?.ok_or_else(|| {
            anyhow!("offsets page was not found")
        })?;

        let offsets = {
            let item_size = u32::get_byte_width();
            ensure!(
                page.len() % item_size == 0,
                "expected offsets page to be multiple of {}",
                item_size
            );
            let mut buf = MutableBuffer::new(page.len());
            buf.extend_from_slice(&page);
            ScalarBuffer::from(buf)
        };

        validate_offsets(&offsets, 0).map_err(|msg| anyhow!(msg))?;
        ensure!(offsets[0] == 0, "");

        let offsets = unsafe {
            OffsetBuffer::new_unchecked(offsets)
        };

        bag.insert(buffer, offsets.clone());

        Ok(offsets)
    }

    fn for_each_page<F: FnMut(usize, &[u8]) -> anyhow::Result<()>>(
        &self,
        buffer: usize,
        pagination: &Pagination<'_>,
        mut cb: F,
    ) -> anyhow::Result<()>
    {
        match pagination.num_pages() {
            0 => {},
            1 => {
                let page_idx = pagination.page_index(0);

                let mut key = self.key.clone();
                let value = self.storage.get(
                    key.page(buffer, page_idx)
                )?.ok_or_else(|| {
                    anyhow!("page {} was not found", page_idx)
                })?;

                cb(0, &value)?;
            },
            n => {
                let mut key = self.key.clone();
                let mut prev_page_idx = 0;
                let mut cursor = self.storage.new_cursor();
                for i in 0..n {
                    let page_idx = pagination.page_index(i);
                    let page_key = key.page(buffer, page_idx);
                    if prev_page_idx + 1 == page_idx {
                        cursor.next()?;
                    } else {
                        cursor.seek(page_key)?;
                    }
                    ensure!(
                        cursor.is_valid() && cursor.key() == page_key,
                        "page {} was not found at expected place",
                        page_idx
                    );
                    cb(i, cursor.value())?;
                    prev_page_idx = page_idx;
                }
            }
        };
        Ok(())
    }

    fn read_offsets(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>,
    ) -> anyhow::Result<(OffsetBuffer<i32>, Option<RangeList<u32>>)>
    {
        Ok(if let Some(ranges) = ranges {
            let (offsets, value_ranges) = self.read_ranged_offsets(buffer, ranges)?;
            (offsets, Some(value_ranges))
        } else {
            let offsets = self.read_all_offsets(buffer)?;
            (offsets, None)
        })
    }

    fn read_all_offsets(&self, buffer: usize) -> anyhow::Result<OffsetBuffer<i32>> {
        let offsets = self.read_native_bytes(buffer, i32::get_byte_width(), None)
            .map(ScalarBuffer::<i32>::from)?;

        validate_offsets(&offsets, 0).map_err(|msg| anyhow!(msg))?;

        Ok(unsafe {
            OffsetBuffer::new_unchecked(offsets)
        })
    }

    fn read_ranged_offsets(
        &self,
        buffer: usize,
        ranges: &RangeList<u32>,
    ) -> anyhow::Result<(OffsetBuffer<i32>, RangeList<u32>)>
    {
        if ranges.len() == 0 {
            return Ok((OffsetBuffer::new_empty(), RangeList::new(vec![])));
        }

        let offset_ranges = RangeList::seal(ranges.iter().map(|r| {
            r.start..r.end + 1
        }));

        let mut buf = self.read_native_bytes(
            buffer,
            i32::get_byte_width(),
            Some(&offset_ranges),
        )?;

        let offsets = buf.typed_data_mut::<i32>();
        validate_offsets(offsets, 0).map_err(|msg| anyhow!(msg))?;

        let mut value_ranges: Vec<Range<u32>> = Vec::with_capacity(ranges.len());
        {
            let mut pos = 0;
            for r in ranges.iter() {
                let beg = pos;
                let end = pos + r.len();
                let vr = offsets[beg] as u32..offsets[end] as u32;
                if vr.start < vr.end {
                    match value_ranges.last_mut() {
                        Some(prev) if prev.end == vr.start => {
                            prev.end = vr.end
                        }
                        _ => {
                            value_ranges.push(vr)
                        }
                    }
                }
                pos = end + 1;
            }
        }

        {
            let mut pos = 0;
            let mut last_offset = 0;
            for (shift, r) in ranges.iter().enumerate() {
                let beg = pos;
                let end = pos + r.len() + 1;
                let r_offset = offsets[beg];
                for o in offsets[beg..end].iter_mut() {
                    *o = *o - r_offset + last_offset
                }
                last_offset = offsets[end - 1];
                offsets.copy_within(beg..end, beg - shift);
                pos = end;
            }
        }

        buf.truncate(buf.len() - (ranges.len() - 1) * i32::get_byte_width());

        Ok(unsafe {
            (
                OffsetBuffer::new_unchecked(ScalarBuffer::from(buf)),
                RangeList::new_unchecked(value_ranges)
            )
        })
    }

    fn read_native_bytes(
        &self,
        buffer: usize,
        item_size: usize,
        ranges: Option<&RangeList<u32>>,
    ) -> anyhow::Result<MutableBuffer>
    {
        let page_offsets = self.get_buffer_pages(buffer)?;
        let pagination = Pagination::new(&page_offsets, ranges);
        let mut buf = MutableBuffer::from_len_zeroed(pagination.num_items() * item_size);

        self.read_native_par(
            item_size,
            buffer,
            &pagination,
            0..pagination.num_pages(),
            buf.as_slice_mut(),
        )?;

        Ok(buf)
    }

    fn read_native_par(
        &self,
        item_size: usize,
        buffer: usize,
        pagination: &Pagination,
        pages: Range<usize>,
        dest: &mut [u8],
    ) -> anyhow::Result<()>
    {
        match pages.len() {
            0 => Ok(()),
            1 => {
                let page_seq = pages.start;
                self.read_native_par_page(item_size, buffer, pagination, page_seq, dest)
            }
            n => {
                let mid = (n / 2) + (n % 2);
                let lower = pages.start..pages.start + mid;
                let upper = lower.end..pages.end;

                let (lower_buf, upper_buf) = dest.split_at_mut(
                    item_size * (pagination.page_write_offset(lower.end) - pagination.page_write_offset(lower.start))
                );

                let (lower_res, upper_res) = rayon::join(
                    || self.read_native_par(item_size, buffer, pagination, lower, lower_buf),
                    || self.read_native_par(item_size, buffer, pagination, upper, upper_buf),
                );

                lower_res?;
                upper_res?;

                Ok(())
            }
        }
    }

    #[inline(never)]
    fn read_native_par_page(
        &self,
        item_size: usize,
        buffer: usize,
        pagination: &Pagination,
        page_seq: usize,
        dest: &mut [u8],
    ) -> anyhow::Result<()>
    {
        let page_idx = pagination.page_index(page_seq);

        let mut key = self.key.clone();
        let data = self.storage.get(
            key.page(buffer, page_idx)
        )?.with_context(|| {
            anyhow!("page {} was not found", page_idx)
        })?;

        ensure!(
            data.len() % item_size == 0,
            "page {} byte size expected to be multiple of {}, but got {}",
            page_idx,
            item_size,
            data.len()
        );

        let page_len = pagination.page_range(page_seq).len();
        ensure!(
            data.len() / item_size == page_len,
            "expected page {} to contain {} items, but got {}",
            page_idx,
            page_len,
            data.len() / item_size
        );

        let mut write_offset = 0;
        for r in pagination.iter_ranges(page_seq) {
            let beg = r.start * item_size;
            let end = r.end * item_size;
            let len = end - beg;
            dest[write_offset..write_offset + len].copy_from_slice(&data[beg..end]);
            write_offset += len;
        }

        Ok(())
    }

    fn read_boolean(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>,
    ) -> anyhow::Result<BooleanBuffer>
    {
        let page_offsets = self.get_buffer_pages(buffer)?;
        self.read_bitmask(buffer, page_offsets, ranges)
    }

    fn read_null_mask(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>,
    ) -> anyhow::Result<Option<NullBuffer>>
    {
        let page_offsets = self.get_nullmask_pages(buffer)?;
        if page_offsets.len() == 2 {
            Ok(None)
        } else {
            let values = self.read_bitmask(
                buffer,
                page_offsets.slice(0, page_offsets.len() - 1),
                ranges
            )?;
            let nulls = NullBuffer::new(values);
            Ok(Some(nulls))
        }
    }

    fn get_nullmask_pages(&self, buffer: usize) -> anyhow::Result<OffsetBuffer<u32>> {
        let pages = self.get_buffer_pages(buffer)?;
        
        ensure!(pages.len() >= 2, "nullmask offsets should contain at least 2 pages");
        
        if pages.len() > 2 {
            let bit_len = pages[pages.len() - 1];
            ensure!(pages[pages.len() - 2] == bit_len, "bitmask and nullmask lengths are different");
        }

        Ok(pages)
    }

    fn read_bitmask(
        &self,
        buffer: usize,
        page_offsets: OffsetBuffer<u32>,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<BooleanBuffer>
    {
        let pagination = Pagination::new(&page_offsets, ranges);
        let mut buf = BooleanBufferBuilder::new(pagination.num_items());
        self.for_each_page(buffer, &pagination, |i, data| {
            let expected_bit_len = pagination.page_range(i).len();
            let expected_byte_len = bit_util::ceil(expected_bit_len, 8);
            ensure!(
                expected_byte_len == data.len(),
                "expected for page {} to have byte length {}, but got {}",
                pagination.page_index(i),
                expected_byte_len,
                data.len()
            );
            for r in pagination.iter_ranges(i) {
                buf.append_packed_range(r, data)
            }
            Ok(())
        })?;
        Ok(buf.finish())
    }
}


impl<S: KvRead + Sync> Storage for TableReader<S> {
    fn read_native_bytes(&self, buffer: usize, item_size: usize, ranges: Option<&RangeList<u32>>) -> anyhow::Result<MutableBuffer> {
        self.read_native_bytes(buffer, item_size, ranges)
    }

    fn read_boolean(&self, buffer: usize, ranges: Option<&RangeList<u32>>) -> anyhow::Result<BooleanBuffer> {
        self.read_boolean(buffer, ranges)
    }

    fn read_null_mask(&self, buffer: usize, ranges: Option<&RangeList<u32>>) -> anyhow::Result<Option<NullBuffer>> {
        self.read_null_mask(buffer, ranges)
    }

    fn read_offsets(&self, buffer: usize, ranges: Option<&RangeList<u32>>) -> anyhow::Result<(OffsetBuffer<i32>, Option<RangeList<u32>>)> {
        self.read_offsets(buffer, ranges)
    }
}


struct CursorReaderFactory<'a, S> {
    table: &'a TableReader<S>,
    buffer: usize
}


impl<'a, S: KvRead + Sync> CursorReaderFactory<'a, S> {
    fn next_bitmask(
        &mut self,
        pages: OffsetBuffer<u32>
    ) -> anyhow::Result<BitmaskIOReader<CursorByteReader<S::Cursor>>>
    {
        let bit_len = pages.last().copied().unwrap() as usize;

        let byte_offsets = pages.iter().enumerate().map(|(i, &o)| {
            Ok(if i == pages.len() - 1 {
                bit_util::ceil(o as usize, 8) as u32
            } else {
                ensure!(
                    o % 8 == 0,
                    "unaligned intermediate bitmask page: buffer {}, page {}",
                    self.buffer,
                    i
                );
                o / 8
            })
        }).collect::<anyhow::Result<Vec<_>>>()?;

        let byte_reader = CursorByteReader::new(
            self.table.storage.new_cursor(),
            self.table.key.clone(),
            self.buffer,
            byte_offsets
        );

        self.buffer += 1;
        Ok(BitmaskIOReader::new_unchecked(byte_reader, bit_len))
    }

    fn next_native_cursor(&mut self, item_size: usize) -> anyhow::Result<CursorByteReader<S::Cursor>> {
        let pages = self.table.get_buffer_pages(self.buffer)?;

        let byte_offsets: Vec<u32> = pages.iter()
            .map(|o| *o * item_size as u32)
            .collect();

        let byte_reader = CursorByteReader::new(
            self.table.storage.new_cursor(),
            self.table.key.clone(),
            self.buffer,
            byte_offsets
        );

        self.buffer += 1;
        Ok(byte_reader)
    }
}


impl<'a, S: KvRead + Sync> ReaderFactory for CursorReaderFactory<'a, S> {
    type Reader = IOReader<CursorByteReader<S::Cursor>>;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Nullmask> {
        let pages = self.table.get_nullmask_pages(self.buffer)?;
        let bit_len = pages.last().copied().unwrap();
        Ok(if pages.len() == 2 {
            NullmaskIOReader::new_empty(bit_len as usize)
        } else {
            let bitmask = self.next_bitmask(pages.slice(0, pages.len() - 1))?;
            NullmaskIOReader::from_bitmask(bitmask)
        })
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Bitmask> {
        let pages = self.table.get_buffer_pages(self.buffer)?;
        self.next_bitmask(pages)
    }

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Native> {
        let byte_reader = self.next_native_cursor(T::get_byte_width())?;
        NativeIOReader::new(byte_reader, T::get_byte_width())
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Offset> {
        let byte_reader = self.next_native_cursor(i32::get_byte_width())?;
        OffsetsIOReader::new(byte_reader)
    }
}