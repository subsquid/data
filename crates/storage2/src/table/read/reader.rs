use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use crate::kv::{KvRead, KvReadCursor};
use crate::table::key::TableKeyFactory;
use crate::table::read::array::{read_array, Storage};
use crate::table::read::pagination::Pagination;
use crate::table::read::stats::Stats;
use crate::table::util;
use anyhow::{anyhow, ensure, Context};
use arrow::array::{ArrayRef, BooleanBufferBuilder, RecordBatch};
use arrow::buffer::{BooleanBuffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowNativeType, Schema, SchemaRef};
use arrow::util::bit_util;
use arrow_buffer::NullBuffer;
use parking_lot::Mutex;
use rayon::prelude::*;
use sqd_primitives::range::RangeList;


pub struct TableReader<S> {
    storage: S,
    key: TableKeyFactory,
    schema: SchemaRef,
    offsets: Mutex<HashMap<(usize, usize), ScalarBuffer<u32>>>,
    stats: Mutex<Vec<Option<Stats>>>
}


impl <S: KvRead + Sync> TableReader<S> {
    pub fn new(storage: S, table_name: &[u8]) -> anyhow::Result<Self> {
        let mut key = TableKeyFactory::new(table_name);

        let schema = {
            let bytes = storage.get(key.schema())?.ok_or_else(|| {
                anyhow!("schema key not found")
            })?;
            arrow::ipc::root_as_schema(&bytes).map(arrow::ipc::convert::fb_to_schema)
                .map_err(|_| anyhow!("failed to deserialize table schema"))?
        };

        let stats = Mutex::new(vec![None; schema.fields().len()]);

        Ok(Self {
            storage,
            key,
            schema: Arc::new(schema),
            offsets: Mutex::new(HashMap::new()),
            stats
        })
    }

    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    pub fn get_column_stats(&self, column_index: usize) -> anyhow::Result<Option<Stats>> {
        // Ok(None)
        let stats = self.stats.lock();
        Ok(if let Some(stats) = stats[column_index].as_ref() {
            Some(stats.clone())
        } else {
            let data = self.storage.get(
                self.key.clone().statistic(column_index)
            )?;
            if let Some(data) = data {
                let stats = Stats::read(&data, self.schema.field(column_index).data_type())
                    .with_context(|| anyhow!(
                        "failed to deserialize stats for column {}",
                        self.schema.field(column_index).name()
                    ))?;
                Some(stats)
            } else {
                None
            }
        })
    }

    pub fn read_table(
        &self,
        projection: Option<&HashSet<&str>>,
        row_ranges: Option<&RangeList<u32>>
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
            &ColumnStorage {
                reader: self,
                column: index
            },
            0,
            ranges,
            self.schema.field(index).data_type()
        )
    }

    fn get_buffer_pages(
        &self,
        column: usize,
        buffer: usize
    ) -> anyhow::Result<ScalarBuffer<u32>>
    {
        let mut bag = self.offsets.lock();
        if let Some(buf) = bag.get(&(column, buffer)) {
            Ok(buf.clone())
        } else {
            let page = self.storage.get(
                self.key.clone().offsets(column, buffer)
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

            util::validate_offsets(&offsets)?;

            bag.insert((column, buffer), offsets.clone());

            Ok(offsets)
        }
    }

    fn for_each_page<F: FnMut(usize, &[u8]) -> anyhow::Result<()>>(
        &self,
        column: usize,
        buffer: usize,
        pagination: &Pagination<'_>,
        mut cb: F
    ) -> anyhow::Result<()>
    {
        match pagination.num_pages() {
            0 => {},
            1 => {
                let page_idx = pagination.page_index(0);

                let value = self.storage.get(
                    self.key.clone().page(column, buffer, page_idx)
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
                    let page_key = key.page(column, buffer, page_idx);
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
        column: usize,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<(OffsetBuffer<i32>, Option<RangeList<u32>>)>
    {
        let offsets = self.read_native_bytes(column, buffer, i32::get_byte_width(), None)
            .map(ScalarBuffer::<i32>::from)?;

        util::validate_offsets(&offsets)?;

        Ok(if let Some(ranges) = ranges {
            // TODO: implement partial offsets reading
            let mut value_ranges = Vec::<Range<u32>>::with_capacity(ranges.len());
            let len: usize = ranges.iter().map(|r| r.len()).sum();
            let mut buf = MutableBuffer::from_len_zeroed((len + 1) * i32::get_byte_width());
            let data = buf.typed_data_mut::<i32>();
            let mut pos = 0;

            for r in ranges.iter() {
                let end_pos = pos + r.len();

                let offset = data[pos];
                data[pos..end_pos + 1].copy_from_slice(
                    &offsets[r.start as usize..r.end as usize + 1]
                );

                let val_range = data[pos] as u32..data[end_pos] as u32;

                let range_offset = data[pos];
                for o in data[pos..end_pos + 1].iter_mut() {
                    *o = *o - range_offset + offset;
                }

                if val_range.start < val_range.end {
                    if let Some(last_range) = value_ranges.last_mut() {
                        if (*last_range).end == val_range.start {
                            last_range.end = val_range.end
                        } else {
                            value_ranges.push(val_range)
                        }
                    } else {
                        value_ranges.push(val_range)
                    }
                }

                pos = end_pos;
            }

            unsafe {(
                OffsetBuffer::new_unchecked(ScalarBuffer::from(buf)),
                Some(RangeList::new(value_ranges))
            )}
        } else {
            let offsets = unsafe {
                OffsetBuffer::new_unchecked(offsets)
            };
            (offsets, None)
        })
    }

    fn read_native_bytes(
        &self,
        column: usize,
        buffer: usize,
        item_size: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<MutableBuffer>
    {
        let page_offsets = self.get_buffer_pages(column, buffer)?;
        let pagination = Pagination::new(&page_offsets, ranges);
        let mut buf = MutableBuffer::from_len_zeroed(pagination.num_items() * item_size);

        self.read_native_par(
            item_size,
            column,
            buffer,
            &pagination,
            0..pagination.num_pages(),
            buf.as_slice_mut()
        )?;

        Ok(buf)
    }

    fn read_native_par(
        &self,
        item_size: usize,
        column: usize,
        buffer: usize,
        pagination: &Pagination,
        pages: Range<usize>,
        dest: &mut [u8]
    ) -> anyhow::Result<()>
    {
        match pages.len() {
            0 => Ok(()),
            1 => {
                let page_seq = pages.start;
                let page_idx = pagination.page_index(page_seq);

                let data = self.storage.get(
                    self.key.clone().page(column, buffer, page_idx)
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
            },
            n => {
                let mid = (n / 2) + (n % 2);
                let lower = pages.start..pages.start + mid;
                let upper = lower.end..pages.end;

                let (lower_buf, upper_buf) = dest.split_at_mut(
                    item_size * (pagination.page_write_offset(lower.end) - pagination.page_write_offset(lower.start))
                );

                let (lower_res, upper_res) = rayon::join(
                    || self.read_native_par(item_size, column, buffer, pagination, lower, lower_buf),
                    || self.read_native_par(item_size, column, buffer, pagination, upper, upper_buf)
                );

                lower_res?;
                upper_res?;

                Ok(())
            }
        }
    }

    fn read_boolean(
        &self,
        column: usize,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<BooleanBuffer>
    {
        let page_offsets = self.get_buffer_pages(column, buffer)?;
        let pagination = Pagination::new(&page_offsets, ranges);
        let mut buf = BooleanBufferBuilder::new(pagination.num_items());
        self.for_each_page(column, buffer, &pagination, |i, data| {
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

    fn read_null_mask(
        &self,
        column: usize,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<Option<NullBuffer>>
    {
        let page_offsets = self.get_buffer_pages(column, buffer)?;
        if page_offsets.last().cloned().unwrap() == 0 {
            return Ok(None)
        }
        let values = self.read_boolean(column, buffer, ranges)?;
        Ok(Some(NullBuffer::new(values)))
    }
}


struct ColumnStorage<'a, S> {
    reader: &'a TableReader<S>,
    column: usize
}


impl <'a, S: KvRead + Sync> Storage for ColumnStorage<'a, S> {
    fn read_native_bytes(
        &self,
        buffer: usize,
        item_size: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<MutableBuffer>
    {
        self.reader.read_native_bytes(self.column, buffer, item_size, ranges)
    }

    fn read_boolean(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<BooleanBuffer>
    {
        self.reader.read_boolean(self.column, buffer, ranges)
    }

    fn read_null_mask(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<Option<NullBuffer>>
    {
        self.reader.read_null_mask(self.column, buffer, ranges)
    }

    fn read_offsets(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<(OffsetBuffer<i32>, Option<RangeList<u32>>)>
    {
        self.reader.read_offsets(self.column, buffer, ranges)
    }
}