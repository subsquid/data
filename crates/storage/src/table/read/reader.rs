use std::cmp::{max, min};
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, ensure};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch, UInt32Array};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{ArrowNativeType, DataType, Schema, SchemaRef, UInt32Type};
use parking_lot::Mutex;

use crate::array::serde::{deserialize_array, deserialize_primitive_array};
use crate::kv::{KvRead, KvReadCursor};
use crate::range::RangeList;
use crate::table::read::projection::ProjectionMask;
use crate::table::key::{Statistic, TableKeyFactory};


type StatsBag = HashMap<Statistic, Option<ArrayRef>>;


pub struct TableReader<S> {
    storage: S,
    key: TableKeyFactory,
    schema: SchemaRef,
    row_group_offsets: UInt32Array,
    stats0: Mutex<Vec<StatsBag>>,
    stats1: Mutex<HashMap<usize, Arc<Mutex<Vec<StatsBag>>>>>
}


impl <S: KvRead> TableReader<S> {
    pub fn new(storage: S, table_name: &[u8]) -> anyhow::Result<Self> {
        let mut key = TableKeyFactory::new(table_name);

        let schema = {
            let bytes = storage.get(key.schema())?.ok_or_else(|| {
                anyhow!("schema key not found")
            })?;
            arrow::ipc::root_as_schema(&bytes).map(arrow::ipc::convert::fb_to_schema)
                .map_err(|_| anyhow!("failed deserialize table schema"))?
        };

        let row_group_offsets = {
            let bytes = storage.get(key.row_group_offsets())?.ok_or_else(|| {
                anyhow!("row group offsets key not found")
            })?;

            deserialize_primitive_array::<UInt32Type>(&bytes)
                .and_then(|array| {
                    ensure!(array.null_count() == 0, "offsets array can't have null values");
                    validate_offsets(array.values().as_ref())?;
                    Ok(array)
                })
                .context("failed to deserialize row group offsets array")?
        };

        let num_columns = schema.fields().len();

        Ok(Self {
            storage,
            key,
            schema: Arc::new(schema),
            row_group_offsets,
            stats0: Mutex::new(
                std::iter::repeat_with(HashMap::new).take(num_columns).collect()
            ),
            stats1: Mutex::new(HashMap::new())
        })
    }

    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    pub fn create_projection<'a, 'b, I: IntoIterator<Item=&'b str>>(&'a self, columns: I) -> ProjectionMask {
        let mut mask = ProjectionMask::new(self.num_columns());
        for name in columns {
            let col_idx = self.schema.index_of(name).unwrap();
            mask.include_column(col_idx)
        }
        mask
    }

    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn num_rows(&self) -> usize {
        self.row_group_offsets.values().last().unwrap().as_usize()
    }

    pub fn num_row_groups(&self) -> usize {
        self.row_group_offsets.len() - 1
    }

    pub fn get_page_offsets(&self, row_group: usize, column: usize) -> anyhow::Result<UInt32Array> {
        Ok(self.get_statistic1(Statistic::Offsets, row_group, column)?
            .unwrap()
            .as_primitive()
            .clone()
        )
    }

    pub fn read_row_group_column(
        &self,
        row_group: usize,
        column: usize,
        row_selection: Option<&RangeList>
    ) -> anyhow::Result<ArrayRef>
    {
        let page_offsets = self.get_page_offsets(row_group, column)?;
        let page_offsets = page_offsets.values().as_ref();

        let pages: Vec<_> = row_selection.map(|ranges| {
            select_pages(page_offsets, ranges).collect()
        }).unwrap_or_else(|| {
            (0..page_offsets.len() - 1).map(|i| (i, None)).collect()
        });

        let mut cursor = self.storage.new_cursor();
        let mut key = self.key.clone();
        let mut prev_page = 0;
        let data_type = self.schema.field(column).data_type();

        let arrays = pages.into_iter().map(|(page_index, maybe_mask)| {
            let page_key = key.page(row_group, column, page_index);
            if prev_page + 1 == page_index {
                cursor.next()?;
            } else {
                cursor.seek(page_key)?;
            }
            ensure!(
                cursor.is_valid() && cursor.key() == page_key,
                "page was not found at expected place"
            );

            let mut array = deserialize_array(cursor.value(), data_type.clone())?;

            let expected_array_len = page_offsets[page_index + 1] - page_offsets[page_index];

            ensure!(
                array.len() == expected_array_len as usize,
                "unexpected array length"
            );

            if let Some(mask) = maybe_mask {
                array = arrow::compute::filter(
                    array.as_ref(),
                    &BooleanArray::new(mask, None)
                )?;
            }

            prev_page = page_index;
            Ok::<ArrayRef, anyhow::Error>(array)
        }.with_context(|| {
            format!("failed to read page {}", page_index)
        })).collect::<anyhow::Result<Vec<_>>>()?;

        let array_refs: Vec<_> = arrays.iter().map(|a| a.as_ref()).collect();

        let result = arrow::compute::concat(array_refs.as_slice())?;

        Ok(result)
    }

    pub fn read_row_group(
        &self,
        row_group: usize,
        row_selection: Option<&RangeList>,
        projection: Option<&ProjectionMask>
    ) -> anyhow::Result<RecordBatch>
    {
        let projection_len = if let Some(mask) = projection {
            ensure!(
                mask.len() == self.num_columns(),
                "projection mask does not match the number of columns"
            );
            mask.selection_len()
        } else {
            self.num_columns()
        };

        ensure!(projection_len > 0, "no columns where selected");

        let mut columns = Vec::with_capacity(projection_len);

        self.for_each_projected_column(projection, |col_idx| {
            let array = self.read_row_group_column(row_group, col_idx, row_selection)
                .with_context(|| {
                    format!("failed to read column {}", col_idx)
                })?;
            columns.push(array);
            Ok(())
        })?;

        let schema = self.projected_schema(projection);
        let record_batch = RecordBatch::try_new(schema, columns)?;
        Ok(record_batch)
    }

    fn for_each_projected_column<F>(
        &self,
        projection: Option<&ProjectionMask>,
        mut f: F
    ) -> anyhow::Result<()> where F: FnMut(usize) -> anyhow::Result<()>
    {
        if let Some(mask) = projection {
            for col_idx in mask.iter() {
                f(col_idx)?
            }
        } else {
            for col_idx in 0..self.num_columns() {
                f(col_idx)?
            }
        }
        Ok(())
    }

    fn projected_schema(&self, projection: Option<&ProjectionMask>) -> SchemaRef {
        if let Some(mask) = projection {
            if mask.selection_len() < self.num_columns() {
                let fields: Vec<_> = mask.iter().map(|col_idx| {
                    self.schema.fields()[col_idx].clone()
                }).collect();
                let schema = Schema::new(fields);
                Arc::new(schema)
            } else {
                self.schema.clone()
            }
        } else {
            self.schema.clone()
        }
    }

    fn get_statistic0(&self, kind: Statistic, column: usize) -> anyhow::Result<Option<ArrayRef>> {
        let bag = &mut self.stats0.lock()[column];
        Self::handle_statistic(
            bag,
            kind,
            |kind, bag| {
                let result = self.read_statistic0(kind, column)?;
                if let Some(array) = result.as_ref() {
                    validate_statistic(self.row_group_offsets.values().as_ref(), kind, array.as_ref())?;
                }
                Ok(result)
            }
        )
    }

    fn get_statistic1(&self, kind: Statistic, row_group: usize, column: usize) -> anyhow::Result<Option<ArrayRef>> {
        let row_group_stats = {
            self.stats1.lock().entry(row_group).or_insert_with(|| {
                Arc::new(Mutex::new(
                    std::iter::repeat_with(HashMap::new).take(self.schema.fields().len()).collect()
                ))
            }).clone()
        };

        let bag = &mut row_group_stats.lock()[column];

        self.handle_statistic1(bag, kind, row_group, column)
    }

    fn handle_statistic1(
        &self,
        bag: &mut StatsBag,
        kind: Statistic,
        row_group: usize,
        column: usize
    ) -> anyhow::Result<Option<ArrayRef>>
    {
        Self::handle_statistic(
            bag,
            kind,
            |kind, bag| {
                let result = self.read_statistic1(kind, row_group, column)?;
                if kind == Statistic::Offsets {
                    let array = result.as_ref().ok_or_else(|| {
                        anyhow!("offsets statistic must be always present")
                    })?;
                    ensure!(array.null_count() == 0, "offsets array can't have null values");
                    let offsets = array.as_primitive::<UInt32Type>().values().as_ref();
                    validate_offsets(offsets)?;
                    let expected_num_rows = self.row_group_offsets.value(row_group + 1) - self.row_group_offsets.value(row_group);
                    ensure!(
                        offsets.last().copied().unwrap() == expected_num_rows,
                        "unexpected offset sizes"
                    );
                } else if let Some(array) = result.as_ref() {
                    let offsets_array = self.handle_statistic1(bag, kind, row_group, column)?.unwrap();
                    let offsets = offsets_array.as_primitive::<UInt32Type>().values().as_ref();
                    validate_statistic(offsets, kind, array.as_ref())?;
                }
                Ok(result)
            }
        )
    }

    fn handle_statistic<R>(
        bag: &mut StatsBag,
        kind: Statistic,
        mut read: R
    ) -> anyhow::Result<Option<ArrayRef>>
        where
            R: FnMut(Statistic, &mut StatsBag) -> anyhow::Result<Option<ArrayRef>>
    {
        if let Some(result) = bag.get(&kind) {
            return Ok(result.clone())
        }
        let result = read(kind, bag)?;
        bag.insert(kind, result.clone());
        Ok(result)
    }

    fn read_statistic0(&self, kind: Statistic, column: usize) -> anyhow::Result<Option<ArrayRef>> {
        let mut key = self.key.clone();
        if let Some(bytes) = self.storage.get(key.statistic0(kind, column))? {
            let array = deserialize_array(&bytes, self.get_statistic_data_type(kind, column))?;
            Ok(Some(array))
        } else {
            Ok(None)
        }
    }

    fn read_statistic1(&self, kind: Statistic, row_group: usize, column: usize) -> anyhow::Result<Option<ArrayRef>> {
        let mut key = self.key.clone();
        if let Some(bytes) = self.storage.get(key.statistic1(kind, row_group, column))? {
            let array = deserialize_array(&bytes, self.get_statistic_data_type(kind, column))?;
            Ok(Some(array))
        } else {
            Ok(None)
        }
    }

    fn get_statistic_data_type(&self, statistic: Statistic, column: usize) -> DataType {
        match statistic {
            Statistic::Offsets => DataType::UInt32,
            Statistic::NullCount => DataType::UInt32,
            Statistic::Min | Statistic::Max => self.schema.field(column).data_type().clone(),
        }
    }
}


fn validate_offsets(offsets: &[u32]) -> anyhow::Result<()> {
    ensure!(offsets.len() > 0, "offsets array can't be empty");
    for i in 1..offsets.len() {
        ensure!(offsets[i] >= offsets[i-1], "offset values are not monotonically increasing")
    }
    Ok(())
}


fn validate_statistic(offsets: &[u32], kind: Statistic, array: &dyn Array) -> anyhow::Result<()> {
    assert_ne!(kind, Statistic::Offsets);
    ensure!(array.len() == offsets.len() - 1, "invalid {:?} array length", kind);
    if kind == Statistic::NullCount {
        ensure!(array.null_count() == 0);
        let null_count = array.as_primitive::<UInt32Type>();
        let bounds_ok = null_count.values().iter().enumerate().all(|(idx, count)| {
            *count <= offsets[idx + 1] - offsets[idx]
        });
        ensure!(bounds_ok, "some null counts are not within expected bounds");
    }
    Ok(())
}


fn select_pages<'a>(
    offsets: &'a [u32],
    row_selection: &'a RangeList
) -> impl Iterator<Item=(usize, Option<BooleanBuffer>)> + 'a
{
    let mut mask = BooleanBufferBuilder::new(0);
    let mut ranges = row_selection.iter().cloned().peekable();

    let mut pages = (0..offsets.len() - 1).map(|i| {
        offsets[i] as usize..offsets[i+1] as usize
    }).enumerate();

    std::iter::from_fn(move || {
        while let Some((idx, page)) = pages.next() {
            let mut beg = page.start;
            let end = page.end;
            while beg < end {
                if let Some(r) = ranges.peek_mut() {
                    if r.end <= beg || r.start >= r.end {
                        ranges.next();
                        continue
                    }
                    if end <= r.start {
                        break
                    }
                    let take_start = max(r.start, beg);
                    let take_end = min(end, r.end);
                    skip(&mut mask, beg, take_start - beg);
                    beg = take_end;
                    r.start = take_end;
                } else {
                    break
                }
            }
            if end - beg < page.len() {
                skip(&mut mask, beg, end);
                return if mask.len() > 0 {
                    mask.append_n(page.len() - mask.len(), true);
                    Some((idx, Some(mask.finish())))
                } else {
                    mask.resize(0);
                    Some((idx, None))
                }
            }
        }
        None
    })
}


fn skip(mask: &mut BooleanBufferBuilder, skip_start: usize, skip_end: usize) {
    if skip_start < skip_end {
        mask.append_n(skip_start - mask.len(), true);
        mask.append_n(skip_end - skip_start, false);
    }
}