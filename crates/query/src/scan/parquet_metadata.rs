use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::page_index::index::Index;
use parquet::file::statistics::Statistics;

use crate::scan::array_predicate::ArrayStats;
use crate::scan::row_predicate::{ColumnStats, RowStats};
use crate::primitives::Name;


pub struct ParquetMetadata {
    metadata: ArrowReaderMetadata,
    row_group_stats: RowGroupStats,
    page_stats: Vec<PageStats>
}


impl ParquetMetadata {
    pub fn new(metadata: ArrowReaderMetadata) -> Self {
        let num_row_groups = metadata.metadata().num_row_groups();
        Self {
            metadata: metadata.clone(),
            row_group_stats: RowGroupStats::new(metadata.clone()),
            page_stats: (0..num_row_groups)
                .map(|i| PageStats::new(metadata.clone(), i))
                .collect()
        }
    }

    pub fn metadata(&self) -> &ArrowReaderMetadata {
        &self.metadata
    }

    pub fn row_group_stats(&self) -> &dyn RowStats {
        &self.row_group_stats
    }

    pub fn page_stats(&self, row_group: usize) -> &dyn RowStats {
        &self.page_stats[row_group]
    }
}


struct RowGroupStats {
    metadata: ArrowReaderMetadata,
    column_stats: parking_lot::Mutex<HashMap<Name, Option<Arc<dyn ColumnStats>>>>
}


impl RowGroupStats {
    pub fn new(metadata: ArrowReaderMetadata) -> Self {
        let num_columns = metadata.parquet_schema().num_columns();
        Self {
            metadata,
            column_stats: parking_lot::Mutex::new(HashMap::with_capacity(num_columns))
        }
    }
}


impl RowStats for RowGroupStats {
    fn get_column_stats(&self, column_name: Name) -> Option<Arc<dyn ColumnStats>> {
        let mut column_stats = self.column_stats.lock();
        column_stats.entry(column_name).or_insert_with(|| {
            self.build_column_stats(column_name).map(|s| Arc::new(s) as Arc<dyn ColumnStats>)
        }).clone()
    }

    fn get_num_rows(&self) -> usize {
        self.metadata.metadata().file_metadata().num_rows() as usize
    }
}


impl RowGroupStats {
    fn build_column_stats(&self, column_name: Name) -> Option<ColStats> {
        let col_idx = find_primitive_column(&self.metadata, column_name)?;

        let num_row_groups = self.metadata.metadata().num_row_groups();
        let mut ranges = Vec::with_capacity(num_row_groups);
        let mut boolean: Option<(BooleanBuilder, BooleanBuilder)> = None;
        let mut int32: Option<(Int32Builder, Int32Builder)> = None;
        let mut int64: Option<(Int64Builder, Int64Builder)> = None;
        let mut binary: Option<(BinaryBuilder, BinaryBuilder)> = None;

        let mut offset = 0i64;

        for rg in self.metadata.metadata().row_groups().iter() {
            let statistics = rg.column(col_idx).statistics()?;
            match statistics {
                Statistics::Boolean(s) => {
                    let min_max = boolean.get_or_insert_with(|| (
                        BooleanBuilder::with_capacity(num_row_groups),
                        BooleanBuilder::with_capacity(num_row_groups)
                    ));
                    if s.has_min_max_set() {
                        min_max.0.append_value(*s.min());
                        min_max.1.append_value(*s.max());
                    } else {
                        min_max.0.append_null();
                        min_max.1.append_null();
                    }
                }
                Statistics::Int32(s) => {
                    let min_max = int32.get_or_insert_with(|| (
                        Int32Builder::with_capacity(num_row_groups),
                        Int32Builder::with_capacity(num_row_groups)
                    ));
                    if s.has_min_max_set() {
                        min_max.0.append_value(*s.min());
                        min_max.1.append_value(*s.max());
                    } else {
                        min_max.0.append_null();
                        min_max.1.append_null();
                    }
                }
                Statistics::Int64(s) => {
                    let min_max = int64.get_or_insert_with(|| (
                        Int64Builder::with_capacity(num_row_groups),
                        Int64Builder::with_capacity(num_row_groups)
                    ));
                    if s.has_min_max_set() {
                        min_max.0.append_value(*s.min());
                        min_max.1.append_value(*s.max());
                    } else {
                        min_max.0.append_null();
                        min_max.1.append_null();
                    }
                }
                Statistics::ByteArray(s) => {
                    let min_max = binary.get_or_insert_with(|| (
                        BinaryBuilder::new(),
                        BinaryBuilder::new()
                    ));
                    if s.has_min_max_set() {
                        min_max.0.append_value(s.min());
                        min_max.1.append_value(s.max());
                    } else {
                        min_max.0.append_null();
                        min_max.1.append_null();
                    }
                }
                _ => return None
            }
            ranges.push(offset..offset + rg.num_rows());
            offset += rg.num_rows();
        }

        macro_rules! complete_min_max {
            ($builder:ident, $num_row_groups:ident) => {
                $builder.and_then(|mut min_max| {
                    if min_max.0.len() == $num_row_groups {
                        Some((
                            Arc::new(min_max.0.finish()) as ArrayRef,
                            Arc::new(min_max.1.finish()) as ArrayRef,
                        ))
                    } else {
                        None
                    }
                })
            };
        }

        None.or_else(|| {
            complete_min_max!(binary, num_row_groups)
        }).or_else(|| {
            complete_min_max!(int32, num_row_groups)
        }).or_else(|| {
            complete_min_max!(int64, num_row_groups)
        }).or_else(|| {
            complete_min_max!(boolean, num_row_groups)
        }).map(|min_max| {
            ColStats {
                ranges,
                min: min_max.0,
                max: min_max.1,
            }
        })
    }
}


struct PageStats {
    metadata: ArrowReaderMetadata,
    row_group_idx: usize,
    column_stats: parking_lot::Mutex<HashMap<Name, Option<Arc<dyn ColumnStats>>>>
}


impl PageStats {
    pub fn new(metadata: ArrowReaderMetadata, row_group_idx: usize) -> Self {
        let num_columns = metadata.parquet_schema().num_columns();
        Self {
            metadata,
            row_group_idx,
            column_stats: parking_lot::Mutex::new(HashMap::with_capacity(num_columns))
        }
    }
}


impl RowStats for PageStats {
    fn get_column_stats(&self, column: Name) -> Option<Arc<dyn ColumnStats>> {
        let mut column_stats = self.column_stats.lock();
        column_stats.entry(column).or_insert_with(|| {
            self.build_column_stats(column).map(|s| Arc::new(s) as Arc<dyn ColumnStats>)
        }).clone()
    }

    fn get_num_rows(&self) -> usize {
        self.metadata.metadata().row_group(self.row_group_idx).num_rows() as usize
    }
}


impl PageStats {
    fn build_column_stats(&self, column_name: Name) -> Option<ColStats> {
        let col_idx = find_primitive_column(&self.metadata, column_name)?;

        let ranges = self.metadata
            .metadata()
            .offset_index()
            .map(|offset_index| {
                let pages = &offset_index[self.row_group_idx][col_idx];
                let mut ranges = Vec::with_capacity(pages.len());
                for i in 0..pages.len() - 1 {
                    ranges.push(pages[i].first_row_index..pages[i+1].first_row_index);
                }
                if let Some(last_page) = pages.last() {
                    let num_rows = self.metadata.metadata().row_group(self.row_group_idx).num_rows();
                    ranges.push(last_page.first_row_index..num_rows);
                }
                ranges
            })?;

        let page_index = self.metadata
            .metadata()
            .column_index()
            .map(|ci| {
                &ci[self.row_group_idx][col_idx]
            })?;

        let (min, max): (ArrayRef, ArrayRef) = match page_index {
            Index::NONE => return None,
            Index::BYTE_ARRAY(s) => {
                (
                    Arc::new(BinaryArray::from_iter(s.indexes.iter().map(|p| p.min.clone()))),
                    Arc::new(BinaryArray::from_iter(s.indexes.iter().map(|p| p.max.clone())))
                )
            },
            Index::INT32(s) => {
                (
                    Arc::new(Int32Array::from_iter(s.indexes.iter().map(|p| p.min))),
                    Arc::new(Int32Array::from_iter(s.indexes.iter().map(|p| p.max)))
                )
            },
            Index::INT64(s) => {
                (
                    Arc::new(Int64Array::from_iter(s.indexes.iter().map(|p| p.min))),
                    Arc::new(Int64Array::from_iter(s.indexes.iter().map(|p| p.max)))
                )
            },
            Index::BOOLEAN(s) => {
                (
                    Arc::new(BooleanArray::from_iter(s.indexes.iter().map(|p| p.min))),
                    Arc::new(BooleanArray::from_iter(s.indexes.iter().map(|p| p.max)))
                )
            },
            _ => return None
        };

        Some(ColStats {
            ranges,
            min,
            max
        })
    }
}


fn find_primitive_column(metadata: &ArrowReaderMetadata, name: Name) -> Option<usize> {
    for (idx, col) in metadata.parquet_schema().columns().iter().enumerate() {
        if col.name() == name && col.self_type().is_primitive() {
            return Some(idx);
        }
    }
    None
}


struct ColStats {
    ranges: Vec<Range<i64>>,
    min: ArrayRef,
    max: ArrayRef
}


impl ArrayStats for ColStats {
    fn get_min(&self) -> ArrayRef{
        self.min.clone()
    }

    fn get_max(&self) -> ArrayRef {
        self.max.clone()
    }
}


impl ColumnStats for ColStats {
    fn get_ranges(&self) -> &[Range<i64>] {
        self.ranges.as_slice()
    }

    fn as_array_stats(&self) -> &dyn ArrayStats {
        self
    }
}
