use super::cast::{IndexCastReader, MaybeCastedReader};
use super::schema_merge::{data_types_equal, merge_schema};
use crate::db::SnapshotTableReader;
use anyhow::ensure;
use arrow::datatypes::{DataType, SchemaRef};
use sqd_array::builder::AnyTableBuilder;
use sqd_array::chunking::ChunkRange;
use sqd_array::reader::ArrayReader;
use sqd_array::schema_metadata::{get_sort_key, SQD_SORT_KEY};
use sqd_array::schema_patch::SchemaPatch;
use sqd_array::slice::{AsSlice, Slice};
use sqd_array::sort::sort_table_to_indexes;
use sqd_array::util::{build_field_offsets, build_offsets};
use sqd_array::writer::ArrayWriter;
use std::sync::Arc;


pub struct TableMerge<'a> {
    chunks: &'a [Arc<SnapshotTableReader<'a>>],
    schema: SchemaRef,
    sort_key: Vec<usize>,
    columns_with_stats: Vec<usize>,
    column_offsets: Vec<usize>
}


impl<'a> TableMerge<'a> {
    pub fn prepare(chunks: &'a [Arc<SnapshotTableReader<'a>>]) -> anyhow::Result<Self> {
        ensure!(chunks.len() > 0, "nothing to merge");
        let last_chunk = chunks.last().unwrap().clone();

        let mut schema = SchemaPatch::new(
            strip_unknown_metadata(last_chunk.schema())
        );

        for t in chunks.iter().rev().skip(1) {
            merge_schema(&mut schema, &t.schema())?;
        }

        let schema = schema.finish();
        let sort_key = get_sort_key(&schema)?;
        let column_offsets = build_field_offsets(0, schema.fields());

        let columns_with_stats = (0..last_chunk.schema().fields().len())
            .filter_map(|i| {
                last_chunk.get_column_stats(i)
                    .map(|maybe_stats| maybe_stats.map(|_| i))
                    .transpose()
            })
            .collect::<anyhow::Result<Vec<usize>>>()?;

        Ok(Self {
            chunks,
            schema,
            sort_key,
            columns_with_stats,
            column_offsets
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn columns_with_stats(&self) -> &[usize] {
        &self.columns_with_stats
    }

    pub fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        if self.sort_key.len() > 0 {
            self.sorted_write(dst)
        } else {
            self.unsorted_write(dst)
        }
    }

    fn sorted_write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        let sort_table_builder = self.read_sort_key()?;
        let sort_table = sort_table_builder.as_slice();

        let order = sort_table_to_indexes(
            &sort_table,
            &(0..sort_table.num_columns()).collect::<Vec<_>>()
        );

        for i in self.sort_key.iter().copied() {
            let mut dst = dst.shift(self.column_offsets[i]);
            sort_table.column(i).write_indexes(&mut dst, order.iter().copied())?
        }

        drop(sort_table_builder);

        if self.sort_key.len() == self.num_columns() {
            return Ok(())
        }

        let chunks = ChunkRange::build_tag_list(
            &build_offsets(0, self.chunks.iter().map(|t| t.num_rows())),
            &order
        );

        for i in 0..self.num_columns() {
            if self.sort_key.contains(&i) {
                continue
            }
            let mut dst = dst.shift(self.column_offsets[i]);
            self.read_sorted_column(i, &chunks, &mut dst)?;
        }

        Ok(())
    }

    fn read_sort_key(&self) -> anyhow::Result<AnyTableBuilder> {
        let schema = Arc::new(self.schema.project(&self.sort_key)?);
        let mut builder = AnyTableBuilder::new(schema);
        for (ki, ci) in self.sort_key.iter().copied().enumerate() {
            let dst = builder.column_writer(ki);
            self.read_unsorted_column(ci, dst)?
        }
        Ok(builder)
    }

    fn read_unsorted_column(&self, index: usize, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        let field = self.schema.field(index);
        let chunk_columns = self.chunk_columns(index);
        for (i, t) in self.chunks.iter().enumerate() {
            let ci = chunk_columns[i];
            create_maybe_casted_reader(t, ci, field.data_type())?.read(dst)?
        }
        Ok(())
    }

    fn read_sorted_column(
        &self,
        index: usize,
        order: &[ChunkRange],
        dst: &mut impl ArrayWriter
    ) -> anyhow::Result<()>
    {
        let field = self.schema.field(index);
        let chunk_columns = self.chunk_columns(index);

        let needs_cast = self.chunks.iter()
            .enumerate()
            .any(|(i, t)| {
                !data_types_equal(
                    field.data_type(),
                    t.schema().field(chunk_columns[i]).data_type()
                )
            });

        if needs_cast {
            self.read_sorted_column_with_cast(
                field.data_type(),
                &chunk_columns,
                order,
                dst
            )
        } else {
            let mut readers = self.chunks.iter()
                .enumerate()
                .map(|(i, t)| {
                    t.create_column_reader(chunk_columns[i])
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            ArrayReader::read_chunk_ranges(
                readers.as_mut_slice(),
                dst,
                order.iter().cloned()
            )
        }
    }

    fn read_sorted_column_with_cast(
        &self,
        target_data_type: &DataType,
        chunk_columns: &[usize],
        order: &[ChunkRange],
        dst: &mut impl ArrayWriter
    ) -> anyhow::Result<()>
    {
        let mut readers = self.chunks.iter()
            .enumerate()
            .map(|(i, t)| {
                let ci = chunk_columns[i];
                create_maybe_casted_reader(t, ci, target_data_type)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        for range in order.iter() {
            readers[range.chunk_index()].read_slice(
                dst,
                range.offset_index(),
                range.len_index()
            )?;
        }

        Ok(())
    }

    fn chunk_columns(&self, index: usize) -> Vec<usize> {
        let name = self.schema.field(index).name();
        self.chunks.iter().map(|c| {
            let schema = c.schema();
            if schema.fields()[index].name() == name {
                index
            } else {
                schema.index_of(name).expect("chunk and target schema mismatch")
            }
        }).collect()
    }

    fn unsorted_write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        for i in 0..self.num_columns() {
            let mut dst = dst.shift(self.column_offsets[i]);
            self.read_unsorted_column(i, &mut dst)?
        }
        Ok(())
    }
}


fn create_maybe_casted_reader<'a>(
    table: &SnapshotTableReader<'a>,
    column_index: usize,
    target_type: &DataType
) -> anyhow::Result<MaybeCastedReader<impl ArrayReader + 'a>>
{
    let schema = table.schema();
    let field = schema.field(column_index);
    let reader = table.create_column_reader(column_index)?;
    Ok(if data_types_equal(field.data_type(), target_type) {
        MaybeCastedReader::Plain(reader)
    } else {
        MaybeCastedReader::Cast(
            IndexCastReader::new(reader, field.data_type(), target_type.clone())
        )
    })
}


fn strip_unknown_metadata(schema: SchemaRef) -> SchemaRef {
    let mut patch = SchemaPatch::new(schema);
    if patch.metadata().keys().any(|key| key != SQD_SORT_KEY) {
        patch.metadata_mut().retain(|k, _| k == SQD_SORT_KEY)
    }
    patch.finish()
}