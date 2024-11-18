use crate::downcast::Downcast;
use crate::table_file::{TableFile, TableFileWriter};
use crate::{SortedTable, TableSorter};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, SchemaRef};
use sqd_array::builder::{AnyBuilder, AnyTableBuilder, ArrayBuilder};
use sqd_array::item_index_cast::cast_item_index;
use sqd_array::slice::{AnyTableSlice, AsSlice, Slice};
use sqd_array::util::build_field_offsets;
use sqd_array::writer::ArrayWriter;
use sqd_dataset::TableDescription;
use std::collections::HashMap;
use std::sync::Arc;
use sqd_array::schema_patch::SchemaPatch;


enum TableWriter {
    Plain(TableFileWriter),
    Sort(TableSorter)
}


impl TableWriter {
    fn push_batch(&mut self, records: &AnyTableSlice<'_>) -> anyhow::Result<()> {
        match self {
            TableWriter::Plain(w) => w.push_batch(records),
            TableWriter::Sort(w) => w.push_batch(records),
        }
    }
    
    fn into_reader(self) -> anyhow::Result<TableReader> {
        match self {
            TableWriter::Plain(w) => w.finish().map(TableReader::Plain),
            TableWriter::Sort(w) => w.finish().map(TableReader::Sort)
        }
    }
}


enum TableReader {
    Plain(TableFile),
    Sort(SortedTable)
}


impl TableReader {
    fn read_column(
        &mut self,
        dst: &mut impl ArrayWriter,
        i: usize,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()> {
        match self {
            TableReader::Plain(reader) => {
                reader.read_column(dst, i, offset, len)
            },
            TableReader::Sort(reader) => {
                reader.read_column(dst, i, offset, len)
            }
        }
    }
    
    fn into_writer(self) -> anyhow::Result<TableWriter> {
        match self {
            TableReader::Plain(reader) => reader.into_writer().map(TableWriter::Plain),
            TableReader::Sort(reader) => reader.into_sorter().map(TableWriter::Sort),
        }
    }
}


pub struct TableProcessor {
    downcast: Downcast,
    schema: SchemaRef,
    block_number_columns: Vec<usize>,
    item_index_columns: Vec<usize>,
    writer: TableWriter,
    num_rows: usize,
    byte_size: usize
}


impl TableProcessor {
    pub fn new(
        downcast: Downcast,
        schema: SchemaRef,
        desc: &TableDescription
    ) -> anyhow::Result<Self>
    {
        let block_number_columns = desc.downcast.block_number.iter().map(|name| {
            schema.index_of(name)
        }).collect::<Result<Vec<_>, _>>()?;

        let item_index_columns = desc.downcast.item_index.iter().map(|name| {
            schema.index_of(name)
        }).collect::<Result<Vec<_>, _>>()?;

        let sort_key = desc.sort_key.iter().map(|name| {
            schema.index_of(name)
        }).collect::<Result<Vec<_>, _>>()?;

        let writer = if sort_key.len() > 0 {
            TableWriter::Sort(
                TableSorter::new(schema.fields(), sort_key)?
            )
        } else {
            TableWriter::Plain(
                TableFileWriter::new(schema.fields())?
            )
        };

        Ok(Self {
            downcast,
            schema,
            block_number_columns,
            item_index_columns,
            writer,
            num_rows: 0,
            byte_size: 0
        })
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
    
    pub fn byte_size(&self) -> usize {
        self.byte_size
    }

    pub fn push_batch(&mut self, records: &AnyTableSlice<'_>) -> anyhow::Result<()> {
        for i in self.block_number_columns.iter().copied() {
            self.downcast.reg_block_number(&records.column(i))
        }
        
        for i in self.item_index_columns.iter().copied() {
            self.downcast.reg_item_index(&records.column(i))
        }
        
        self.num_rows += records.len();
        self.byte_size += records.byte_size();
        
        self.writer.push_batch(records)
    }

    pub fn finish(self) -> anyhow::Result<PreparedTable> {
        PreparedTable::new(self)
    }
}


pub struct PreparedTable {
    downcast: Downcast,
    block_number_columns: Vec<usize>,
    item_index_columns: Vec<usize>,
    writer_schema: SchemaRef,
    prepared_schema: SchemaRef,
    reader: TableReader,
    column_offsets: Vec<usize>,
    buffers: HashMap<DataType, AnyBuilder>,
    num_rows: usize
}


impl PreparedTable {
    fn new(processor: TableProcessor) -> anyhow::Result<Self> {
        let prepared_schema = downcast_schema(
            processor.schema.clone(),
            &processor.block_number_columns,
            &processor.item_index_columns,
            processor.downcast.get_block_number_type(),
            processor.downcast.get_item_index_type()
        );
        
        let column_offsets = build_field_offsets(0, processor.schema.fields());
        let num_rows = processor.num_rows;
        let reader = processor.writer.into_reader()?;
        
        Ok(Self {
            downcast: processor.downcast,
            block_number_columns: processor.block_number_columns,
            item_index_columns: processor.item_index_columns,
            writer_schema: processor.schema,
            prepared_schema,
            reader,
            column_offsets,
            buffers: HashMap::with_capacity(3),
            num_rows
        })
    }
    
    pub fn into_processor(self) -> anyhow::Result<TableProcessor> {
        Ok(TableProcessor {
            downcast: self.downcast,
            schema: self.writer_schema,
            block_number_columns: self.block_number_columns,
            item_index_columns: self.item_index_columns,
            writer: self.reader.into_writer()?,
            num_rows: 0,
            byte_size: 0
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.prepared_schema.clone()
    }

    pub fn num_columns(&self) -> usize {
        self.column_offsets.len() - 1
    }
    
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
    
    pub fn read_record_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let mut builder = AnyTableBuilder::new(self.prepared_schema.clone());
        self.read(&mut builder, offset, len)?;
        Ok(builder.finish())
    }

    pub fn read(
        &mut self,
        dst: &mut impl ArrayWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()>
    {
        assert!(offset + len <= self.num_rows());
        if len == 0 {
            return Ok(())
        }

        for i in 0..self.num_columns() {
            let mut dst = dst.shift(self.column_offsets[i]);
            self.read_column(&mut dst, i, offset, len)?;
        }

        Ok(())
    }

    pub fn read_column(
        &mut self,
        dst: &mut impl ArrayWriter,
        i: usize,
        mut offset: usize,
        mut len: usize
    ) -> anyhow::Result<()>
    {
        assert!(i < self.num_columns());
        assert!(offset + len <= self.num_rows());
        if len == 0 {
            return Ok(())
        }

        let src_dt = self.writer_schema.field(i).data_type();
        let target_dt = self.prepared_schema.field(i).data_type();
        if src_dt == target_dt {
            self.reader.read_column(dst, i, offset, len)
        } else {
            let buf = self.buffers.entry(src_dt.clone()).or_insert_with(|| {
               AnyBuilder::new(src_dt)
            });

            while len > 0 {
                let step_len = std::cmp::min(len, 1000);

                buf.clear();
                self.reader.read_column(buf, i, offset, step_len)?;
                cast_item_index(&buf.as_slice(), target_dt, dst)?;

                offset += step_len;
                len -= step_len;
            }

            Ok(())
        }
    }
}


fn downcast_schema(
    schema: SchemaRef,
    block_number_columns: &[usize],
    item_index_columns: &[usize],
    block_number_type: DataType,
    item_index_type: DataType
) -> SchemaRef
{
    let mut patch = SchemaPatch::new(schema.clone());

    for (columns, ty) in [
        (block_number_columns, block_number_type),
        (item_index_columns, item_index_type)
    ] {
        for idx in columns.iter().copied() {
            let f = schema.field(idx);
            
            let target_type = match f.data_type() {
                DataType::List(f) => DataType::List(
                    Arc::new(Field::new(f.name(), ty.clone(), f.is_nullable()))
                ),
                _ => ty.clone()
            };
            
            patch.set_field_type(idx, target_type)
        }
    }

    patch.finish()
}