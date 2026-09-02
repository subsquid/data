use arrow::{array::RecordBatch, datatypes::SchemaRef};

use crate::{
    builder::{memory_writer::MemoryWriter, AnyBuilder, ArrayBuilder},
    slice::{AnyTableSlice, AsSlice},
    util::{bisect_offsets, build_field_offsets, invalid_buffer_access},
    writer::{ArrayWriter, Writer}
};

pub struct AnyTableBuilder {
    schema: SchemaRef,
    column_offsets: Vec<usize>,
    columns: Vec<AnyBuilder>
}

impl AnyTableBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        let buffers = build_field_offsets(0, schema.fields());

        let columns = schema.fields().iter().map(|f| AnyBuilder::new(f.data_type())).collect();

        Self {
            schema,
            column_offsets: buffers,
            columns
        }
    }

    pub fn finish(self) -> RecordBatch {
        RecordBatch::try_new(self.schema, self.columns.into_iter().map(|c| c.finish()).collect()).unwrap()
    }

    pub unsafe fn finish_unchecked(self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema,
            self.columns.into_iter().map(|c| c.finish_unchecked()).collect()
        )
        .unwrap()
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn column_writer(&mut self, column: usize) -> &mut impl ArrayWriter<Writer = MemoryWriter> {
        &mut self.columns[column]
    }

    pub fn clear(&mut self) {
        for c in self.columns.iter_mut() {
            c.clear()
        }
    }

    fn find_column(&self, buf: usize) -> (usize, usize) {
        if let Some(col) = bisect_offsets(&self.column_offsets, buf) {
            (col, buf - self.column_offsets[col])
        } else {
            invalid_buffer_access!()
        }
    }
}

impl ArrayWriter for AnyTableBuilder {
    type Writer = MemoryWriter;

    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        let (col, buf) = self.find_column(buf);
        self.columns[col].bitmask(buf)
    }

    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        let (col, buf) = self.find_column(buf);
        self.columns[col].nullmask(buf)
    }

    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        let (col, buf) = self.find_column(buf);
        self.columns[col].native(buf)
    }

    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        let (col, buf) = self.find_column(buf);
        self.columns[col].offset(buf)
    }
}

impl AsSlice for AnyTableBuilder {
    type Slice<'a> = AnyTableSlice<'a>;

    fn as_slice(&self) -> Self::Slice<'_> {
        AnyTableSlice::new(self.columns.iter().map(|c| c.as_slice()).collect())
    }
}
