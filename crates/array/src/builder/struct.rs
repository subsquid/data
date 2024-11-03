use crate::builder::memory_writer::MemoryWriter;
use crate::builder::nullmask::NullmaskBuilder;
use crate::builder::{AnyBuilder, ArrayBuilder};
use crate::slice::{AnyStructSlice, AsSlice};
use crate::util::{bisect_offsets, build_field_offsets, invalid_buffer_access};
use crate::writer::{ArrayWriter, Writer};
use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, Fields};
use std::sync::Arc;


pub struct AnyStructBuilder {
    fields: Fields,
    column_offsets: Vec<usize>,
    nulls: NullmaskBuilder,
    columns: Vec<AnyBuilder>
}


impl AnyStructBuilder {
    pub fn new(fields: Fields) -> Self {
        let column_offsets = build_field_offsets(&fields, 1);
        
        let columns = fields.iter()
            .map(|f| AnyBuilder::new(f.data_type()))
            .collect();
        
        Self {
            fields,
            column_offsets,
            nulls: NullmaskBuilder::new(0),
            columns
        }
    }
    
    pub fn finish(self) -> StructArray {
        StructArray::new(
            self.fields,
            self.columns.into_iter().map(|c| c.finish()).collect(),
            self.nulls.finish()
        )
    }
    
    pub unsafe fn finish_unchecked(self) -> StructArray {
        StructArray::new_unchecked(
            self.fields,
            self.columns.into_iter().map(|c| c.finish_unchecked()).collect(),
            self.nulls.finish()
        )
    }
    
    fn find_column(&self, buf: usize) -> (usize, usize) {
        if let Some(col) = bisect_offsets(&self.column_offsets, buf) {
            (col, buf - self.column_offsets[col])
        } else {
            invalid_buffer_access!()
        }
    }
}


impl ArrayWriter for AnyStructBuilder {
    type Writer = MemoryWriter;

    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        let (col, buf) = self.find_column(buf);
        self.columns[col].bitmask(buf)
    }

    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        if buf == 0 {
            &mut self.nulls
        } else {
            let (col, buf) = self.find_column(buf);
            self.columns[col].nullmask(buf)
        }
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


impl AsSlice for AnyStructBuilder {
    type Slice<'a> = AnyStructSlice<'a>;

    fn as_slice(&self) -> Self::Slice<'_> {
        AnyStructSlice::new(
            self.nulls.as_slice(),
            self.columns.iter().map(|c| c.as_slice()).collect()
        )
    }
}


impl ArrayBuilder for AnyStructBuilder {
    fn data_type(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.columns.iter().map(|c| c.byte_size()).sum::<usize>()
    }

    fn clear(&mut self) {
        self.nulls.clear();
        for c in self.columns.iter_mut() {
            c.clear()
        }
    }

    fn finish(self) -> ArrayRef {
        Arc::new(self.finish())
    }

    unsafe fn finish_unchecked(self) -> ArrayRef {
        Arc::new(self.finish_unchecked())
    }
}