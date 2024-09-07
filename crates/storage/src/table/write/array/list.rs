use crate::table::write::array::bitmask::NullMaskBuilder;
use crate::table::write::array::primitive::NativeBuilder;
use crate::table::write::array::{Builder, FlushCallback};
use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow_buffer::OffsetBuffer;


pub struct ListBuilder<T> {
    nulls: NullMaskBuilder,
    offsets: NativeBuilder<i32>,
    values: T,
    last_offset: i32
}


impl <T: Builder> ListBuilder<T> {
    pub fn new(page_size: usize, values: T) -> Self {
        assert_eq!(values.total_len(), 0);
        
        let mut offsets = NativeBuilder::new(page_size);
        offsets.push_slice(&[0]);

        let mut builder = Self {
            nulls: NullMaskBuilder::new(page_size),
            offsets,
            values,
            last_offset: 0
        };
        builder.set_index(0);
        builder
    }
}


impl <T: Builder> Builder for ListBuilder<T> {
    fn num_buffers(&self) -> usize {
        2 + self.values.num_buffers()
    }

    fn get_index(&self) -> usize {
        self.nulls.get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.nulls.set_index(index);
        self.offsets.set_index(index + 1);
        self.values.set_index(index + 2)
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush(cb)?;
        self.offsets.flush(cb)?;
        self.values.flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush_all(cb)?;
        self.offsets.flush_all(cb)?;
        self.values.flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        let array = array.as_list::<i32>();
        self.nulls.push(array.len(), array.nulls());
        push_offsets(&mut self.offsets, array.offsets(), &mut self.last_offset);
        self.values.push_array(array.values());
    }

    fn total_len(&self) -> usize {
        self.nulls.total_len()
    }
}


fn push_offsets(
    builder: &mut NativeBuilder<i32>,
    offsets: &OffsetBuffer<i32>,
    last_offset: &mut i32
) {
    let beg = offsets[0];

    builder.extend(offsets[1..].iter().map(|o| {
        *o - beg + *last_offset
    }));

    *last_offset += offsets.last().unwrap().clone() - beg;
}


pub struct BinaryBuilder {
    nulls: NullMaskBuilder,
    offsets: NativeBuilder<i32>,
    values: NativeBuilder<u8>,
    last_offset: i32
}


impl BinaryBuilder {
    pub fn new(page_size: usize) -> Self {
        let mut offsets = NativeBuilder::new(page_size);
        offsets.push_slice(&[0]);
        
        let mut builder = Self {
            nulls: NullMaskBuilder::new(page_size),
            offsets,
            values: NativeBuilder::new(page_size),
            last_offset: 0
        };
        builder.set_index(0);
        builder
    }
}


impl Builder for BinaryBuilder {
    fn num_buffers(&self) -> usize {
        3
    }

    fn get_index(&self) -> usize {
        self.nulls.get_index()
    }

    fn set_index(&mut self, index: usize) {
        self.nulls.set_index(index);
        self.offsets.set_index(index + 1);
        self.values.set_index(index + 2)
    }

    fn flush(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush(cb)?;
        self.offsets.flush(cb)?;
        self.values.flush(cb)
    }

    fn flush_all(&mut self, cb: FlushCallback<'_>) -> anyhow::Result<()> {
        self.nulls.flush_all(cb)?;
        self.offsets.flush_all(cb)?;
        self.values.flush_all(cb)
    }

    fn push_array(&mut self, array: &dyn Array) {
        let (offsets, values) = match array.data_type() {
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                (array.offsets(), array.values())
            },
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                (array.offsets(), array.values())
            },
            ty => panic!("got non-binary arrow array - {}", ty)
        };

        self.nulls.push(array.len(), array.nulls());
        push_offsets(&mut self.offsets, offsets, &mut self.last_offset);
        self.values.push_slice(values);
    }

    fn total_len(&self) -> usize {
        self.nulls.total_len()
    }
}