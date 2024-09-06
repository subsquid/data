use anyhow::ensure;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryArray, BooleanArray, GenericByteArray, PrimitiveArray, StringArray};
use arrow::datatypes::{ByteArrayType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_buffer::{bit_util, ArrowNativeType, BooleanBuffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use std::sync::Arc;


pub fn get_num_buffers(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean |
        DataType::Int8 |
        DataType::Int16 |
        DataType::Int32 |
        DataType::Int64 |
        DataType::UInt8 |
        DataType::UInt16 |
        DataType::UInt32 |
        DataType::UInt64 |
        DataType::Float16 |
        DataType::Float32 |
        DataType::Float64 |
        DataType::Timestamp(_, _) |
        DataType::Date32 |
        DataType::Date64 |
        DataType::Time32(_) |
        DataType::Time64(_) |
        DataType::Duration(_) |
        DataType::Interval(_) => {
            2
        }
        DataType::Binary |
        DataType::Utf8 => {
            3
        }
        DataType::List(f) => {
            2 + get_num_buffers(f.data_type())
        }
        DataType::Struct(fields) => {
            1 + fields.iter().map(|f| get_num_buffers(f.data_type())).sum::<usize>()
        }
        ty => panic!("unsupported arrow data type - {}", ty)
    }
}


pub type BufferCallback<'a> = &'a mut dyn FnMut(usize, &[u8]);


pub trait BufferBag {
    fn for_each_buffer(&self, cb: BufferCallback<'_>);

    fn serialize(&self, out: &mut Vec<u8>) {
        let mut byte_len = 0;

        self.for_each_buffer(&mut |_, buf| {
            byte_len += buf.len();
            byte_len += size_of::<u32>();
        });

        out.reserve(byte_len);

        self.for_each_buffer(&mut |len, buf| {
            out.extend_from_slice(&(len as u32).to_le_bytes());
            out.extend_from_slice(buf)
        })
    }
}


pub struct BufferReader<'a> {
    data: &'a [u8]
}


impl <'a> BufferReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data
        }
    }

    pub fn next_len(&mut self) -> anyhow::Result<usize> {
        let bytes = self.next_bytes(size_of::<u32>())?;
        let val = u32::from_le_bytes(bytes.try_into().unwrap());
        Ok(val as usize)
    }

    pub fn next_buffer(&mut self, len: usize) -> anyhow::Result<&'a [u8]> {
        self.next_bytes(len)
    }

    fn next_bytes(&mut self, len: usize) -> anyhow::Result<&'a [u8]> {
        ensure!(
            self.data.len() >= len,
            "unexpected EOF: wanted to take {} byte(s), but only {} has left",
            len,
            self.data.len()
        );
        let (next, left) = self.data.split_at(len);
        self.data = left;
        Ok(next)
    }

    pub fn ensure_eof(&self) -> anyhow::Result<()> {
        ensure!(
            self.data.len() == 0,
            "expected to reach EOF, but got {} byte(s) left",
            self.data.len()
        );
        Ok(())
    }
}


pub trait FromBufferReader: Sized {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self>;
}


impl <'a> BufferBag for Option<&'a NullBuffer> {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        if let Some(buf) = self {
            cb(buf.len(), buf.buffer())
        } else {
            cb(0, &[])
        }
    }
}


impl FromBufferReader for Option<NullBuffer> {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self> {
        let len = reader.next_len()?;
        if len == 0 {
            return Ok(None)
        }
        let byte_len = bit_util::ceil(len, 8);
        let data = reader.next_buffer(byte_len)?;
        let mut buf = MutableBuffer::new(byte_len);
        buf.extend_from_slice(data);
        Ok(Some(NullBuffer::new(BooleanBuffer::new(buf.into(), 0, len))))
    }
}


impl <T: ArrowNativeType> BufferBag for ScalarBuffer<T> {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        cb(self.len(), self.inner())
    }
}


impl <T: ArrowNativeType> FromBufferReader for ScalarBuffer<T> {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self> {
        let len = reader.next_len()?;
        let byte_len = len * T::get_byte_width();
        let data = reader.next_buffer(byte_len)?;
        let mut buf = MutableBuffer::new(byte_len);
        buf.extend_from_slice(data);
        Ok(ScalarBuffer::from(buf))
    }
}


impl <T: ArrowPrimitiveType> BufferBag for PrimitiveArray<T> {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        self.nulls().for_each_buffer(cb);
        self.values().for_each_buffer(cb)
    }
}


impl <T: ArrowPrimitiveType> FromBufferReader for PrimitiveArray<T> {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self> {
        let nulls = Option::<NullBuffer>::from_buffer_reader(reader)?;
        let values = ScalarBuffer::<T::Native>::from_buffer_reader(reader)?;
        let array = PrimitiveArray::try_new(values, nulls)?;
        Ok(array)
    }
}


impl BufferBag for BooleanBuffer {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        cb(self.len(), self.inner())
    }
}


impl FromBufferReader for BooleanBuffer {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self> {
        let len = reader.next_len()?;
        let byte_len = bit_util::ceil(len, 8);
        let data = reader.next_buffer(byte_len)?;
        let mut buf = MutableBuffer::new(byte_len);
        buf.extend_from_slice(data);
        Ok(BooleanBuffer::new(buf.into(), 0, len))
    }
}


impl BufferBag for BooleanArray {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        self.nulls().for_each_buffer(cb);
        self.values().for_each_buffer(cb)
    }
}


impl FromBufferReader for BooleanArray {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self> {
        let nulls = Option::<NullBuffer>::from_buffer_reader(reader)?;
        let values = BooleanBuffer::from_buffer_reader(reader)?;
        if let Some(nulls) = nulls.as_ref() {
            ensure!(
                nulls.len() == values.len(),
                "null mask length and boolean values array length do not match"
            );
        }
        Ok(BooleanArray::new(values, nulls))
    }
}


impl <T: ByteArrayType> BufferBag for GenericByteArray<T> {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        self.nulls().for_each_buffer(cb);
        self.offsets().inner().for_each_buffer(cb);
        cb(self.values().len(), self.values())
    }
}


impl <T: ByteArrayType> FromBufferReader for GenericByteArray<T> {
    fn from_buffer_reader(reader: &mut BufferReader) -> anyhow::Result<Self> {
        let nulls = Option::<NullBuffer>::from_buffer_reader(reader)?;

        let offsets = {
            let buf = ScalarBuffer::<T::Offset>::from_buffer_reader(reader)?;
            validate_offsets(&buf)?;
            OffsetBuffer::new(buf)
        };

        let values = ScalarBuffer::<u8>::from_buffer_reader(reader)?;

        let array = GenericByteArray::try_new(offsets, values.into_inner(), nulls)?;
        Ok(array)
    }
}


impl <'a> BufferBag for &'a dyn Array {
    fn for_each_buffer(&self, cb: BufferCallback<'_>) {
        match self.data_type() {
            DataType::Boolean => self.as_boolean().for_each_buffer(cb),
            DataType::Int8 => self.as_primitive::<Int8Type>().for_each_buffer(cb),
            DataType::Int16 => self.as_primitive::<Int16Type>().for_each_buffer(cb),
            DataType::Int32 => self.as_primitive::<Int32Type>().for_each_buffer(cb),
            DataType::Int64 => self.as_primitive::<Int64Type>().for_each_buffer(cb),
            DataType::UInt8 => self.as_primitive::<UInt8Type>().for_each_buffer(cb),
            DataType::UInt16 => self.as_primitive::<UInt16Type>().for_each_buffer(cb),
            DataType::UInt32 => self.as_primitive::<UInt32Type>().for_each_buffer(cb),
            DataType::UInt64 => self.as_primitive::<UInt64Type>().for_each_buffer(cb),
            DataType::Binary => self.as_binary::<i32>().for_each_buffer(cb),
            DataType::Utf8 => self.as_string::<i32>().for_each_buffer(cb),
            ty => panic!("unsupported arrow type - {}", ty)
        }
    }
}


pub fn read_array_from_buffers<'a>(
    reader: &mut BufferReader<'a>,
    data_type: &DataType
) -> anyhow::Result<ArrayRef>
{
    macro_rules! ok {
        ($arr:expr) => {
            Ok(Arc::new($arr))
        };
    }
    match data_type {
        DataType::Boolean => ok!(BooleanArray::from_buffer_reader(reader)?),
        DataType::Int8 => ok!(PrimitiveArray::<Int8Type>::from_buffer_reader(reader)?),
        DataType::Int16 => ok!(PrimitiveArray::<Int16Type>::from_buffer_reader(reader)?),
        DataType::Int32 => ok!(PrimitiveArray::<Int32Type>::from_buffer_reader(reader)?),
        DataType::Int64 => ok!(PrimitiveArray::<Int64Type>::from_buffer_reader(reader)?),
        DataType::UInt8 => ok!(PrimitiveArray::<UInt8Type>::from_buffer_reader(reader)?),
        DataType::UInt16 => ok!(PrimitiveArray::<UInt16Type>::from_buffer_reader(reader)?),
        DataType::UInt32 => ok!(PrimitiveArray::<UInt32Type>::from_buffer_reader(reader)?),
        DataType::UInt64 => ok!(PrimitiveArray::<UInt64Type>::from_buffer_reader(reader)?),
        DataType::Binary => ok!(BinaryArray::from_buffer_reader(reader)?),
        DataType::Utf8 => ok!(StringArray::from_buffer_reader(reader)?),
        ty => panic!("unsupported arrow type - {}", ty)
    }
}


pub fn validate_offsets<T: Ord + Default + Copy>(offsets: &[T]) -> anyhow::Result<()> {
    ensure!(offsets.len() > 0, "got empty offsets array");
    ensure!(offsets[0] == T::default(), "offsets array does not start from 0");
    validate_offsets_monotonicity(offsets)?;
    Ok(())
}


pub fn validate_offsets_monotonicity<T: Ord + Default + Copy>(
    offsets: &[T]
) -> anyhow::Result<()>
{
    let mut prev = offsets[0];
    for i in 1..offsets.len() {
        let current = offsets[i];
        ensure!(prev <= current, "offset values are not monotonically increasing");
        prev = current
    }
    Ok(())
}


pub fn add_null_mask(array: &dyn Array, nulls: NullBuffer) -> ArrayRef {
    let data = array.to_data()
        .into_builder()
        .nulls(Some(nulls))
        .build()
        .unwrap();
    arrow::array::make_array(data)
}