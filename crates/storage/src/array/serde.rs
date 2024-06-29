use std::cmp::min;

use anyhow::{anyhow, Context, ensure};
use arrow::array::{Array, ArrayData, ArrayRef, ArrowPrimitiveType, BufferSpec, layout, make_array, OffsetSizeTrait, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use arrow::datatypes::DataType;
use borsh::{BorshDeserialize, BorshSerialize};


pub fn serialize_array(array: &dyn Array, out: &mut Vec<u8>) {
    serialize_array_data(&array.to_data(), out)
}


pub fn serialize_array_data(array_data: &ArrayData, out: &mut Vec<u8>) {
    array_data.len().serialize(out).unwrap();

    let nulls = array_data.nulls().map(|nulls| nulls.inner().sliced());
    nulls.as_ref().map(|buf| buf.as_slice()).serialize(out).unwrap();

    let num_bufs = array_data.buffers().len();
    num_bufs.serialize(out).unwrap();

    match array_data.data_type() {
        DataType::Boolean => {
            assert_eq!(num_bufs, 1);
            array_data.buffers()[0]
                .bit_slice(array_data.offset(), array_data.len())
                .as_slice()
                .serialize(out)
                .unwrap()
        },
        DataType::Int8 |
        DataType::Int16 |
        DataType::Int32 |
        DataType::Int64 |
        DataType::UInt8 |
        DataType::UInt16 |
        DataType::UInt32 |
        DataType::UInt64 |
        DataType::Timestamp(_, _) => {
            assert_eq!(num_bufs, 1);

            let buffer = &array_data.buffers()[0];
            let layout = layout(array_data.data_type());
            let spec = &layout.buffers[0];

            let byte_width = match spec {
                BufferSpec::FixedWidth { byte_width, .. } => *byte_width,
                _ => 0,
            };

            let min_length = array_data.len() * byte_width;
            let buffer_slice = if buffer_need_truncate(array_data.offset(), buffer, spec, min_length) {
                let byte_offset = array_data.offset() * byte_width;
                let buffer_length = min(min_length, buffer.len() - byte_offset);
                &buffer.as_slice()[byte_offset..(byte_offset + buffer_length)]
            } else {
                buffer.as_slice()
            };

            buffer_slice.serialize(out).unwrap()
        },
        DataType::Binary |
        DataType::Utf8 => {
            assert_eq!(num_bufs, 2);
            let (offsets, values) = get_byte_array_buffers::<i32>(array_data);
            for buffer in [offsets, values] {
                buffer.as_slice().serialize(out).unwrap()
            }
        },
        DataType::List(_) => {
            assert_eq!(num_bufs, 1);
            let (offsets, children) = get_list_array_buffers::<i32>(array_data);
            offsets.as_slice().serialize(out).unwrap();
            serialize_array_data(&children, out)
        },
        DataType::Struct(_) => {
            assert_eq!(num_bufs, 0);
            for child in array_data.child_data().iter() {
                serialize_array_data(child, out)
            }
        },
        t => panic!("don't know how to serialize {}", t)
    }
}


/// Common functionality for re-encoding offsets. Returns the new offsets as well as
/// original start offset and length for use in slicing child data.
fn reencode_offsets<O: OffsetSizeTrait>(
    offsets: &Buffer,
    data: &ArrayData,
) -> (Buffer, usize, usize) {
    let offsets_slice: &[O] = offsets.typed_data::<O>();
    let offset_slice = &offsets_slice[data.offset()..data.offset() + data.len() + 1];

    let start_offset = offset_slice.first().unwrap();
    let end_offset = offset_slice.last().unwrap();

    let offsets = match start_offset.as_usize() {
        0 => offsets.clone(),
        _ => offset_slice.iter().map(|x| *x - *start_offset).collect(),
    };

    let start_offset = start_offset.as_usize();
    let end_offset = end_offset.as_usize();

    (offsets, start_offset, end_offset - start_offset)
}


/// Returns the values and offsets [`Buffer`] for a ByteArray with offset type `O`
///
/// In particular, this handles re-encoding the offsets if they don't start at `0`,
/// slicing the values buffer as appropriate. This helps reduce the encoded
/// size of sliced arrays, as values that have been sliced away are not encoded
fn get_byte_array_buffers<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, Buffer) {
    if data.is_empty() {
        return (MutableBuffer::new(0).into(), MutableBuffer::new(0).into());
    }

    let (offsets, original_start_offset, len) = reencode_offsets::<O>(&data.buffers()[0], data);
    let values = data.buffers()[1].slice_with_length(original_start_offset, len);
    (offsets, values)
}


/// Similar logic as [`get_byte_array_buffers()`] but slices the child array instead
/// of a values buffer.
fn get_list_array_buffers<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, ArrayData) {
    if data.is_empty() {
        return (
            MutableBuffer::new(0).into(),
            data.child_data()[0].slice(0, 0),
        );
    }

    let (offsets, original_start_offset, len) = reencode_offsets::<O>(&data.buffers()[0], data);
    let child_data = data.child_data()[0].slice(original_start_offset, len);
    (offsets, child_data)
}


/// Whether to truncate the buffer
#[inline]
fn buffer_need_truncate(
    array_offset: usize,
    buffer: &Buffer,
    spec: &BufferSpec,
    min_length: usize,
) -> bool {
    spec != &BufferSpec::AlwaysNull && (array_offset != 0 || min_length < buffer.len())
}


pub fn deserialize_array(bytes: &[u8], data_type: DataType) -> anyhow::Result<ArrayRef> {
    let mut input = bytes;
    let array_data = deserialize_array_data(&mut input, data_type)?;
    ensure!(input.len() == 0, "unconsumed bytes are left");
    Ok(make_array(array_data))
}


pub fn deserialize_primitive_array<T: ArrowPrimitiveType>(bytes: &[u8]) -> anyhow::Result<PrimitiveArray<T>> {
    let mut input = bytes;
    let array_data = deserialize_array_data(&mut input, T::DATA_TYPE)?;
    ensure!(input.len() == 0, "unconsumed bytes are left");
    Ok(PrimitiveArray::from(array_data))
}


pub fn deserialize_array_data(input: &mut &[u8], data_type: DataType) -> anyhow::Result<ArrayData> {
    let len = usize::deserialize(input).context("failed to read array length")?;

    let nulls = read_option(input).and_then(|has_nulls| {
        Ok(if has_nulls {
            let buffer = read_buffer(input)?;
            let bit_len = buffer.len().saturating_mul(8);
            ensure!(len <= bit_len, "nulls buffer length doesn't match array length");
            Some(NullBuffer::new(BooleanBuffer::new(buffer, 0, len)))
        } else {
            None
        })
    }).context("failed to read nulls buffer")?;

    let n_buffers = usize::deserialize(input).context("failed to read number of buffers")?;

    let buffers = (0..n_buffers).map(|i| {
        read_buffer(input).with_context(|| {
            format!("failed to read buffer {}", i)
        })
    }).collect::<anyhow::Result<Vec<_>>>()?;

    let children_types = get_child_data_types(&data_type).ok_or_else(|| {
        anyhow!("deserialization of {} is not supported", &data_type)
    })?;

    let children = children_types.into_iter().enumerate().map(|(idx, ty)| {
        let child = deserialize_array_data(input, ty).with_context(|| {
            format!("failed to read child {}", idx)
        })?;
        Ok(child)
    }).collect::<anyhow::Result<Vec<_>>>()?;

    Ok(ArrayData::builder(data_type)
        .len(len)
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()?
    )
}


fn read_option(input: &mut &[u8]) -> anyhow::Result<bool> {
    let flag = u8::deserialize(input)?;
    match flag {
        0 => Ok(false),
        1 => Ok(true),
        x => Err(anyhow!("invalid option flag - {}", x))
    }
}


fn read_buffer(input: &mut &[u8]) -> anyhow::Result<Buffer> {
    let len = u32::deserialize(input).context("failed to read buffer length")? as usize;
    ensure_byte_length(*input, len)?;
    let mut buf = MutableBuffer::with_capacity(len);
    buf.extend_from_slice(&input[0..len]);
    *input = &input[len..];
    Ok(buf.into())
}


#[inline]
fn ensure_byte_length(bytes: &[u8], len: usize) -> anyhow::Result<()> {
    ensure!(bytes.len() >= len, "unexpected end of input");
    Ok(())
}


fn get_child_data_types(data_type: &DataType) -> Option<Vec<DataType>> {
    match data_type {
        DataType::Boolean => Some(vec![]),
        DataType::Int8 => Some(vec![]),
        DataType::Int16 => Some(vec![]),
        DataType::Int32 => Some(vec![]),
        DataType::Int64 => Some(vec![]),
        DataType::UInt8 => Some(vec![]),
        DataType::UInt16 => Some(vec![]),
        DataType::UInt32 => Some(vec![]),
        DataType::UInt64 => Some(vec![]),
        DataType::Timestamp(_, _) => Some(vec![]),
        DataType::Binary => Some(vec![]),
        DataType::Utf8 => Some(vec![]),
        DataType::List(f) => Some(vec![f.data_type().clone()]),
        DataType::Struct(fields) => Some(fields.iter().map(|f| f.data_type().clone()).collect()),
        _ => None
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, PrimitiveArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, UInt32Type};

    use crate::array::serde::{deserialize_array, serialize_array};


    fn test_ser_de(array: &dyn Array) {
        let mut bytes = Vec::new();
        serialize_array(array, &mut bytes);
        let de = deserialize_array(&bytes, array.data_type().clone()).unwrap();
        assert_eq!(de.as_ref(), array)
    }

    #[test]
    fn primitive() {
        test_ser_de(&PrimitiveArray::from(vec![
            Some(1u32),
            Some(2u32),
            None,
            Some(4u32)
        ]))
    }

    #[test]
    fn struct_array_with_offset() {
        let array = StructArray::new(
            Fields::from(vec![
                Field::new("a", DataType::UInt32, true),
                Field::new("b", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                    Some(0),
                    Some(1),
                    None,
                    Some(3)
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("b"),
                    Some("c")
                ]))
            ],
            None
        ).slice(1, 2);

        test_ser_de(&array)
    }
}