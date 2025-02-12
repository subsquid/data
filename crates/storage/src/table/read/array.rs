use anyhow::{anyhow, ensure, Context};
use arrow::array::{ArrayDataBuilder, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, ListArray, PrimitiveArray, StringArray, StructArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowNativeType, DataType, Decimal128Type, FieldRef, Fields, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_buffer::MutableBuffer;
use rayon::prelude::*;
use sqd_array::util::build_field_offsets;
use sqd_primitives::range::RangeList;
use std::sync::Arc;


pub trait Storage: Sync {
    fn read_native<T: ArrowNativeType>(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<ScalarBuffer<T>>
    {
        let buf = self.read_native_bytes(buffer, T::get_byte_width(), ranges)?;
        Ok(ScalarBuffer::from(buf))
    }

    fn read_native_bytes(
        &self,
        buffer: usize,
        item_size: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<MutableBuffer>;

    fn read_boolean(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<BooleanBuffer>;
    
    fn read_null_mask(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<Option<NullBuffer>>;

    fn read_offsets(
        &self,
        buffer: usize,
        ranges: Option<&RangeList<u32>>
    ) -> anyhow::Result<(OffsetBuffer<i32>, Option<RangeList<u32>>)>;
}


pub fn read_array(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>,
    data_type: &DataType
) -> anyhow::Result<ArrayRef> {
    match data_type {
        DataType::Boolean => read_boolean_array(storage, pos, ranges),
        DataType::Int8 => read_primitive_array::<Int8Type>(storage, pos, ranges),
        DataType::Int16 => read_primitive_array::<Int16Type>(storage, pos, ranges),
        DataType::Int32 => read_primitive_array::<Int32Type>(storage, pos, ranges),
        DataType::Int64 => read_primitive_array::<Int64Type>(storage, pos, ranges),
        DataType::UInt8 => read_primitive_array::<UInt8Type>(storage, pos, ranges),
        DataType::UInt16 => read_primitive_array::<UInt16Type>(storage, pos, ranges),
        DataType::UInt32 => read_primitive_array::<UInt32Type>(storage, pos, ranges),
        DataType::UInt64 => read_primitive_array::<UInt64Type>(storage, pos, ranges),
        DataType::Float16 => read_primitive_array::<Float16Type>(storage, pos, ranges),
        DataType::Float32 => read_primitive_array::<Float32Type>(storage, pos, ranges),
        DataType::Float64 => read_primitive_array::<Float64Type>(storage, pos, ranges),
        DataType::Decimal128(_, _) => read_primitive_array::<Decimal128Type>(storage, pos, ranges),
        DataType::Timestamp(unit, tz) => read_primitive_data(
            storage,
            pos,
            ranges,
            DataType::Timestamp(*unit, tz.clone())
        ),
        DataType::Binary => read_binary(storage, pos, ranges),
        DataType::Utf8 => read_string(storage, pos, ranges),
        DataType::List(f) => read_list(storage, pos, ranges, f.clone()),
        DataType::Struct(fields) => read_struct(storage, pos, ranges, fields.clone()),
        ty => panic!("unsupported arrow data type - {}", ty)
    }
}


fn read_boolean_array(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>
) -> anyhow::Result<ArrayRef>
{
    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;

    let values = storage.read_boolean(pos + 1, ranges)
        .context("failed to read boolean values")?;

    if let Some(mask) = nulls.as_ref() {
        ensure!(
            mask.len() == values.len(),
            "null mask length does not match the length of the array"
        );
    }

    let array = BooleanArray::new(values, nulls);

    Ok(Arc::new(array))
}


fn read_primitive_array<T: ArrowPrimitiveType>(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>
) -> anyhow::Result<ArrayRef>
{
    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;

    let values = storage.read_native::<T::Native>(pos + 1, ranges)
        .context("failed to read values buffer")?;

    let array = PrimitiveArray::<T>::try_new(values, nulls)?;

    Ok(Arc::new(array))
}


fn read_primitive_data(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>,
    data_type: DataType
) -> anyhow::Result<ArrayRef>
{
    let value_size = data_type.primitive_width().expect("not a primitive data type");

    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;

    let values = storage.read_native_bytes(pos + 1, value_size, ranges) 
        .context("failed to read values buffer")?;

    let array_data = ArrayDataBuilder::new(data_type)
        .len(values.len() / value_size)
        .nulls(nulls)
        .buffers(vec![values.into()])
        .build()?;

    Ok(arrow::array::make_array(array_data))
}


fn read_binary(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>
) -> anyhow::Result<ArrayRef>
{
    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;

    let (offsets, value_ranges) = storage.read_offsets(pos + 1, ranges)
        .context("failed to read offsets")?;

    let values = storage.read_native::<u8>(pos + 2, value_ranges.as_ref())
        .context("failed to read values array")?;

    let array = BinaryArray::try_new(offsets, values.into_inner(), nulls)?;

    Ok(Arc::new(array))
}


fn read_string(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>
) -> anyhow::Result<ArrayRef>
{
    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;

    let (offsets, value_ranges) = storage.read_offsets(pos + 1, ranges)
        .context("failed to read offsets")?;

    let values = storage.read_native::<u8>(pos + 2, value_ranges.as_ref())
        .context("failed to read values array")?;

    let array = StringArray::try_new(offsets, values.into_inner(), nulls)?;

    Ok(Arc::new(array))
}


fn read_list(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>,
    field: FieldRef
) -> anyhow::Result<ArrayRef>
{
    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;

    let (offsets, value_ranges) = storage.read_offsets(pos + 1, ranges)
        .context("failed to read offsets")?;

    let values = read_array(
        storage,
        pos + 2,
        value_ranges.as_ref(),
        field.data_type()
    ).context("failed to read list values array")?;

    let array = ListArray::try_new(field, offsets, values, nulls)?;

    Ok(Arc::new(array))
}


fn read_struct(
    storage: &impl Storage,
    pos: usize,
    ranges: Option<&RangeList<u32>>,
    fields: Fields
) -> anyhow::Result<ArrayRef>
{
    let nulls = storage.read_null_mask(pos, ranges)
        .context("failed to read null mask")?;
    
    let field_positions = build_field_offsets(pos + 1, &fields);

    let arrays = (0..fields.len())
        .into_par_iter()
        .map(|i| {
            read_array(
                storage, 
                field_positions[i], 
                ranges, 
                fields[i].data_type()
            ).with_context(|| {
                anyhow!("failed to read field `{}`", fields[i].name())
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    
    let struct_array = StructArray::try_new(fields, arrays, nulls)?;
    
    Ok(Arc::new(struct_array))
}