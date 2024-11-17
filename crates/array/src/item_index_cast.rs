use crate::offsets::Offsets;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::{AnyListItem, AnySlice, ListSlice, PrimitiveSlice, Slice};
use crate::writer::{ArrayWriter, NativeWriter, OffsetsWriter};
use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::ArrowNativeType;
use std::fmt::Debug;
use std::sync::Arc;


pub fn is_item_index(data_type: &DataType) -> bool {
    match data_type {
        DataType::UInt16 => true,
        DataType::UInt32 => true,
        DataType::UInt64 => true,
        DataType::List(f) => {
            match f.data_type() {
                DataType::UInt16 => true,
                DataType::UInt32 => true,
                _ => false
            }
        },
        _ => false
    }
}


pub fn common_item_index_type(a: &DataType, b: &DataType) -> Option<DataType> {
    match (a, b) { 
        (DataType::List(ai), DataType::List(bi)) => {
            common_index(ai.data_type(), bi.data_type()).map(|t| {
                let field = Field::new(
                    ai.name(),
                    t,
                    ai.is_nullable() || bi.is_nullable()
                );
                DataType::List(Arc::new(field))
            })
        },
        (a, b) => common_index(a, b)
    }
}


fn common_index(a: &DataType, b: &DataType) -> Option<DataType> {
    common_index_inner(a, b).or_else(|| common_index_inner(b, a))
}


fn common_index_inner(a: &DataType, b: &DataType) -> Option<DataType> {
    match (a, b) {
        (DataType::UInt16, DataType::UInt16) => Some(DataType::UInt16),
        (DataType::UInt16, DataType::UInt32) => Some(DataType::UInt32),
        (DataType::UInt32, DataType::UInt32) => Some(DataType::UInt32),
        (DataType::UInt32, DataType::UInt64) => Some(DataType::UInt64),
        (DataType::UInt64, DataType::UInt64) => Some(DataType::UInt64),
        _ => None
    }
}


macro_rules! invalid_cast {
    ($from:expr, $to:expr) => {
        bail!("invalid index cast from {} to {}", $from, $to)
    };
}


pub fn cast_item_index(
    src: &AnySlice<'_>,
    target_type: &DataType,
    dst: &mut impl ArrayWriter
) -> anyhow::Result<()>
{
    match src {
        AnySlice::UInt16(s) => match target_type {
            DataType::UInt32 => cast_primitive::<_, u32>(s, dst),
            _ => invalid_cast!(DataType::UInt16, target_type)
        },
        AnySlice::UInt32(s) => match target_type {
            DataType::UInt16 => cast_primitive::<_, u16>(s, dst),
            DataType::UInt64 => cast_primitive::<_, u64>(s, dst),
            _ => invalid_cast!(DataType::UInt32, target_type)
        },
        AnySlice::UInt64(s) => match target_type {
            DataType::UInt32 => cast_primitive::<_, u32>(s, dst),
            _ => invalid_cast!(DataType::UInt16, target_type)
        },
        AnySlice::List(s) => cast_address(s, target_type, dst),
        _ => bail!("src is not an index slice")
    }
}


fn cast_primitive<S, T>(
    src: &PrimitiveSlice<'_, S>,
    dst: &mut impl ArrayWriter
) -> anyhow::Result<()>
where
    S: ArrowNativeType,
    T: ArrowNativeType,
    T: TryFrom<S, Error: Debug>
{
    src.nulls().write(dst.nullmask(0))?;
    
    let mut conversion_error: Option<T::Error> = None;
    
    dst.native(1).write_iter(
        src.values().iter().map(|v| {
            match T::try_from(*v) {
                Ok(value) => value,
                Err(err) => {
                    conversion_error = Some(err);
                    T::default()
                }
            }
        })
    )?;
    
    if let Some(err) = conversion_error {
        Err(anyhow!("conversion error: {:?}", err))
    } else {
        Ok(())
    }
}


fn cast_address(
    src: &ListSlice<'_, AnyListItem<'_>>,
    target_type: &DataType,
    dst: &mut impl ArrayWriter
) -> anyhow::Result<()>
{
    match src.values().item() {
        AnySlice::UInt16(items) => match target_type {
            DataType::List(f) if f.data_type() == &DataType::UInt32 => {
                cast_address_impl::<_, u32>(
                    src.nulls(),
                    src.offsets(),
                    items,
                    dst
                )
            },
            _ => invalid_cast!("u16 list", target_type)
        },
        AnySlice::UInt32(items) => match target_type {
            DataType::List(f) if f.data_type() == &DataType::UInt16 => {
                cast_address_impl::<_, u16>(
                    src.nulls(),
                    src.offsets(),
                    items,
                    dst
                )
            },
            _ => invalid_cast!("u32 list", target_type)
        },
        _ => bail!("src is not an index slice")
    }
}


fn cast_address_impl<S, T>(
    nulls: NullmaskSlice<'_>,
    offsets: Offsets<'_>,
    items: PrimitiveSlice<'_, S>, 
    dst: &mut impl ArrayWriter
) -> anyhow::Result<()> 
where
    S: ArrowNativeType,
    T: ArrowNativeType,
    T: TryFrom<S, Error: Debug>
{
    nulls.write(dst.nullmask(0))?;
    
    dst.offset(1).write_slice(offsets)?;

    let item_range = offsets.range();
    
    cast_primitive::<S, T>(
        &items.slice(item_range.start, item_range.len()),
        &mut dst.shift(2)
    )
}