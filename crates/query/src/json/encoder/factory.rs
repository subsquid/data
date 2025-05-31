use super::*;
use crate::primitives::SchemaError;
use arrow::array::{Array, AsArray, BinaryArray, BooleanArray, FixedSizeBinaryArray, GenericListArray, GenericStringArray, PrimitiveArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::*;


macro_rules! _ok_box {
    ($value:expr) => {
        Ok(Box::new($value) as EncoderObject)
    };
}


macro_rules! _nullable_encoder {
    ($maybe_nulls:expr, $encoder:expr, $constructor:path) => {
        if let Some(nulls) = $maybe_nulls {
            $constructor!(NullableEncoder::new($encoder, nulls))
        } else {
            $constructor!($encoder)
        }
    };
}


macro_rules! _primitive_encoder {
    ($array:expr, $t:ty, $constructor:path) => {{
        let array = $array.as_primitive::<$t>();
        let (_, values, nulls) = array.clone().into_parts();
        _nullable_encoder!(nulls, PrimitiveEncoder::new(values), $constructor)
    }};
}


macro_rules! _make_non_list_encoder {
    ($array:expr, $constructor:path) => {{
        let array = $array;
        match array.data_type() {
            DataType::Int8 => _primitive_encoder!(array, Int8Type, $constructor),
            DataType::Int16 => _primitive_encoder!(array, Int16Type, $constructor),
            DataType::Int32 => _primitive_encoder!(array, Int32Type, $constructor),
            DataType::Int64 => _primitive_encoder!(array, Int64Type, $constructor),
            DataType::UInt8 => _primitive_encoder!(array, UInt8Type, $constructor),
            DataType::UInt16 => _primitive_encoder!(array, UInt16Type, $constructor),
            DataType::UInt32 => _primitive_encoder!(array, UInt32Type, $constructor),
            DataType::UInt64 => _primitive_encoder!(array, UInt64Type, $constructor),
            DataType::Float32 => _primitive_encoder!(array, Float32Type, $constructor),
            DataType::Float64 => _primitive_encoder!(array, Float64Type, $constructor),
            DataType::Timestamp(TimeUnit::Second, _) => {
                _primitive_encoder!(array, TimestampSecondType, $constructor)
            },
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                _primitive_encoder!(array, TimestampMillisecondType, $constructor)
            },
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                _primitive_encoder!(array, TimestampMicrosecondType, $constructor)
            },
            DataType::Boolean => {
                let array = array.as_boolean();
                let (values, nulls) = array.clone().into_parts();
                _nullable_encoder!(nulls, BooleanEncoder::new(values), $constructor)
            },
            DataType::FixedSizeBinary(_) => {
                let array = array.as_fixed_size_binary().clone();
                let nulls = array.nulls().cloned();
                _nullable_encoder!(nulls, FixedSizedBinaryEncoder::new(array), $constructor)
            },
            DataType::Binary => {
                let array = array.as_binary::<i32>().clone();
                let nulls = array.nulls().cloned();
                _nullable_encoder!(nulls, BinaryEncoder::new(array), $constructor)
            },
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                let nulls = array.nulls().cloned();
                _nullable_encoder!(nulls, StringEncoder::new(array.clone()), $constructor)
            },
            DataType::Struct(_) => {
                let array = array.as_struct();
                let encoder = make_struct_encoder(array)?;
                let nulls = array.nulls().cloned();
                _nullable_encoder!(nulls, encoder, $constructor)
            },
            _ => Err(SchemaError::new(
                format!("unsupported arrow type - {}", array.data_type())
            ))
        }}
    };
}


pub fn make_encoder(array: &dyn Array) -> Result<EncoderObject, SchemaError> {
    match array.data_type() {
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            let (_, offsets, values, nulls) = array.clone().into_parts();
            match values.data_type() {
                DataType::List(_) => {
                    let item_encoder = make_encoder(&values)?;
                    _nullable_encoder!(nulls, ListEncoder::new(item_encoder, offsets), _ok_box)
                },
                _ => {
                    macro_rules! make_list_encoder {
                        ($item_encoder:expr) => {
                            _nullable_encoder!(nulls, ListEncoder::new($item_encoder, offsets), _ok_box)
                        };
                    }
                    _make_non_list_encoder!(values, make_list_encoder)
                }
            }.map_err(|err| err.at("item"))
        },
        _ => _make_non_list_encoder!(array, _ok_box)
    }
}


pub fn make_struct_encoder(array: &StructArray) -> Result<StructEncoder, SchemaError> {
    let mut fields = Vec::with_capacity(array.columns().len());
    for (array, &name) in array.columns().iter().zip(array.column_names().iter()) {
        let encoder = make_encoder(array.as_ref()).map_err(|err| err.at(name))?;
        let field = StructField::new(name, encoder);
        fields.push(field)
    }
    Ok(StructEncoder::new(fields))
}


#[inline]
pub fn make_nullable_encoder<E: Encoder + 'static>(encoder: E, maybe_nulls: Option<NullBuffer>) -> EncoderObject {
    if let Some(nulls) = maybe_nulls {
        Box::new(NullableEncoder::new(encoder, nulls))
    } else {
        Box::new(encoder)
    }
}


pub fn extract_nulls(array: &dyn Array) -> Result<(Option<Box<dyn Array>>, Option<NullBuffer>), SchemaError> {
    if array.nulls().is_none() {
        return Ok((None, None))
    }

    macro_rules! ok {
        ($array:expr, $nulls:expr) => {
            Ok((Some(Box::new($array) as Box<dyn Array>), $nulls))
        };
    }

    macro_rules! primitive {
        ($t:ty) => {{
            let array = array.as_primitive::<$t>();
            let (_, values, nulls) = array.clone().into_parts();
            ok!(PrimitiveArray::<$t>::new(values, None), nulls)
        }};
    }

    match array.data_type() {
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Boolean => {
            let array = array.as_boolean();
            let (values, nulls) = array.clone().into_parts();
            ok!(BooleanArray::new(values, None), nulls)
        },
        DataType::FixedSizeBinary(_) => {
            let array = array.as_fixed_size_binary();
            let (size, values, nulls) = array.clone().into_parts();
            ok!(FixedSizeBinaryArray::new(size, values, None), nulls)
        },
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            let (offsets, values, nulls) = array.clone().into_parts();
            ok!(unsafe {
                BinaryArray::new_unchecked(offsets, values, None)
            }, nulls)
        },
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            let (offsets, values, nulls) = array.clone().into_parts();
            ok!(unsafe {
                GenericStringArray::<i32>::new_unchecked(offsets, values, None)
            }, nulls)
        },
        DataType::List(_) => {
            let array = array.as_list();
            let (field, offsets, values, nulls) = array.clone().into_parts();
            ok!(GenericListArray::<i32>::new(field, offsets, values, None), nulls)
        },
        DataType::Struct(_) => {
            let array = array.as_struct();
            let (fields, columns, nulls) = array.clone().into_parts();
            ok!(unsafe {
                StructArray::new_unchecked(fields, columns, None)
            }, nulls)
        },
        _ => Err(SchemaError::new(
            format!("unsupported arrow type - {}", array.data_type())
        ))
    }
}