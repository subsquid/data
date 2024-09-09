use std::ops::Deref;

use arrow::array::{Array, AsArray, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, TimestampMillisecondType, TimestampSecondType, TimeUnit};

use crate::json::encoder::{Encoder, EncoderObject, JsonEncoder, ListSpreadEncoder, NullableEncoder, PrimitiveEncoder, SafeStringEncoder, StructEncoder, StructField, TimestampEncoder};
use crate::json::encoder::factory::{extract_nulls, make_encoder, make_nullable_encoder};
use crate::json::encoder::util::json_close;
use crate::primitives::{Name, schema_error, SchemaError};


#[derive(Debug, Clone)]
pub enum Exp {
    Value,
    Json,
    BigNum,
    TimestampSecond,
    TimestampMillisecond,
    Object(Vec<(Name, Exp)>),
    Prop(Name, Box<Exp>),
    Roll {
        columns: Vec<Name>,
        exp: Box<Exp>
    },
    Enum {
        tag_column: Name,
        variants: Vec<(Name, Exp)>
    }
}


impl Exp {
    pub fn for_each_column<F>(&self, f: &mut F) where F: FnMut(Name) {
        match self {
            Exp::Object(props) => {
                props.iter().for_each(|(_name, exp)| {
                    exp.for_each_column(f);
                })
            },
            Exp::Prop(name, _) => {
                f(*name)
            },
            Exp::Roll { columns, .. } => {
                columns.iter().for_each(|name| f(*name))
            },
            Exp::Enum { tag_column, variants } => {
                f(*tag_column);
                variants.iter().for_each(|(_name, exp)| {
                    exp.for_each_column(f);
                })
            },
            Exp::Value |
            Exp::Json |
            Exp::BigNum |
            Exp::TimestampSecond |
            Exp::TimestampMillisecond => {},
        }
    }

    pub fn eval(&self, array: &dyn Array) -> Result<EncoderObject, SchemaError> {
        match self {
            Exp::Value => make_encoder(array),
            Exp::Json => eval_json(array),
            Exp::BigNum => eval_bignum(array),
            Exp::TimestampSecond => eval_timestamp(array, TimeUnit::Second),
            Exp::TimestampMillisecond => eval_timestamp(array, TimeUnit::Millisecond),
            Exp::Object(props) => eval_object(array, props),
            Exp::Prop(name, exp) => eval_prop(array, name, exp),
            Exp::Roll { columns, exp } => eval_roll(array, columns, exp),
            Exp::Enum { tag_column, variants } => eval_enum(array, tag_column, variants)
        }
    }
}


macro_rules! extract_nulls {
    ($array:expr, $result_array:ident, $nulls:ident) => {
        let array = $array;
        let (maybe_array, $nulls) = extract_nulls(array)?;
        let $result_array = maybe_array.as_ref().map(|b| b.deref()).unwrap_or(array);
    };
}


fn eval_json(array: &dyn Array) -> Result<EncoderObject, SchemaError> {
    let (offsets, buffer, nulls) = match array.data_type() {
        DataType::Binary => {
            array.as_binary().clone().into_parts()
        },
        DataType::Utf8 => {
            array.as_string().clone().into_parts()
        },
        ty => return Err(schema_error!(
            "Expected a raw JSON column, but got - {}", ty
        ))
    };
    let encoder = JsonEncoder::new(buffer, offsets);
    Ok(make_nullable_encoder(encoder, nulls))
}


fn eval_bignum(array: &dyn Array) -> Result<EncoderObject, SchemaError> {
    use arrow::datatypes::*;

    macro_rules! make {
        ($ty:ty) => {{
            let array = array.as_primitive::<$ty>();
            let (_, buffer, nulls) = array.clone().into_parts();
            let encoder = PrimitiveEncoder::new(buffer);
            Ok(make_nullable_encoder(
                SafeStringEncoder::new(encoder),
                nulls
            ))
        }};
    }

    match array.data_type() {
        DataType::Int8 => make!(Int8Type),
        DataType::Int16 => make!(Int16Type),
        DataType::Int32 => make!(Int32Type),
        DataType::Int64 => make!(Int64Type),
        DataType::UInt8 => make!(UInt8Type),
        DataType::UInt16 => make!(UInt16Type),
        DataType::UInt32 => make!(UInt32Type),
        DataType::UInt64 => make!(UInt64Type),
        DataType::Float32 => make!(Float32Type),
        DataType::Float64 => make!(Float64Type),
        DataType::Decimal128(_, 0) => make!(Decimal128Type),
        ty => Err(schema_error!(
            "Expected numeric primitive value, but got - {}", ty
        ))
    }
}


fn eval_timestamp(array: &dyn Array, target_unit: TimeUnit) -> Result<EncoderObject, SchemaError> {
    let (unit, buffer, nulls) = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = array.as_primitive::<TimestampSecondType>();
            let (_, buffer, nulls) = array.clone().into_parts();
            (TimeUnit::Second, buffer, nulls)
        },
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = array.as_primitive::<TimestampMillisecondType>();
            let (_, buffer, nulls) = array.clone().into_parts();
            (TimeUnit::Millisecond, buffer, nulls)
        },
        ty => return Err(
            schema_error!("expected Timestamp measured in seconds or milliseconds, but got {}", ty)
        )
    };
    let (mul, div) = match (unit, target_unit) {
        (TimeUnit::Second, TimeUnit::Millisecond) => (1000, 1),
        (TimeUnit::Millisecond, TimeUnit::Second) => (1, 1000),
        (actual, target) if actual == target => (1, 1),
        units => panic!("unexpected combination of actual and target time units - {:?}", units)
    };
    let encoder = TimestampEncoder::new(buffer, mul, div);
    Ok(make_nullable_encoder(encoder, nulls))
}


fn eval_object(array: &dyn Array, props: &Vec<(Name, Exp)>) -> Result<EncoderObject, SchemaError> {
    extract_nulls!(array, array, nulls);
    let mut fields = Vec::with_capacity(props.len());
    for (name, exp) in props.iter() {
        let encoder = exp.eval(array)?;
        let field = StructField::new(*name, encoder);
        fields.push(field)
    }
    let struct_encoder = StructEncoder::new(fields);
    Ok(make_nullable_encoder(struct_encoder, nulls))
}


fn eval_prop(array: &dyn Array, name: Name, exp: &Exp) -> Result<EncoderObject, SchemaError> {
    let array: &StructArray = array.as_any().downcast_ref().ok_or_else(|| {
        schema_error!("expected a StructArray, but got {}", array.data_type())
    })?;

    let column_array = array.column_by_name(name).ok_or_else(|| {
        schema_error!("column `{}` not found", name)
    })?;

    let encoder = exp.eval(column_array).map_err(|err| err.at(name))?;

    if let Some(nulls) = array.nulls() {
        Ok(Box::new(NullableEncoder::new(encoder, nulls.clone())))
    } else {
        Ok(encoder)
    }
}


fn eval_roll(array: &dyn Array, columns: &Vec<Name>, exp: &Exp) -> Result<EncoderObject, SchemaError> {
    extract_nulls!(array, array, array_nulls);

    let struct_array: &StructArray = array.as_any().downcast_ref().ok_or_else(|| {
        schema_error!("expected a StructArray, but got {}", array.data_type())
    })?;

    let mut non_nullable = Vec::with_capacity(columns.len());
    let mut nullable = Vec::with_capacity(columns.len());

    for (idx, name) in columns.iter().cloned().enumerate() {
        let item_array = struct_array.column_by_name(name).ok_or_else(|| {
            schema_error!("column `{}` is not found", name)
        })?;

        extract_nulls!(item_array, item_array, item_nulls);

        if item_nulls.is_none() && nullable.len() > 0 {
            return Err(
                schema_error!(
                    "Failed to construct list roll: column `{}` is nullable, while the next in the roll `{}` is not",
                    columns[idx-1],
                    name
                )
            )
        }

        let encoder = if let Some(list) = item_array.as_list_opt() {
            if list.null_count() > 0 {
                return Err(
                    SchemaError::new("list item of a roll is not supposed to be nullable")
                        .at("item")
                        .at(name)
                )
            }
            let encoder = exp.eval(list.values()).map_err(|err| err.at("item").at(name))?;
            Box::new(ListSpreadEncoder::new(encoder, list.offsets().clone()))
        } else {
            exp.eval(item_array).map_err(|err| err.at(name))?
        };

        if let Some(nulls) = item_nulls {
            nullable.push((nulls, encoder))
        } else {
            non_nullable.push(encoder)
        }
    }

    let roll_encoder = ListRollEncoder {
        non_nullable,
        nullable
    };

    Ok(make_nullable_encoder(roll_encoder, array_nulls))
}


struct ListRollEncoder {
    non_nullable: Vec<EncoderObject>,
    nullable: Vec<(NullBuffer, EncoderObject)>
}


impl Encoder for ListRollEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'[');
        for item in self.non_nullable.iter_mut() {
            item.encode(idx, out);
            out.push(b',')
        }
        for (nulls, item) in self.nullable.iter_mut() {
            if nulls.is_null(idx) {
                break
            }
            item.encode(idx, out);
            out.push(b',')
        }
        json_close(b']', out)
    }
}


fn eval_enum(
    array: &dyn Array,
    tag_column: Name,
    variants: &Vec<(Name, Exp)>
) -> Result<EncoderObject, SchemaError>
{
    extract_nulls!(array, array, array_nulls);

    let struct_array: &StructArray = array.as_struct_opt().ok_or_else(|| {
        schema_error!("expected a StructArray, but got {}", array.data_type())
    })?;

    let tag_array = struct_array.column_by_name(tag_column).ok_or_else(|| {
        schema_error!("column `{}` not found", tag_column)
    })?;

    extract_nulls!(tag_array, tag_array, tag_nulls);

    let tag_array = tag_array.as_string_opt().ok_or_else(|| {
        schema_error!("expected a StringArray, but got {}", tag_array.data_type()).at(tag_column)
    })?;

    let variants = variants.iter().map(|(name, exp)| {
        exp.eval(array).map(|encoder| {
            (*name, encoder)
        })
    }).collect::<Result<Vec<_>, _>>()?;

    let encoder = EnumEncoder {
        tag: tag_array.clone(),
        variants
    };

    Ok(if let Some(tag_nulls) = tag_nulls {
        make_nullable_encoder(NullableEncoder::new(encoder, tag_nulls), array_nulls)
    } else {
        make_nullable_encoder(encoder, array_nulls)
    })
}


struct EnumEncoder {
    tag: StringArray,
    variants: Vec<(Name, EncoderObject)>
}


impl Encoder for EnumEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let tag = self.tag.value(idx);
        for (v, e) in self.variants.iter_mut() {
            if *v == tag {
                e.encode(idx, out);
                return
            }
        }
        out.extend_from_slice(b"null")
    }
}