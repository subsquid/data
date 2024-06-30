use std::io::{Read, Write};
use std::marker::PhantomData;
use std::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use serde_json::Value as JsonValue;


pub type Base58Bytes = String;


struct JsonStringParser<T> {
    phantom_data: PhantomData<T>
}


impl <T> JsonStringParser<T> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData::default()
        }
    }
}


impl <'de, T: FromStr> serde::de::Visitor<'de> for JsonStringParser<T> {
    type Value = T;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a string literal representing {}", std::any::type_name::<T>())
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        T::from_str(v).map_err(|_| {
            serde::de::Error::custom(
                format!("failed to deserialize `{}` as {}", v, std::any::type_name::<T>())
            )
        })
    }
}


pub struct StringEncoded<T>(pub T);


impl <'de, T: FromStr> serde::Deserialize<'de> for StringEncoded<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        deserializer.deserialize_str(JsonStringParser::<T>::new()).map(StringEncoded)
    }
}


impl <T: BorshSerialize> BorshSerialize for StringEncoded<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.0.serialize(writer)
    }
}


impl <T: BorshDeserialize> BorshDeserialize for StringEncoded<T> {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        T::deserialize(buf).map(StringEncoded)
    }

    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        T::deserialize_reader(reader).map(StringEncoded)
    }
}


impl<T: Clone> Clone for StringEncoded<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}


impl <T: Copy> Copy for StringEncoded<T> {}


#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct JSON(pub JsonValue);


impl JSON {
    pub fn to_string(&self) -> String {
        serde_json::to_string(&self.0).unwrap()
    }
}


impl BorshSerialize for JSON {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        serialize_json_value(&self.0, writer)
    }
}


impl BorshDeserialize for JSON {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        deserialize_json_value(reader).map(JSON)
    }
}


fn serialize_json_value<W: Write>(value: &JsonValue, writer: &mut W) -> std::io::Result<()> {
    match value {
        JsonValue::Null => {
            0u8.serialize(writer)
        },
        JsonValue::Bool(v) => {
            1u8.serialize(writer)?;
            v.serialize(writer)
        },
        JsonValue::Number(v) => {
            2u8.serialize(writer)?;
            if v.is_f64() {
                0u8.serialize(writer)?;
                v.as_f64().unwrap().serialize(writer)
            } else {
                let i = if v.is_u64() {
                    v.as_u64().unwrap() as i64
                } else {
                    v.as_i64().unwrap()
                };
                1u8.serialize(writer)?;
                i.serialize(writer)
            }
        },
        JsonValue::String(x) => {
            3u8.serialize(writer)?;
            x.serialize(writer)
        }
        JsonValue::Array(values) => {
            4u8.serialize(writer)?;
            values.len().serialize(writer)?;
            for v in values.iter() {
                serialize_json_value(v, writer)?;
            }
            Ok(())
        }
        JsonValue::Object(map) => {
            5u8.serialize(writer)?;
            map.len().serialize(writer)?;
            for (k, v) in map.iter() {
                k.serialize(writer)?;
                serialize_json_value(v, writer)?;
            }
            Ok(())
        }
    }
}


fn deserialize_json_value<R: Read>(reader: &mut R) -> std::io::Result<JsonValue> {
    use serde_json::*;

    macro_rules! bail {
        ($msg:expr) => {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, $msg))
        };
    }

    Ok(match u8::deserialize_reader(reader)? {
        0 => Value::Null,
        1 => Value::Bool(bool::deserialize_reader(reader)?),
        2 => match u8::deserialize_reader(reader)? {
            0 => {
                let float = f64::deserialize_reader(reader)?;
                if let Some(num) = Number::from_f64(float) {
                    Value::Number(num)
                } else {
                    bail!("NaN or infinite float is not a JSON number")
                }
            },
            1 => Value::Number(Number::from(i64::deserialize_reader(reader)?)),
            _ => bail!("unexpected number case")
        },
        3 => Value::String(String::deserialize_reader(reader)?),
        4 => {
            let len = usize::deserialize_reader(reader)?;
            let mut array = Vec::with_capacity(len);
            for _ in 0..len {
                let item = deserialize_json_value(reader)?;
                array.push(item)
            }
            Value::Array(array)
        },
        5 => {
            let len = usize::deserialize_reader(reader)?;
            let mut map = Map::with_capacity(len);
            for _ in 0..len {
                let key = String::deserialize_reader(reader)?;
                let value = deserialize_json_value(reader)?;
                map.insert(key, value);
            }
            Value::Object(map)
        },
        _ => bail!("unexpected JsonValue case")
    })
}