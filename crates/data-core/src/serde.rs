use std::marker::PhantomData;
use std::str::FromStr;


struct StringParser<T> {
    phantom_data: PhantomData<T>
}


impl <T> StringParser<T> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData
        }
    }
}


impl <'de, T: FromStr> serde::de::Visitor<'de> for StringParser<T> {
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


struct StringOptionParser<T> {
    phantom_data: PhantomData<T>
}


impl <T> StringOptionParser<T> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData
        }
    }
}


impl <'de, T: FromStr> serde::de::Visitor<'de> for StringOptionParser<T> {
    type Value = Option<T>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "an optional string literal representing {}", std::any::type_name::<T>())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(StringParser::<T>::new()).map(Some)
    }
}


pub fn decode_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where D: serde::Deserializer<'de>,
      T: FromStr
{
    deserializer.deserialize_str(StringParser::<T>::new())
}


pub fn decode_string_option<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where D: serde::Deserializer<'de>,
      T: FromStr
{
    deserializer.deserialize_option(StringOptionParser::<T>::new())
}                                                                                                                                                       