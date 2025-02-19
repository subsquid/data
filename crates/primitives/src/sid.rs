use std::{fmt::{Debug, Display, Formatter}, str::FromStr};


#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SID<const N: usize> {
    bytes: [u8; N]
}


impl <const N: usize> AsRef<[u8]> for SID<N> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}


impl <const N: usize> TryFrom<&[u8]> for SID<N> {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() > N {
            return Err("binary string is too long")
        }
        let mut bytes = [0; N];
        bytes[..value.len()].copy_from_slice(value);
        Self::try_new(bytes)
    }
}


impl <const N: usize> TryFrom<&str> for SID<N> {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() > N {
            return Err("string is too long")
        }

        if !value.as_bytes().iter().copied().all(Self::is_valid_byte) {
            return Err("only ascii alphanumeric, '-' and '_' characters are allowed in short id strings")
        }

        let mut bytes = [0; N];
        bytes[..value.len()].copy_from_slice(value.as_bytes());

        Ok(Self {
            bytes
        })
    }
}

impl <const N: usize> FromStr for SID<N> {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}


impl <const N: usize> Default for SID<N> {
    fn default() -> Self {
        Self {
            bytes: [0; N]
        }
    }
}


impl <const N: usize> SID<N> {
    pub fn from_str(s: &str) -> Self {
        s.try_into().unwrap()    
    }
    
    pub fn try_new(bytes: [u8; N]) -> Result<Self, &'static str> {
        let slice = if let Some(end) = bytes.iter().position(|b| *b == 0) {
            if !bytes[end..].iter().all(|b| *b == 0) {
                return Err("only trailing 0 bytes are allowed in SID")
            }
            &bytes[0..end]
        } else {
            &bytes
        };

        if !slice.iter().copied().all(Self::is_valid_byte) {
            return Err("only ascii alphanumeric, '-' and '_' characters are allowed in SID strings")
        }

        Ok(Self {
            bytes
        })
    }

    fn is_valid_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(
            if let Some(end) = self.bytes.iter().position(|b| *b == 0) {
                &self.bytes[0..end]
            } else {
                &self.bytes
            }
        ).unwrap()
    }
}


impl <'a, const N: usize> From<&'a SID<N>> for &'a str {
    fn from(val: &'a SID<N>) -> Self {
        val.as_str()
    }
}


impl <const N: usize> Display for SID<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}


impl <const N: usize> Debug for SID<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}


#[cfg(feature = "borsh")]
impl <const N: usize> borsh::BorshSerialize for SID<N> {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.bytes.serialize(writer)
    }
}


#[cfg(feature = "borsh")]
impl <const N: usize> borsh::BorshDeserialize for SID<N> {
    fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
        let bytes = <[u8; N]>::deserialize(buf)?;
        Self::try_new(bytes).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err
            )
        })
    }
    
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let bytes = <[u8; N]>::deserialize_reader(reader)?;
        Self::try_new(bytes).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err
            )
        })
    }
}


#[cfg(feature = "serde")]
impl <const N: usize> serde::ser::Serialize for SID<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer
    {
        serializer.serialize_str(self.as_str())
    }
}


#[cfg(feature = "serde")]
impl <'de, const N: usize> serde::de::Deserialize<'de> for SID<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>
    {
        deserializer.deserialize_str(serde_visitor::SIDVisitor)
    }
}


#[cfg(feature = "serde")]
mod serde_visitor {
    use super::SID;


    pub struct SIDVisitor<const N: usize>;


    impl <'de, const N: usize> serde::de::Visitor<'de> for SIDVisitor<N> {
        type Value = SID<N>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a string literal representing {}-byte short id string", N)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            SID::try_from(v).map_err(|msg| {
                serde::de::Error::custom(
                    format!("failed to deserialize `{}` as {}-byte short id string: {}", v, N, msg)
                )
            })
        }
    }
}