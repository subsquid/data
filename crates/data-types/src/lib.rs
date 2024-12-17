pub type BlockNumber = u64;


pub trait Block {
    fn number(&self) -> BlockNumber;

    fn hash(&self) -> &str;

    fn parent_number(&self) -> BlockNumber;

    fn parent_hash(&self) -> &str;
}


#[cfg(feature = "de")]
pub trait FromJsonBytes: Sized {
    fn from_json_bytes(bytes: bytes::Bytes) -> anyhow::Result<Self>;
}


#[cfg(feature = "de")]
impl <T: serde::de::DeserializeOwned> FromJsonBytes for T {
    #[inline]
    fn from_json_bytes(bytes: bytes::Bytes) -> anyhow::Result<Self> {
        serde_json::from_slice(&bytes).map_err(|e| e.into())
    }
} 