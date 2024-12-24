pub type Name = &'static str;
pub type BlockNumber = u64;
pub type ItemIndex = u32;


#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "borsh", derive(borsh::BorshSerialize, borsh::BorshDeserialize))]
pub struct BlockRef {
    pub number: BlockNumber,
    pub hash: String
}


pub trait Block {
    fn number(&self) -> BlockNumber;

    fn hash(&self) -> &str;

    fn parent_number(&self) -> BlockNumber;

    fn parent_hash(&self) -> &str;
}


#[cfg(feature = "from_json_bytes")]
pub trait FromJsonBytes: Sized {
    fn from_json_bytes(bytes: bytes::Bytes) -> anyhow::Result<Self>;
}


#[cfg(feature = "from_json_bytes")]
#[cfg(feature = "serde_json")]
impl <T: serde::de::DeserializeOwned> FromJsonBytes for T {
    #[inline]
    fn from_json_bytes(bytes: bytes::Bytes) -> anyhow::Result<Self> {
        serde_json::from_slice(&bytes).map_err(|e| e.into())
    }
} 
