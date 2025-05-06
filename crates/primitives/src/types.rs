use std::fmt::{Debug, Display, Formatter};
use std::time::SystemTime;


pub type Name = &'static str;
pub type BlockNumber = u64;
pub type ItemIndex = u32;


#[cfg_attr(feature = "borsh", derive(borsh::BorshSerialize, borsh::BorshDeserialize))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "valuable", derive(valuable::Valuable))]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlockRef {
    pub number: BlockNumber,
    pub hash: String
}


impl Display for BlockRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.number, self.hash)
    }
}


pub struct DisplayBlockRefOption<'a>(pub Option<&'a BlockRef>);


impl<'a> Display for DisplayBlockRefOption<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(r) = self.0 {
            write!(f, "{}", r)
        } else {
            write!(f, "None")
        }
    }
}


pub trait Block {
    fn number(&self) -> BlockNumber;

    fn hash(&self) -> &str;

    fn parent_number(&self) -> BlockNumber;

    fn parent_hash(&self) -> &str;

    fn timestamp(&self) -> Option<SystemTime> {
        None
    }
}