use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;


pub type Name = &'static str;
pub type BlockNumber = u64;
pub type ItemIndex = u32;


#[cfg_attr(feature = "borsh", derive(borsh::BorshSerialize, borsh::BorshDeserialize))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "valuable", derive(valuable::Valuable))]
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockRef {
    pub number: BlockNumber,
    pub hash: String
}


impl BlockRef {
    pub fn set_hash(&mut self, hash: &str) {
        self.hash.clear();
        self.hash.push_str(hash)
    }

    pub fn ptr(&self) -> BlockPtr<'_> {
        BlockPtr {
            number: self.number,
            hash: &self.hash
        }
    }
}


#[cfg_attr(feature = "valuable", derive(valuable::Valuable))]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BlockPtr<'a> {
    pub number: BlockNumber,
    pub hash: &'a str
}


impl<'a> BlockPtr<'a> {
    pub fn to_ref(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash.to_string()
        }
    }
}


impl Display for BlockRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.number, self.hash)
    }
}


impl<'a> Display for BlockPtr<'a> {
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

    fn timestamp(&self) -> Option<i64> {
        None
    }

    #[inline]
    fn ptr(&self) -> BlockPtr<'_> {
        BlockPtr {
            number: self.number(),
            hash: self.hash()
        }
    }

    #[inline]
    fn parent_ptr(&self) -> BlockPtr<'_> {
        BlockPtr {
            number: self.parent_number(),
            hash: self.parent_hash()
        }
    }
}


impl<'a, T: Block> Block for &'a T {
    #[inline]
    fn number(&self) -> BlockNumber {
        (*self).number()
    }

    #[inline]
    fn hash(&self) -> &str {
        (*self).hash()
    }

    #[inline]
    fn parent_number(&self) -> BlockNumber {
        (*self).parent_number()
    }

    #[inline]
    fn parent_hash(&self) -> &str {
        (*self).parent_hash()
    }

    #[inline]
    fn timestamp(&self) -> Option<i64> {
        (*self).timestamp()
    }

    #[inline]
    fn ptr(&self) -> BlockPtr<'_> {
        (*self).ptr()
    }

    #[inline]
    fn parent_ptr(&self) -> BlockPtr<'_> {
        (*self).parent_ptr()
    }
} 


impl<T: Block> Block for Arc<T> {
    #[inline]
    fn number(&self) -> BlockNumber {
        self.as_ref().number()
    }

    #[inline]
    fn hash(&self) -> &str {
        self.as_ref().hash()
    }

    #[inline]
    fn parent_number(&self) -> BlockNumber {
        self.as_ref().parent_number()
    }

    #[inline]
    fn parent_hash(&self) -> &str {
        self.as_ref().parent_hash()
    }

    #[inline]
    fn timestamp(&self) -> Option<i64> {
        self.as_ref().timestamp()
    }

    #[inline]
    fn ptr(&self) -> BlockPtr<'_> {
        self.as_ref().ptr()
    }

    #[inline]
    fn parent_ptr(&self) -> BlockPtr<'_> {
        self.as_ref().parent_ptr()
    }
}


pub trait AsBlockPtr {
    fn as_block_ptr(&self) -> BlockPtr<'_>;
}


impl AsBlockPtr for BlockRef {
    #[inline]
    fn as_block_ptr(&self) -> BlockPtr<'_> {
        self.ptr()
    }
}


impl<B: Block> AsBlockPtr for B {
    #[inline]
    fn as_block_ptr(&self) -> BlockPtr<'_> {
        self.ptr()
    }
}


impl<'a> AsBlockPtr for BlockPtr<'a> {
    #[inline]
    fn as_block_ptr(&self) -> BlockPtr<'_> {
        *self
    }
}