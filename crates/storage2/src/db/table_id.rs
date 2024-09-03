use std::fmt::{Display, Formatter};

use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;


#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct TableId {
    uuid: Uuid
}


impl AsRef<[u8]> for TableId {
    fn as_ref(&self) -> &[u8]{
        self.uuid.as_bytes()
    }
}


impl TableId {
    pub fn new() -> Self {
        Self {
            uuid: Uuid::now_v7()
        }
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        Self {
            uuid: Uuid::from_slice(bytes).unwrap()
        }
    }
}


impl Display for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.uuid.fmt(f)
    }
}