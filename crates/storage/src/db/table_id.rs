use std::fmt::{Display, Formatter};

use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct TableId {
    uuid: Uuid
}

impl AsRef<[u8]> for TableId {
    fn as_ref(&self) -> &[u8] {
        self.uuid.as_bytes()
    }
}

impl TableId {
    pub fn new() -> Self {
        Self { uuid: Uuid::now_v7() }
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        Self::try_from_slice(bytes).expect("TableId::from_slice: not a 16-byte UUID")
    }

    /// Decode a key into a `TableId`, or `None` if not exactly a 16-byte UUID. Use this
    /// (not [`TableId::from_slice`]) when scanning a CF whose keys could be corrupt, so a
    /// malformed key is skipped instead of panicking and wedging the scan.
    pub fn try_from_slice(bytes: &[u8]) -> Option<Self> {
        Uuid::from_slice(bytes).ok().map(|uuid| Self { uuid })
    }
}

impl Display for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.uuid.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_slice_rejects_non_16_byte_keys() {
        // A real 16-byte id round-trips.
        let id = TableId::new();
        assert_eq!(TableId::try_from_slice(id.as_ref()), Some(id));

        // Anything that is not exactly 16 bytes decodes to None instead of
        // panicking (so a corrupt/short key can be skipped, not wedge cleanup).
        assert_eq!(TableId::try_from_slice(&[]), None);
        assert_eq!(TableId::try_from_slice(&[0u8; 15]), None);
        assert_eq!(TableId::try_from_slice(&[0u8; 17]), None);
    }
}
