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
    /// UUIDv7 generation is monotonic within one process. Runtime reclaim uses an unused
    /// id as a fence: tables allocated after the fence sort above its unlink range.
    pub fn new() -> Self {
        Self { uuid: Uuid::now_v7() }
    }

    /// Decode a raw column-family key, or `None` if it is not exactly a 16-byte UUID, so a
    /// corrupt bookkeeping key is skipped instead of wedging the scan with a panic.
    ///
    /// Not named `try_from_slice`: that would shadow the `BorshDeserialize` method.
    pub fn try_from_key(bytes: &[u8]) -> Option<Self> {
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
    fn try_from_key_rejects_non_16_byte_keys() {
        let id = TableId::new();
        assert_eq!(TableId::try_from_key(id.as_ref()), Some(id));

        // Anything not exactly 16 bytes decodes to None instead of panicking.
        assert_eq!(TableId::try_from_key(&[]), None);
        assert_eq!(TableId::try_from_key(&[0u8; 15]), None);
        assert_eq!(TableId::try_from_key(&[0u8; 17]), None);
    }

    #[test]
    fn new_ids_are_ordered_for_reclaim_fences() {
        let mut previous = TableId::new();
        for _ in 0..1_000 {
            let next = TableId::new();
            assert!(previous < next);
            previous = next;
        }
    }
}
