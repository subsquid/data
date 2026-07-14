use anyhow::{bail, ensure};

/// Durable lifecycle of a table after its last chunk reference is deleted.
///
/// Empty values predate this state machine and remain `DeleteRequested`, so databases
/// written by older releases upgrade without a migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeletedTableState {
    DeleteRequested,
    PurgedUnstamped,
    Purged { sequence: u64 }
}

impl DeletedTableState {
    const PURGED_UNSTAMPED: u8 = 1;
    const PURGED: u8 = 2;

    pub(crate) fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        match bytes {
            [] => Ok(Self::DeleteRequested),
            [Self::PURGED_UNSTAMPED] => Ok(Self::PurgedUnstamped),
            [Self::PURGED, sequence @ ..] => {
                ensure!(
                    sequence.len() == size_of::<u64>(),
                    "invalid purged-table sequence length"
                );
                let sequence = u64::from_le_bytes(sequence.try_into().unwrap());
                Ok(Self::Purged { sequence })
            }
            [tag, ..] => bail!("unknown deleted-table state tag {tag}")
        }
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        match self {
            Self::DeleteRequested => Vec::new(),
            Self::PurgedUnstamped => vec![Self::PURGED_UNSTAMPED],
            Self::Purged { sequence } => {
                let mut value = Vec::with_capacity(1 + size_of::<u64>());
                value.push(Self::PURGED);
                value.extend_from_slice(&sequence.to_le_bytes());
                value
            }
        }
    }

    pub(crate) fn is_reclaim_safe(self, oldest_snapshot_sequence: Option<u64>) -> bool {
        match (self, oldest_snapshot_sequence) {
            (Self::Purged { .. }, None) => true,
            (Self::Purged { sequence }, Some(oldest)) => sequence <= oldest,
            (Self::DeleteRequested | Self::PurgedUnstamped, _) => false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_legacy_value_is_delete_requested() {
        assert_eq!(
            DeletedTableState::decode(&[]).unwrap(),
            DeletedTableState::DeleteRequested
        );
    }

    #[test]
    fn states_round_trip() {
        for state in [
            DeletedTableState::DeleteRequested,
            DeletedTableState::PurgedUnstamped,
            DeletedTableState::Purged { sequence: 42 }
        ] {
            assert_eq!(DeletedTableState::decode(&state.encode()).unwrap(), state);
        }
    }

    #[test]
    fn malformed_state_fails_closed() {
        assert!(DeletedTableState::decode(&[DeletedTableState::PURGED, 1]).is_err());
        assert!(DeletedTableState::decode(&[99]).is_err());
    }

    #[test]
    fn only_sequence_stamped_purges_can_be_reclaimed() {
        assert!(!DeletedTableState::DeleteRequested.is_reclaim_safe(None));
        assert!(!DeletedTableState::PurgedUnstamped.is_reclaim_safe(None));

        let purged = DeletedTableState::Purged { sequence: 42 };
        assert!(purged.is_reclaim_safe(None));
        assert!(!purged.is_reclaim_safe(Some(41)));
        assert!(purged.is_reclaim_safe(Some(42)));
        assert!(purged.is_reclaim_safe(Some(43)));
    }
}
