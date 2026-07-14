//! Rules shared by [`crate::db::Database::reclaim_disk_space`] and the out-of-process
//! `reclaim-measure` probe, so the number the probe prints matches what the flag frees.
//! (They cannot share a scan: one runs on a secondary `rocksdb::DB`, the other on an
//! `OptimisticTransactionDB`, and `rocksdb::DBInner` is not exported.)

use crate::db::{deleted_table::DeletedTableState, table_id::TableId};

/// Diagnostic classification of a `CF_DELETED_TABLES` value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeletedTableRecordKind {
    /// Point tombstones have not been written yet.
    DeleteRequested,
    /// Point tombstones were written, but their conservative sequence was not persisted.
    PurgedUnstamped,
    /// Point tombstones and their conservative sequence were persisted.
    Purged,
    /// The value cannot be decoded as any supported deletion state.
    Malformed
}

/// Classify deletion bookkeeping without exposing its internal sequence-state encoding.
pub fn deleted_table_record_kind(value: &[u8]) -> DeletedTableRecordKind {
    match DeletedTableState::decode(value) {
        Ok(DeletedTableState::DeleteRequested) => DeletedTableRecordKind::DeleteRequested,
        Ok(DeletedTableState::PurgedUnstamped) => DeletedTableRecordKind::PurgedUnstamped,
        Ok(DeletedTableState::Purged { .. }) => DeletedTableRecordKind::Purged,
        Err(_) => DeletedTableRecordKind::Malformed
    }
}

/// What a snapshot-aware runtime reclaim observed and retired.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeReclaimResult {
    /// RocksDB snapshots that existed when the safety horizon was sampled.
    pub snapshot_count: u64,
    /// Sequence of the oldest live snapshot, or `None` when there were no snapshots.
    pub oldest_snapshot_sequence: Option<u64>,
    /// Purged table records old enough for every live snapshot and removed this pass.
    pub safe_deleted_tables: usize,
    /// Deleted tables still awaiting point deletes/stamping or still needed by an old snapshot.
    pub unsafe_deleted_tables: usize,
    /// Undecodable committed chunks ignored by runtime reclaim. Their table data was already
    /// inaccessible through the query API; startup reclaim remains fail-closed on them.
    pub skipped_malformed_chunks: usize,
    /// Smallest table id that the whole-file unlink was not allowed to cross.
    pub watermark: Option<TableId>
}

/// Lower bound for `DeleteFilesInRange`. Keys are `table_id (16B) ++ tag ++ ..`.
pub const RECLAIM_LOWER_BOUND: [u8; 16] = [0u8; 16];

/// Upper bound for `DeleteFilesInRange`.
///
/// A bare 16-byte id is exclusive in practice even though the C binding passes
/// `include_end = true` (keeping files with `largest_user_key <= end`): `TableKeyFactory`
/// always appends a tag byte, so no key is ever exactly 16 bytes. With nothing live,
/// 17 x `0xFF` sorts above every key.
pub fn reclaim_upper_bound(watermark: Option<TableId>) -> Vec<u8> {
    match watermark {
        Some(id) => id.as_ref().to_vec(),
        None => vec![0xFFu8; 17]
    }
}

/// Smallest table id still live: referenced by a committed chunk, or pending in
/// `CF_DIRTY_TABLES`. Taken across all datasets, which absorbs UUIDv7 clock skew.
pub fn watermark(live: impl IntoIterator<Item = TableId>, dirty: impl IntoIterator<Item = TableId>) -> Option<TableId> {
    live.into_iter().chain(dirty).min()
}

/// Whether `DeleteFilesInRange(.., RECLAIM_LOWER_BOUND, upper_bound)` unlinks this SST.
///
/// Mirrors `DBImpl::DeleteFilesInRanges`: only files lying entirely inside the range are
/// dropped, and its loop starts at level 1, so L0 is skipped outright. RocksDB also skips
/// files it is compacting, invisible from here -- this is an upper bound.
pub fn sst_is_unlinkable(level: i32, end_key: Option<&[u8]>, upper_bound: &[u8]) -> bool {
    level > 0 && end_key.is_some_and(|end| end <= upper_bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> TableId {
        TableId::try_from_key(&[byte; 16]).unwrap()
    }

    #[test]
    fn upper_bound_excludes_the_watermark_tables_own_keys() {
        let wm = id(7);
        let bound = reclaim_upper_bound(Some(wm));

        // The watermark table's own keys sort above the bound, so its files survive.
        let mut smallest_key = wm.as_ref().to_vec();
        smallest_key.push(0);
        assert!(smallest_key.as_slice() > bound.as_slice());
        assert!(RECLAIM_LOWER_BOUND.as_slice() < bound.as_slice());
    }

    #[test]
    fn no_live_table_means_every_key_is_below_the_bound() {
        let bound = reclaim_upper_bound(None);
        let mut largest_key = [0xFFu8; 16].to_vec();
        largest_key.push(0xFF); // above any real tag byte
        assert!(largest_key.as_slice() <= bound.as_slice());
    }

    #[test]
    fn watermark_is_the_min_over_live_and_dirty() {
        let (a, b) = (id(1), id(2));
        assert_eq!(watermark([b], [a]), Some(a));
        assert_eq!(watermark([a], [b]), Some(a));
        assert_eq!(watermark(None, Some(b)), Some(b));
        assert_eq!(watermark(None::<TableId>, None), None);
    }

    #[test]
    fn level_zero_files_are_never_unlinkable() {
        let bound = reclaim_upper_bound(None);
        assert!(!sst_is_unlinkable(0, Some(b"anything"), &bound));
        assert!(sst_is_unlinkable(1, Some(b"anything"), &bound));
        assert!(!sst_is_unlinkable(1, None, &bound));
    }

    #[test]
    fn files_reaching_above_the_bound_are_kept() {
        let wm = id(7);
        let bound = reclaim_upper_bound(Some(wm));

        let mut reaches_into_wm = wm.as_ref().to_vec();
        reaches_into_wm.push(0);
        assert!(!sst_is_unlinkable(3, Some(&reaches_into_wm), &bound));

        assert!(sst_is_unlinkable(3, Some(&RECLAIM_LOWER_BOUND), &bound));
    }

    #[test]
    fn deleted_table_records_are_classified_for_diagnostics() {
        assert_eq!(deleted_table_record_kind(&[]), DeletedTableRecordKind::DeleteRequested);
        assert_eq!(deleted_table_record_kind(&[1]), DeletedTableRecordKind::PurgedUnstamped);
        assert_eq!(
            deleted_table_record_kind(&[2, 0, 0, 0, 0, 0, 0, 0, 0]),
            DeletedTableRecordKind::Purged
        );
        assert_eq!(deleted_table_record_kind(&[99]), DeletedTableRecordKind::Malformed);
    }
}
