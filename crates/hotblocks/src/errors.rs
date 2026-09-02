use std::fmt::{Display, Formatter};

use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::DatasetId;

#[derive(Debug)]
pub struct Busy;

impl Display for Busy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "service is busy")
    }
}

impl std::error::Error for Busy {}

#[derive(Debug)]
pub struct UnsupportedQuery {
    pub query_kind: &'static str
}

impl Display for UnsupportedQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} queries are not supported", self.query_kind)
    }
}

impl std::error::Error for UnsupportedQuery {}

#[derive(Debug)]
pub struct QueryTaskPanicked;

impl Display for QueryTaskPanicked {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "query execution task panicked")
    }
}

impl std::error::Error for QueryTaskPanicked {}

/// A replay below the finalized head cannot publish a partial chunk merely to cut at a
/// data-availability boundary.
#[derive(Debug)]
pub struct DataAvailabilityChangedDuringFinalizedReplay {
    pub block_number: BlockNumber
}

impl Display for DataAvailabilityChangedDuringFinalizedReplay {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "data availability changed at block {} inside the replayed finalized range",
            self.block_number
        )
    }
}

impl std::error::Error for DataAvailabilityChangedDuringFinalizedReplay {}

/// Stable reason for refusing a fork or finality transition.
///
/// The snake-case representation is exported as the `cause` label of
/// `dataset_epoch_failures`; dynamic details belong in the error context.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UnapplicableForkReason {
    HintsBelowFinalizedHead,
    HintConflictsWithFinalizedHead,
    BelowRetainedWindow,
    StaleRollbackBoundary,
    DropsFinalizedBlock,
    RewritesFinalizedHistory,
    FinalityHashChanged,
    FinalityContradictsChunk,
    FinalityEvictsFinalizedBlock,
    FinalityContradictsStoredBlock
}

impl UnapplicableForkReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HintsBelowFinalizedHead => "hints_below_finalized_head",
            Self::HintConflictsWithFinalizedHead => "hint_conflicts_with_finalized_head",
            Self::BelowRetainedWindow => "below_retained_window",
            Self::StaleRollbackBoundary => "stale_rollback_boundary",
            Self::DropsFinalizedBlock => "drops_finalized_block",
            Self::RewritesFinalizedHistory => "rewrites_finalized_history",
            Self::FinalityHashChanged => "finality_hash_changed",
            Self::FinalityContradictsChunk => "finality_contradicts_chunk",
            Self::FinalityEvictsFinalizedBlock => "finality_evicts_finalized_block",
            Self::FinalityContradictsStoredBlock => "finality_contradicts_stored_block"
        }
    }
}

impl Display for UnapplicableForkReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A fork or finality transition that cannot be applied safely. Kills the update task, which then
/// parks for a minute.
#[derive(Debug)]
pub struct UnapplicableFork {
    pub reason: UnapplicableForkReason
}

impl Display for UnapplicableFork {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "received fork can not be applied: {}", self.reason)
    }
}

impl std::error::Error for UnapplicableFork {}

#[derive(Debug)]
pub struct UnknownDataset {
    pub dataset_id: DatasetId
}

impl Display for UnknownDataset {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "dataset {} does not exist", self.dataset_id)
    }
}

impl std::error::Error for UnknownDataset {}

#[derive(Debug)]
pub struct BlockRangeMissing {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber
}

impl Display for BlockRangeMissing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "blocks from {} to {} are not available in the dataset",
            self.first_block, self.last_block
        )
    }
}

impl std::error::Error for BlockRangeMissing {}

#[derive(Debug)]
pub struct QueryIsAboveTheHead {
    pub finalized_head: Option<BlockRef>
}

impl Display for QueryIsAboveTheHead {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "first block requested by the query is above the current dataset head"
        )
    }
}

impl std::error::Error for QueryIsAboveTheHead {}

#[derive(Debug)]
pub struct QueryKindMismatch {
    pub query_kind: sqd_storage::db::DatasetKind,
    pub dataset_kind: sqd_storage::db::DatasetKind
}

impl Display for QueryKindMismatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} query was issued against {} dataset",
            self.query_kind, self.dataset_kind
        )
    }
}

impl std::error::Error for QueryKindMismatch {}

#[derive(Debug)]
pub struct BlockItemIsNotAvailable {
    pub item_name: &'static str,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber
}

impl Display for BlockItemIsNotAvailable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "'{}' data is not available for blocks {}..{}",
            self.item_name, self.first_block, self.last_block
        )
    }
}

impl std::error::Error for BlockItemIsNotAvailable {}
