use std::ops::RangeBounds;
use sqd_primitives::BlockNumber;
use std::collections::Bound;
use crate::types::BlockRange;


pub fn to_range(bounds: impl RangeBounds<BlockNumber>) -> BlockRange {
    let start = match bounds.start_bound() {
        Bound::Included(n) => *n,
        Bound::Excluded(n) => n.saturating_add(1),
        Bound::Unbounded => 0
    };
    
    let end = match bounds.end_bound() {
        Bound::Included(n) => n.saturating_add(1),
        Bound::Excluded(n) => *n,
        Bound::Unbounded => BlockNumber::MAX
    };

    start..end
}