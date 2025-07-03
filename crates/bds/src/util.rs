use crate::block::BlockRange;
use sqd_primitives::{Block, BlockNumber, BlockRef};
use std::collections::Bound;
use std::ops::RangeBounds;


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


pub fn compute_fork_base<B: Block>(chain: &[B], prev: &mut &[BlockRef]) -> Option<usize> {
    for i in (0..chain.len()).rev() {
        let b = &chain[i];

        if prev.last().map_or(false, |p| p.number < b.number()) {
            continue
        }

        while prev.last().map_or(false, |p| p.number > b.number()) {
            *prev = pop(prev);
        }
        
        if prev.is_empty() {
            return Some(i)
        }
        
        let p = prev.last().unwrap();
        if b.number() == p.number && b.hash() == &p.hash {
            return Some(i)
        } else {
            *prev = pop(prev)
        }
    }
 
    None
}


fn pop<T>(slice: &[T]) -> &[T] {
    &slice[0..slice.len() - 1]
}