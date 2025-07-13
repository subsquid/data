use sqd_primitives::{AsBlockPtr, BlockRef};


pub fn compute_fork_base<B: AsBlockPtr>(
    reversed_chain: impl IntoIterator<Item = B>, 
    prev: &mut &[BlockRef]
) -> Option<BlockRef>
{
    for b in reversed_chain {
        let b = b.as_block_ptr();

        if prev.last().map_or(false, |p| p.number < b.number) {
            continue
        }

        while prev.last().map_or(false, |p| p.number > b.number) {
            *prev = pop(prev);
        }
        
        if prev.is_empty() {
            return Some(b.to_ref())
        }
        
        let p = prev.last().unwrap();
        if b.number == p.number && b.hash == p.hash {
            return Some(b.to_ref())
        } else {
            *prev = pop(prev)
        }
    }

    None
}


fn pop<T>(slice: &[T]) -> &[T] {
    &slice[0..slice.len() - 1]
}