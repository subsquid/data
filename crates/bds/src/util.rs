use sqd_primitives::{Block, BlockRef};


pub fn compute_fork_base<'a, 'b, 'c, B: Block + 'a>(
    reversed_chain: impl IntoIterator<Item = &'a B>, 
    prev: &'b mut &'c [BlockRef]
) -> Option<BlockRef> 
{
    for b in reversed_chain {
        if prev.last().map_or(false, |p| p.number < b.number()) {
            continue
        }

        while prev.last().map_or(false, |p| p.number > b.number()) {
            *prev = pop(prev);
        }
        
        if prev.is_empty() {
            return Some(BlockRef {
                number: b.number(),
                hash: b.hash().to_string()
            })
        }
        
        let p = prev.last().unwrap();
        if b.number() == p.number && b.hash() == &p.hash {
            return Some(p.clone())
        } else {
            *prev = pop(prev)
        }
        
        if prev.is_empty() {
            return Some(BlockRef {
                number: b.parent_number(),
                hash: b.parent_hash().to_string()
            })
        }
    }

    None
}


fn pop<T>(slice: &[T]) -> &[T] {
    &slice[0..slice.len() - 1]
}