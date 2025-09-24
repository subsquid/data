use anyhow::bail;
use sqd_primitives::{AsBlockPtr, BlockNumber, BlockPtr, BlockRef};
use std::cmp::Ordering;
use std::ops::Index;


pub fn compute_fork_base<'a, 'b, B: AsBlockPtr + 'a>(
    reversed_chain: impl IntoIterator<Item = &'a B>, 
    mut prev: &'b [BlockRef]
) -> Option<BlockPtr<'a>>
{
    let prev = &mut prev;
    for b in reversed_chain {
        let b = b.as_block_ptr();

        if prev.last().map_or(false, |p| p.number < b.number) {
            continue
        }

        while prev.last().map_or(false, |p| p.number > b.number) {
            *prev = pop(prev);
        }
        
        if prev.is_empty() {
            return Some(b)
        }
        
        let p = prev.last().unwrap();
        if b.number == p.number && b.hash == p.hash {
            return Some(b)
        } else {
            *prev = pop(prev)
        }
    }

    None
}


fn pop<T>(slice: &[T]) -> &[T] {
    &slice[0..slice.len() - 1]
}


pub fn bisect<B: AsBlockPtr>(
    len: usize, 
    chain: &impl Index<usize, Output = B>, 
    block_number: BlockNumber
) -> usize 
{
    let Some(mut end) = len.checked_sub(1) else {
        return 0
    };

    let mut beg = match chain[end].as_block_ptr().number.cmp(&block_number) {
        Ordering::Less => return end + 1,
        Ordering::Equal => return end,
        Ordering::Greater => {
            let diff = chain[end].as_block_ptr().number - block_number;
            end.saturating_sub(diff as usize)
        }
    };

    if chain[beg].as_block_ptr().number >= block_number {
        return beg
    }

    while end - beg > 1  {
        let mid = beg + (end - beg) / 2;
        match chain[mid].as_block_ptr().number.cmp(&block_number) {
            Ordering::Less => beg = mid,
            Ordering::Equal => return mid,
            Ordering::Greater => end = mid
        }
    }

    end
}


pub fn task_termination_error(
    task_name: &str,
    res: Result<anyhow::Result<()>, tokio::task::JoinError>
) -> anyhow::Result<()>
{
    match res {
        Ok(Ok(_)) => bail!("{} task unexpectedly terminated", task_name),
        Ok(Err(err)) => Err(err.context(format!("{} task failed", task_name))),
        Err(join_error) => bail!("{} task terminated: {}", task_name, join_error)
    }
}