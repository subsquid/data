use sqd_primitives::{Block, BlockNumber, BlockPtr};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Index;


pub struct Chain<B> {
    blocks: VecDeque<B>,
    droppable: Vec<bool>,
    min_size: usize,
}


impl <B: Block> Chain<B> {
    pub fn new() -> Self {
        Self {
            blocks: VecDeque::with_capacity(10),
            droppable: Vec::with_capacity(10),
            min_size: 1
        }
    }

    pub fn base(&self) -> Option<BlockPtr<'_>> {
        self.first().map(|b| b.parent_ptr())
    }
    
    pub fn first(&self) -> Option<&B> {
        self.blocks.get(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = &B> + DoubleEndedIterator  {
        self.blocks.iter()
    }
    
    pub fn bisect(&self, block_number: BlockNumber) -> usize {
        let Some(mut end) = self.blocks.len().checked_sub(1) else {
            return 0
        };

        let mut beg = match self.blocks[end].number().cmp(&block_number) {
            Ordering::Less => return end + 1,
            Ordering::Equal => return end,
            Ordering::Greater => {
                let diff = self.blocks[end].number() - block_number;
                end.saturating_sub(diff as usize)
            }
        };

        if self.blocks[beg].number() >= block_number {
            return beg
        }

        while end - beg > 1  {
            let mid = beg + (end - beg) / 2;
            match self.blocks[mid].number().cmp(&block_number) {
                Ordering::Less => beg = mid,
                Ordering::Equal => return mid,
                Ordering::Greater => end = mid
            }
        }

        end
    }
    
    pub fn push(&mut self, block: B) {
        todo!()
    }
    
    pub fn drop(&mut self, number: BlockNumber, hash: &str) -> bool {
        todo!()
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }
}


impl<B> Index<usize> for Chain<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.blocks.index(index)
    }
}