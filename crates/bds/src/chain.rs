use sqd_primitives::{Block, BlockNumber, BlockPtr};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Index;


pub struct Chain<B> {
    blocks: VecDeque<B>,
    droppable: VecDeque<bool>,
    min_size: usize,
}


impl <B: Block> Chain<B> {
    pub fn new(min_size: usize) -> Self {
        Self {
            blocks: VecDeque::with_capacity(min_size + 10),
            droppable: VecDeque::with_capacity(min_size + 10),
            min_size
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
    
    pub fn is_droppable(&self, idx: usize) -> bool {
        self.droppable[idx]
    }

    pub fn push(&mut self, block: B) {
        self.push_impl(block);
        self.clean();
    }
    
    fn push_impl(&mut self, block: B) {
        loop {
            let Some(prev) = self.blocks.back() else {
                self.blocks.push_back(block);
                self.droppable.push_back(false);
                return;
            };
            if prev.number() == block.parent_number() && prev.hash() == block.parent_hash() {
                self.blocks.push_back(block);
                self.droppable.push_back(false);
                return;
            } else {
                self.blocks.pop_back();
                self.droppable.pop_back();
            }
        }
    }
    
    pub fn drop(&mut self, number: BlockNumber, hash: &str) -> bool {
        let pos = self.bisect(number);
        
        let Some(block) = self.blocks.get(pos) else {
            return false  
        };
        
        if block.number() != number || block.hash() != hash {
            return false
        }
        
        self.droppable[pos] = true;
        true
    }
    
    pub fn clean(&mut self) -> bool {
        let mut dropped = false;
        for _ in 0..self.len().saturating_sub(self.min_size) {
            if self.droppable[0] {
                self.droppable.pop_front();
                self.blocks.pop_front();
                dropped = true;
            } else {
                break
            }
        }
        dropped
    }

    pub fn droppable_head(&self) -> Option<BlockPtr<'_>> {
        match self.droppable.iter().position(|&is_droppable| !is_droppable) {
            None => self.blocks.back().map(|b| b.ptr()),
            Some(i) => i.checked_sub(1).map(|i| self.blocks[i].ptr())
        }
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}


impl<B> Index<usize> for Chain<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.blocks.index(index)
    }
}