use crate::util::{bisect, compute_fork_base};
use anyhow::{anyhow, bail, ensure};
use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};
use std::collections::VecDeque;
use std::ops::Index;


/// Last blocks of a chain, starting from the highest finalized block
#[derive(Debug)]
pub struct HeadChain {
    pub blocks: Vec<BlockRef>,
    /// indicates, that the first block in `self.blocks` is final 
    pub first_finalized: bool
}


impl HeadChain {
    pub fn empty() -> Self {
        Self {
            blocks: Vec::new(),
            first_finalized: false
        }
    }
}


pub struct Chain<B> {
    chain: VecDeque<BlockRef>,
    fin: bool,
    blocks: VecDeque<B>,
    stored: VecDeque<bool>,
    min_size: usize,
}


impl <B: Block> Chain<B> {
    pub fn new(chain: HeadChain, min_size: usize) -> Self {
        assert!(!(chain.first_finalized && chain.blocks.is_empty()));
        Self {
            chain: chain.blocks.into(),
            fin: chain.first_finalized,
            blocks: VecDeque::with_capacity(min_size + 10),
            stored: VecDeque::with_capacity(min_size + 10),
            min_size
        }
    }

    pub fn base(&self) -> Option<BlockPtr> {
        self.first().map(|b| b.parent_ptr())
    }
    
    pub fn first(&self) -> Option<&B> {
        self.blocks.get(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = &B> + DoubleEndedIterator  {
        self.blocks.iter()
    }
    
    pub fn bisect(&self, block_number: BlockNumber) -> usize {
        bisect(self.blocks.len(), &self.blocks, block_number)
    }
    
    pub fn is_stored(&self, idx: usize) -> bool {
        self.stored[idx]
    }

    pub fn push(&mut self, block: B) -> anyhow::Result<()> {
        loop {
            let Some(prev) = self.chain.back() else {
                self.chain.push_back(block.ptr().to_ref());
                self.blocks.push_back(block);
                self.stored.push_back(false);
                return Ok(());
            };
            
            if prev.number == block.parent_number() && prev.hash == block.parent_hash() {
                self.chain.push_back(block.ptr().to_ref());
                self.blocks.push_back(block);
                self.stored.push_back(false);
                return Ok(());
            }

            if block.parent_number() > prev.number {
                bail!(
                    "block {} is missing between {} and {}",
                    block.parent_ptr(),
                    prev,
                    block.ptr()
                );
            }

            if self.fin {
                ensure!(
                    self.chain.len() > 1,
                    "block {} is not based on finalized head {}",
                    block.ptr(),
                    prev
                );
            } else {
                assert_eq!(self.chain.len(), self.blocks.len());
            }

            self.chain.pop_back();
            self.blocks.pop_back();
            self.stored.pop_back();
        }
    }
    
    pub fn mark_stored(&mut self, number: BlockNumber, hash: &str) -> bool {
        let pos = self.bisect(number);
        
        let Some(block) = self.blocks.get(pos) else {
            return false  
        };
        
        if block.number() != number || block.hash() != hash {
            return false
        }
        
        self.stored[pos] = true;
        true
    }
    
    pub fn clean(&mut self) -> bool {
        let mut dropped = false;
        for _ in 0..self.len().saturating_sub(self.min_size) {
            if self.stored[0] {
                self.stored.pop_front();
                self.blocks.pop_front();
                if !self.fin {
                    self.chain.pop_front();
                }
                dropped = true;
            } else {
                break
            }
        }
        dropped
    }

    pub fn finalize(&mut self, head: BlockPtr) -> anyhow::Result<bool> {
        if self.chain.front().map_or(true, |b| head.number < b.number) {
            return Ok(false);
        }

        if self.chain.back().unwrap().number < head.number {
            return if self.chain.len() > 1 {
                self.finalize_all();
                Ok(true)
            } else {
                Ok(false)
            }
        }

        let pos = bisect(self.chain.len(), &self.chain, head.number);
        if self.chain[pos].ptr() == head {
            let fin = self.fin;
            self.fin = true;
            for _ in 0..pos {
                self.chain.pop_front();
            }
            return Ok(!fin || pos > 0)
        }

        bail!(
            "block {} is not part of a chain, its position is occupied by {}",
            head,
            self.chain[pos]
        );
    }

    pub fn finalize_all(&mut self) {
        match self.chain.len() {
            0 => {},
            1 => {
                self.fin = true;
            },
            _ => {
                let head = self.chain.pop_back().unwrap();
                self.chain.clear();
                self.chain.push_back(head);
                self.fin = true;
            }
        }
    }

    pub fn stored_head(&self) -> Option<BlockPtr> {
        match self.stored.iter().position(|&is_stored| !is_stored) {
            None => self.chain.back().map(|b| b.ptr()),
            Some(i) => Some(self.blocks[i].parent_ptr())
        }
    }
    
    pub fn stored_finalized_head(&self) -> Option<BlockPtr> {
        if !self.fin {
            return None
        }
        if self.chain.len() > self.blocks.len() {
            return Some(self.chain[0].ptr())
        }
        for i in (0..=self.blocks.len() - self.chain.len()).rev() {
            if self.stored[i] {
                return Some(self.blocks[i].ptr())
            }   
        }
        None
    }

    pub fn head(&self) -> Option<BlockPtr> {
        self.chain.back().map(|b| b.ptr())
    }

    pub fn compute_fork_base(&self, prev: &[BlockRef]) -> anyhow::Result<Option<BlockPtr>> {
        if let Some(head) = compute_fork_base(self.chain.iter().rev(), prev) {
            Ok(Some(head))
        } else if self.fin && !self.chain.is_empty() {
            Err(anyhow!("rollback below finalized head {}", self.chain[0]))
        } else {
            Ok(None)
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