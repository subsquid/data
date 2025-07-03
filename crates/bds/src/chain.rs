use sqd_primitives::{Block, BlockNumber, BlockRef};
use std::collections::VecDeque;
use std::ops::Index;


pub enum BlockPos {
    Index(usize),
    BelowFirst,
    BelowHead,
    Head,
    AboveHead,
    Unavailable,
}


pub struct Chain<B> {
    blocks: VecDeque<B>,
    head: Option<BlockRef>,
    droppable: Vec<bool>
}


impl <B: Block> Chain<B> {
    pub fn new() -> Self {
        Self {
            blocks: VecDeque::with_capacity(10),
            head: None,
            droppable: Vec::with_capacity(10)
        }
    }
    
    pub fn first(&self) -> Option<&B> {
        self.blocks.get(0)
    }
    
    pub fn position(&self, block_number: BlockNumber) -> BlockPos {
        todo!()
    }
    
    pub fn push(&mut self, block: B) {
        todo!()
    }
    
    pub fn drop(&mut self, number: BlockNumber, hash: &str) -> bool {
        todo!()
    }
    
    pub fn block_slices(&self) -> (&[B], &[B]) {
        self.blocks.as_slices()
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