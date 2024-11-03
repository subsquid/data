
#[derive(Clone, Default)]
pub struct ChunkRange {
    pub chunk: u32,
    pub offset: u32,
    pub len: u32
}


impl ChunkRange {
    pub fn new(chunk: usize, offset: usize, len: usize) -> Self {
        Self {
            chunk: chunk as u32,
            offset: offset as u32,
            len: len as u32
        }
    }
    
    #[inline]
    pub fn chunk_index(&self) -> usize {
        self.chunk as usize
    }

    #[inline]
    pub fn offset_index(&self) -> usize {
        self.offset as usize
    }
    
    #[inline]
    pub fn len_index(&self) -> usize {
        self.len as usize
    }
}