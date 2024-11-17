use crate::util::get_offset_position;


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
    
    pub fn build_list_from_chunk_offsets_and_order(
        chunk_offsets: &[usize],
        order: &[usize]
    ) -> Vec<Self> 
    {
        let mut chunks = Vec::new();

        let mut last = ChunkRange {
            chunk: chunk_offsets.len() as u32 - 1, // never existing chunk
            offset: 0,
            len: 0,
        };

        for i in order.iter().copied() {
            let chunk = get_offset_position(chunk_offsets, i, last.chunk as usize);
            if chunk == last.chunk_index() {
                debug_assert_eq!(last.offset + last.len, i as u32);
                last.len += 1;
            } else {
                if last.len > 0 {
                    chunks.push(std::mem::take(&mut last))
                }
                last.chunk = chunk as u32;
                last.offset = i as u32;
                last.len = 1;
            }
        }

        if last.len > 0 {
            chunks.push(std::mem::take(&mut last))
        }
        
        chunks
    }
}
