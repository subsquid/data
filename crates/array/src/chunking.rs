use crate::util::get_offset_position;


#[derive(Clone, Default, Debug)]
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
    
    pub fn build_tag_list(
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
            if chunk == last.chunk_index() && last.offset + last.len == i as u32 {
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


#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use crate::chunking::ChunkRange;
    use crate::util::build_offsets;


    #[test]
    fn test_tag_list_building() {
        let arb = prop::collection::vec(1..20usize, 100).prop_flat_map(|lengths| {
            let offsets = build_offsets(0, lengths.iter().copied());
            let len = offsets.last().copied().unwrap();
            let order_arb = Just((0..len).collect::<Vec<_>>()).prop_shuffle();
            (Just(offsets), order_arb)
        });

        proptest!(|((offsets, order) in arb)| {
            let chunks = ChunkRange::build_tag_list(&offsets, &order);
            let mut resulting_order = Vec::with_capacity(order.len());
            for c in chunks.iter() {
                assert!(c.len_index() > 0);
                let chunk_beg = offsets[c.chunk_index()];
                let chunk_end = offsets[c.chunk_index() + 1];
                assert!(chunk_beg <= c.offset_index());
                assert!(chunk_end >= c.offset_index() + c.len_index());
                for i in 0..c.len_index() {
                    resulting_order.push(c.offset_index() + i);
                }
            }
            assert_eq!(order, resulting_order);
        });
    }
}
