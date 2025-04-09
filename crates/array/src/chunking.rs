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
    
    pub fn build_abs_order_list(
        chunk_offsets: &[usize],
        order: &[usize]
    ) -> Vec<Self> 
    {
        Self::build_order_list(chunk_offsets, order, true)
    }

    pub fn build_rel_order_list(
        chunk_offsets: &[usize],
        order: &[usize]
    ) -> Vec<Self>
    {
        Self::build_order_list(chunk_offsets, order, false)
    }

    fn build_order_list(
        chunk_offsets: &[usize],
        order: &[usize],
        is_abs: bool
    ) -> Vec<Self>
    {
        let mut chunks = Vec::new();

        let mut last = ChunkRange {
            chunk: chunk_offsets.len() as u32 - 1, // never existing chunk
            offset: 0,
            len: 0,
        };

        let mut prev = 0;
        for i in order.iter().copied() {
            let chunk = get_offset_position(chunk_offsets, i, last.chunk as usize);
            if chunk == last.chunk_index() && prev + 1 == i {
                last.len += 1;
            } else {
                if last.len > 0 {
                    chunks.push(std::mem::take(&mut last))
                }
                last.chunk = chunk as u32;
                last.offset = if is_abs { i } else { i - chunk_offsets[chunk] } as u32;
                last.len = 1;
            }
            prev = i;
        }

        if last.len > 0 {
            chunks.push(std::mem::take(&mut last))
        }

        chunks
    }
}


#[cfg(test)]
mod test {
    use crate::chunking::ChunkRange;
    use crate::util::build_offsets;
    use proptest::prelude::*;

    #[test]
    fn test_abs_order_list_building() {
        let arb = prop::collection::vec(1..20usize, 100).prop_flat_map(|lengths| {
            let offsets = build_offsets(0, lengths.iter().copied());
            let len = offsets.last().copied().unwrap();
            let order_arb = Just((0..len).collect::<Vec<_>>()).prop_shuffle();
            (Just(offsets), order_arb)
        });

        proptest!(|((offsets, order) in arb)| {
            let chunks = ChunkRange::build_abs_order_list(&offsets, &order);
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

    #[test]
    fn test_rel_order_list_building() {
        let arb = prop::collection::vec(1..20usize, 100).prop_flat_map(|lengths| {
            let offsets = build_offsets(0, lengths.iter().copied());
            let len = offsets.last().copied().unwrap();
            let order_arb = Just((0..len).collect::<Vec<_>>()).prop_shuffle();
            (Just(offsets), order_arb)
        });

        proptest!(|((offsets, order) in arb)| {
            let chunks = ChunkRange::build_rel_order_list(&offsets, &order);
            let mut resulting_order = Vec::with_capacity(order.len());
            for c in chunks.iter() {
                assert!(c.len_index() > 0);
                let chunk_beg = offsets[c.chunk_index()];
                let chunk_end = offsets[c.chunk_index() + 1];
                assert!(chunk_end - chunk_beg >= c.len_index() + c.offset_index());
                for i in 0..c.len_index() {
                    resulting_order.push(chunk_beg + c.offset_index() + i);
                }
            }
            assert_eq!(order, resulting_order);
        });
    }
}
