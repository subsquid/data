use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::Xxh3Builder;


pub struct BloomFilter {
    bytes: Box<[u8]>,
    num_hashes: usize,
}


impl BloomFilter {
    pub fn new(byte_size: usize, num_hashes: usize) -> Self {
        BloomFilter {
            bytes: vec![0; byte_size].into_boxed_slice(),
            num_hashes,
        }
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }
    
    pub fn clear(&mut self) {
        for i in &mut self.bytes {
            *i = 0;
        }
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        for i in 0..self.num_hashes {
            let bit = self.hash_bit(i, item);
            set_bit(&mut self.bytes, bit)
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        for i in 0..self.num_hashes {
            let bit = self.hash_bit(i, item);
            if !get_bit(&self.bytes, bit) {
                return false;
            }
        }
        true
    }

    fn hash_bit<T: Hash>(&self, n: usize, item: &T) -> usize {
        let mut hasher = Xxh3Builder::new().with_seed(n as u64).build();
        item.hash(&mut hasher);
        let bit = hasher.finish() % (self.bytes.len() * 8) as u64;
        bit as usize
    }
}


fn get_bit(data: &[u8], i: usize) -> bool {
    data[i / 8] & (1 << (i % 8)) != 0
}


fn set_bit(data: &mut [u8], i: usize) {
    data[i / 8] |= 1 << (i % 8);
}


#[cfg(test)]
mod test {
    use crate::BloomFilter;


    #[test]
    fn basic_test() {
        let mut bloom_filter = BloomFilter::new(64, 7);

        bloom_filter.insert(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN");
        bloom_filter.insert(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY");
        bloom_filter.insert(&"6YBhJ2Jr3QSMLkQcp6wsfKZFN1rW46mtyBwqeCciR6d5");

        assert!(bloom_filter.contains(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN"));
        assert!(bloom_filter.contains(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY"));
        assert!(!bloom_filter.contains(&"11111111111111111111111111111111"));
    }
}
