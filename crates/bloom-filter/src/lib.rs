use arrow_buffer::bit_util;
use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::Xxh3Builder;


pub struct BloomFilter<const N: usize> {
    byte_array: [u8; N],
    num_hashes: usize,
}


impl<const N: usize> BloomFilter<N> {
    pub fn new(num_hashes: usize) -> Self {
        BloomFilter {
            byte_array: [0; N],
            num_hashes,
        }
    }

    pub fn to_byte_array(self) -> [u8; N] {
        self.byte_array
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i as u64);
            let byte_index = hash / 8;
            let bit_index = hash % 8;
            unsafe {
                let byte = self.byte_array.get_unchecked_mut(byte_index);
                bit_util::set_bit_raw(byte, bit_index);
            }
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i as u64);
            let byte_index = hash / 8;
            let bit_index = hash % 8;
            unsafe {
                let byte = self.byte_array.get_unchecked(byte_index);
                if !bit_util::get_bit_raw(byte, bit_index) {
                    return false;
                }
            }
        }
        true
    }

    fn hash<T: Hash>(&self, item: &T, seed: u64) -> usize {
        let mut hasher = Xxh3Builder::new().with_seed(seed).build();
        item.hash(&mut hasher);
        hasher.finish() as usize % (N * 8)
    }
}


#[cfg(test)]
mod test {
    use crate::BloomFilter;


    #[test]
    fn basic_test() {
        let mut bloom_filter = BloomFilter::<64>::new(7);

        bloom_filter.insert(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN");
        bloom_filter.insert(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY");
        bloom_filter.insert(&"6YBhJ2Jr3QSMLkQcp6wsfKZFN1rW46mtyBwqeCciR6d5");

        assert!(bloom_filter.contains(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN"));
        assert!(bloom_filter.contains(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY"));
        assert!(!bloom_filter.contains(&"11111111111111111111111111111111"));
    }
}
