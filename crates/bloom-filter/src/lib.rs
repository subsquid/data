use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::Xxh3Builder;


pub struct BloomFilter {
    bit_array: Vec<bool>,
    num_hashes: usize,
}


impl BloomFilter {
    pub fn new(size: usize, num_hashes: usize) -> Self {
        BloomFilter {
            bit_array: vec![false; size],
            num_hashes,
        }
    }

    pub fn from_bit_array(bit_array: Vec<bool>, num_hashes: usize) -> Self {
        BloomFilter { bit_array, num_hashes }
    }

    pub fn to_bit_array(self) -> Vec<bool> {
        self.bit_array
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i as u64);
            self.bit_array[hash] = true;
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i as u64);
            if !self.bit_array[hash] {
                return false;
            }
        }
        true
    }

    fn hash<T: Hash>(&self, item: &T, seed: u64) -> usize {
        let mut hasher = Xxh3Builder::new().with_seed(seed).build();
        item.hash(&mut hasher);
        (hasher.finish() % self.bit_array.len() as u64) as usize
    }
}


#[cfg(test)]
mod test {
    use crate::BloomFilter;


    #[test]
    fn basic_test() {
        let mut bloom_filter = BloomFilter::new(64 * 8, 7);

        bloom_filter.insert(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN");
        bloom_filter.insert(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY");
        bloom_filter.insert(&"6YBhJ2Jr3QSMLkQcp6wsfKZFN1rW46mtyBwqeCciR6d5");

        assert!(bloom_filter.contains(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN"));
        assert!(bloom_filter.contains(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY"));
        assert!(!bloom_filter.contains(&"11111111111111111111111111111111"));
    }
}
