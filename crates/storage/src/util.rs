use borsh::BorshSerialize;


pub fn is_within_tolerance(val: usize, target: usize, tol: usize) -> bool {
    target - target * tol / 100 <= val && val <= target + target * tol / 100
}


pub fn next_chunk(target_chunk_size: usize, items_left: usize) -> usize {
    assert!(target_chunk_size > 0);
    if items_left <= target_chunk_size {
        return items_left
    }
    let mut n_chunks = items_left / target_chunk_size;
    let rem = items_left - n_chunks * target_chunk_size;
    if rem > target_chunk_size / 2 {
        n_chunks += 1;
    }
    items_left / n_chunks
}


pub fn borsh_serialize<T: BorshSerialize>(value: &T) -> Vec<u8> {
    borsh::to_vec(value).unwrap()
}