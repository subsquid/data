pub fn arrange<T>(list: &mut [T], mut indexes: Vec<usize>) {
    for el in 0..list.len() {
        let mut i = el;
        while indexes[i] != el {
            list.swap(indexes[i], i);
            std::mem::swap(&mut indexes[i], &mut i);
        }
        indexes[i] = i;
    }
}


#[cfg(test)]
mod test {
    use crate::util::arrange;

    #[test]
    fn test_arrange() {
        let mut vec: Vec<_> = (0..6).collect();
        let order = vec![3, 5, 4, 2, 0, 1];
        arrange(&mut vec, order);
        assert_eq!(vec, vec![3, 5, 4, 2, 0, 1]);
    }
}