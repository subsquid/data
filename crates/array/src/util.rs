
#[inline]
pub fn validate_offsets<I: Ord + Default + Copy>(offsets: &[I]) -> Result<(), &'static str> {
    if offsets.len() == 0 {
        return Err("offsets slice can't be empty")
    }
    
    let mut prev = offsets[0];
    if prev < I::default() {
        return Err("found negative offset value")
    }
    
    for &val in offsets[1..].iter() {
        if val < prev {
            return Err("offset values are not monotonically increasing")
        }
        prev = val
    }
    
    Ok(())
}


macro_rules! invalid_buffer_access {
    () => {
        panic!("invalid arrow buffer access")
    };
}
pub(crate) use invalid_buffer_access;