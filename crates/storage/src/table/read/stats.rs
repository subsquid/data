use arrow::array::ArrayRef;
use arrow_buffer::OffsetBuffer;


#[derive(Clone)]
pub struct Stats {
    pub offsets: OffsetBuffer<u32>,
    pub min: ArrayRef,
    pub max: ArrayRef
}