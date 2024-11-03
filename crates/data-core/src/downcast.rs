use arrow::datatypes::DataType;
use sqd_array::slice::{AnySlice, Slice};
use std::sync::Arc;


#[derive(Clone)]
pub struct Downcast {
    inner: Arc<parking_lot::Mutex<DowncastState>>
}


impl Downcast {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(parking_lot::Mutex::new(DowncastState::new()))
        }
    }
    
    pub fn reset(&self) {
        self.inner.lock().reset()
    }

    pub fn reg_block_number(&mut self, array: &AnySlice<'_>) {
        let val = get_max(array);
        self.inner.lock().reg_block_number(val)
    }

    pub fn reg_item_index(&mut self, array: &AnySlice<'_>) {
        let val = get_max(array);
        self.inner.lock().reg_item_index(val)
    }

    pub fn get_block_number_type(&self) -> DataType {
        self.inner.lock().get_block_number_type()
    }

    pub fn get_item_index_type(&self) -> DataType {
        self.inner.lock().get_item_index_type()
    }
}


pub struct DowncastState {
    max_block_number: u64,
    max_item_index: u64
}


impl DowncastState {
    pub fn new() -> Self {
        Self {
            max_block_number: u32::MAX as u64,
            max_item_index: u16::MAX as u64
        }
    }

    pub fn reset(&mut self) {
        self.max_block_number = u32::MAX as u64;
        self.max_item_index = u16::MAX as u64;
    }
    
    pub fn reg_block_number(&mut self, val: u64) {
        self.max_block_number = std::cmp::max(
            self.max_block_number,
            val
        )
    }
    
    pub fn reg_item_index(&mut self, val: u64) {
        self.max_item_index = std::cmp::max(
            self.max_item_index,
            val
        );
    }

    pub fn get_block_number_type(&self) -> DataType {
        get_minimal_type(self.max_block_number)
    }

    pub fn get_item_index_type(&self) -> DataType {
        get_minimal_type(self.max_item_index)
    }
}


fn get_max(array: &AnySlice<'_>) -> u64 {
    match array {
        AnySlice::UInt8(s) => s.values().iter().copied().max().unwrap_or(0) as u64,
        AnySlice::UInt16(s) => s.values().iter().copied().max().unwrap_or(0) as u64,
        AnySlice::UInt32(s) => s.values().iter().copied().max().unwrap_or(0) as u64,
        AnySlice::UInt64(s) => s.values().iter().copied().max().unwrap_or(0),
        AnySlice::Int32(s) => s.values().iter().copied().max().unwrap_or(0) as u64,
        AnySlice::List(s) => {
            let range = s.offsets().range();
            get_max(&s.values().item().slice(range.start, range.len()))
        },
        _ => panic!("invalid index type")
    }
}


fn get_minimal_type(max: u64) -> DataType {
    if u8::try_from(max).is_ok() {
        return DataType::UInt8
    }
    if u16::try_from(max).is_ok() {
        return DataType::UInt16
    }
    if u32::try_from(max).is_ok() {
        return DataType::UInt32
    }
    DataType::UInt64
}