mod bitmask;
mod native;
mod nullmask;
mod offsets;
mod page;
mod storage_cell;
mod storage_writer;
mod table;


pub use storage_cell::*;
pub use table::*;


pub fn use_small_buffers() -> RestoreBufferSizesGuard {
    RestoreBufferSizesGuard {
        bitmask_page_size: set_buf_size(&bitmask::PAGE_SIZE, 4),
        native_page_size: set_buf_size(&native::PAGE_SIZE, 256),
        offset_page_len: set_buf_size(&offsets::PAGE_LEN, 32)
    }
}


pub struct RestoreBufferSizesGuard {
    bitmask_page_size: usize,
    native_page_size: usize,
    offset_page_len: usize
}


impl Drop for RestoreBufferSizesGuard {
    fn drop(&mut self) {
        set_buf_size(&bitmask::PAGE_SIZE, self.bitmask_page_size);
        set_buf_size(&native::PAGE_SIZE, self.native_page_size);
        set_buf_size(&offsets::PAGE_LEN, self.offset_page_len);
    }
}


fn set_buf_size(cell: &parking_lot::RwLock<usize>, new_val: usize) -> usize {
    let mut lock = cell.write();
    let current = lock.clone();
    *lock = new_val;
    current
}