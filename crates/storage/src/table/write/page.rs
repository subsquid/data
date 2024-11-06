use crate::kv::KvWrite;
use crate::table::key::TableKeyFactory;
use arrow_buffer::ToByteSlice;


pub trait PageWriter {
    fn write_page(&mut self, item_count: usize, bytes: &[u8]) -> anyhow::Result<()>;
}


pub struct BufferPageWriter<S> {
    storage: S,
    key: TableKeyFactory,
    buffer_index: usize,
    page_offsets: Vec<u32>
}


impl<S: KvWrite> BufferPageWriter<S> {
    pub fn new(storage: S, key: TableKeyFactory, buffer_index: usize) -> Self {
        Self {
            storage,
            key,
            buffer_index,
            page_offsets: vec![0]
        }
    }

    pub fn num_pages(&self) -> usize {
        self.page_offsets.len() - 1
    }
    
    pub fn num_items(&self) -> usize {
        self.page_offsets.last().copied().unwrap() as usize
    }
    
    pub fn finish(mut self) -> anyhow::Result<S> {
        let key = self.key.offsets(self.buffer_index);
        self.storage.put(key, self.page_offsets.to_byte_slice())?;
        Ok(self.storage)
    }
}


impl<S: KvWrite> PageWriter for BufferPageWriter<S> {
    fn write_page(&mut self, item_count: usize, bytes: &[u8]) -> anyhow::Result<()> {
        let key = self.key.page(self.buffer_index, self.num_pages());
        self.storage.put(key, bytes)?;
        self.page_offsets.push((self.num_items() + item_count) as u32);
        Ok(())
    }
}