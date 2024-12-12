use crate::chunk_processor::ChunkProcessor;
use sqd_array::slice::AnyTableSlice;
use std::collections::BTreeMap;


pub trait BaseBuilder {
    fn byte_size(&self) -> usize;
    fn clear(&mut self);
    fn as_slice(&self) -> BTreeMap<&'static str, AnyTableSlice<'_>>;
    fn chunk_processor(&self) -> ChunkProcessor;
    fn dataset_description(&self) -> sqd_dataset::DatasetDescriptionRef;
}


pub trait Builder: BaseBuilder {
    fn push(&mut self, line: &String) -> anyhow::Result<HashAndHeight>;
}


pub struct HashAndHeight {
    pub hash: String,
    pub height: u64,
}
