use std::ops::Deref;


pub trait KvWrite {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
}


pub trait KvRead {
    type Cursor: KvReadCursor;
    
    fn get(&self, key: &[u8]) -> anyhow::Result<Option<impl Deref<Target=[u8]>>>;

    fn new_cursor(&self) -> Self::Cursor;
}


pub trait KvReadCursor {
    fn seek_first(&mut self) -> anyhow::Result<()>;
    
    fn seek(&mut self, key: &[u8]) -> anyhow::Result<()>;
    
    fn seek_prev(&mut self, key: &[u8]) -> anyhow::Result<()>;

    fn next(&mut self) -> anyhow::Result<()>;
    
    fn prev(&mut self) -> anyhow::Result<()>;

    fn is_valid(&self) -> bool;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
}