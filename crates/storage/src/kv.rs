use std::ops::Deref;


pub trait KvWrite {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
}


pub trait KvRead {
    fn get<'a, 'b>(&'a self, key: &'b [u8]) -> anyhow::Result<Option<impl Deref<Target=[u8]> + 'a>>;

    fn new_cursor(&self) -> impl KvReadCursor;
}


pub trait KvReadCursor {
    fn seek(&mut self, key: &[u8]) -> anyhow::Result<()>;

    fn next(&mut self) -> anyhow::Result<()>;

    fn is_valid(&self) -> bool;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
}