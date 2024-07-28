use std::ops::Range;


pub trait Slice<'a>: Sized + Clone {
    fn read_page(bytes: &'a [u8]) -> anyhow::Result<Self>;

    unsafe fn read_valid_page(bytes: &'a [u8]) -> Self {
        Self::read_page(bytes).unwrap()
    }

    fn write_page(&self, buf: &mut Vec<u8>);

    fn len(&self) -> usize;

    fn slice(&self, offset: usize, len: usize) -> Self {
        self.slice_range(offset..offset + len)
    }

    fn slice_range(&self, range: Range<usize>) -> Self {
        self.slice(range.start, range.len())
    }
}


pub trait Builder {
    type Slice<'a>: Slice<'a>;

    fn push_slice(&mut self, slice: &Self::Slice<'_>);

    fn as_slice(&self) -> Self::Slice<'_>;

    fn len(&self) -> usize;
    
    fn capacity(&self) -> usize;
}


pub trait DataBuilder {
    fn push_page(&mut self, page: &[u8]) -> anyhow::Result<()>;

    unsafe fn push_valid_page(&mut self, page: &[u8]);

    fn push_page_range(&mut self, page: &[u8], range: Range<usize>) -> anyhow::Result<()>;

    unsafe fn push_valid_page_range(&mut self, page: &[u8], range: Range<usize>);
}


impl <T> DataBuilder for T where T: Builder
{
    fn push_page(&mut self, page: &[u8]) -> anyhow::Result<()> {
        let slice = T::Slice::read_page(page)?;
        self.push_slice(&slice);
        Ok(())
    }

    unsafe fn push_valid_page(&mut self, page: &[u8]) {
        let slice = T::Slice::read_valid_page(page);
        self.push_slice(&slice)
    }

    fn push_page_range(&mut self, page: &[u8], range: Range<usize>) -> anyhow::Result<()> {
        let slice = T::Slice::read_page(page)?;
        self.push_slice(&slice.slice_range(range));
        Ok(())
    }

    unsafe fn push_valid_page_range(&mut self, page: &[u8], range: Range<usize>) {
        let slice = T::Slice::read_valid_page(page);
        self.push_slice(&slice.slice_range(range));
    }
}