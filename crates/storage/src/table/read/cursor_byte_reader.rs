use crate::kv::KvReadCursor;
use crate::table::key::TableKeyFactory;
use anyhow::{ensure, Context};
use sqd_array::io::reader::ByteReader;
use sqd_array::util::bisect_offsets;


pub struct CursorByteReader<C> {
    cursor: C,
    key: TableKeyFactory,
    buffer: usize,
    page_offsets: Vec<u32>,
    current_page: Option<usize>
}


impl<C: KvReadCursor> CursorByteReader<C> {
    pub fn new(
        cursor: C,
        key: TableKeyFactory,
        buffer: usize,
        page_offsets: Vec<u32>
    ) -> Self
    {
        Self {
            cursor, 
            key, 
            buffer, 
            page_offsets,
            current_page: None
        }
    }
    
    fn find_page(&self, offset: usize) -> usize {
        let offset = offset as u32;
        if let Some(page) = self.current_page {
            let beg = self.page_offsets[page];
            let end = self.page_offsets[page + 1];
            if beg <= offset && offset < end {
                return page
            }
        }
        bisect_offsets(&self.page_offsets, offset).expect("out of bounds access")
    }
    
    fn go_to_page(&mut self, page: usize) -> anyhow::Result<()> {
        match self.current_page {
            Some(current) if current == page => return Ok(()),
            Some(current) if current + 1 == page => {
                self.current_page = None;
                self.cursor.next()?;
                ensure!(
                    self.cursor.is_valid() && self.cursor.key() == self.key.page(self.buffer, page), 
                    "page was not found at expected place"
                );
            },
            _ => {
                self.current_page = None;
                self.cursor.seek(self.key.page(self.buffer, page))?;
                ensure!(self.cursor.is_valid(), "page was not found")
            }
        }
        
        let expected_len = self.page_offsets[page + 1] - self.page_offsets[page];
        ensure!(
            self.cursor.value().len() == expected_len as usize,
            "expected page to have length {}, but got {}",
            expected_len,
            self.cursor.value().len()
        );
        
        self.current_page = Some(page);
        Ok(())
    }
}


impl<C: KvReadCursor> ByteReader for CursorByteReader<C> {
    fn len(&self) -> usize {
        self.page_offsets.last().copied().unwrap() as usize
    }

    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]> {
        assert!(offset + len <= self.len());
        let page = self.find_page(offset);
        
        self.go_to_page(page).with_context(|| {
            format!("failed to navigate to page {} of buffer {}", page, self.buffer)
        })?;
        
        let beg = offset - self.page_offsets[page] as usize;
        let end = std::cmp::min(
            beg + len, 
            self.page_offsets[page + 1] as usize - self.page_offsets[page] as usize
        );
        
        Ok(&self.cursor.value()[beg..end])
    }
}