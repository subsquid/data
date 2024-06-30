use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

use crate::core::row::Row;
use crate::core::util::arrange;


pub struct Sorter<R: Row> {
    file: BufWriter<File>,
    keys: Vec<R::Key>,
    offsets: Vec<usize>,
    chunks: Vec<usize>,
    buf: Vec<u8>,
    buf_offsets: Vec<usize>,
    in_memory_threshold: usize
}


impl <R: Row> Default for Sorter<R> {
    fn default() -> Self {
        Self::new(250_000, 8 * 1024 * 1024)
    }
}


impl <R: Row> Sorter<R> {
    pub fn new(capacity: usize, memory_threshold: usize) -> Self {
        Self {
            file: BufWriter::with_capacity(16 * 1024, tempfile::tempfile().unwrap()),
            keys: Vec::with_capacity(capacity),
            offsets: {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                offsets
            },
            chunks: {
                let mut chunks = Vec::with_capacity(1024);
                chunks.push(0);
                chunks
            },
            buf: Vec::with_capacity(memory_threshold + min(256 * 1024, memory_threshold / 3)),
            buf_offsets: {
                let mut offsets = Vec::with_capacity(1024);
                offsets.push(0);
                offsets
            },
            in_memory_threshold: memory_threshold
        }
    }

    pub fn num_rows(&self) -> usize {
        self.keys.len()
    }

    pub fn num_bytes(&self) -> usize {
        self.offsets.last().copied().unwrap() + self.buf_offsets.last().copied().unwrap()
    }

    pub fn keys(&self) -> &[R::Key] {
        self.keys.as_slice()
    }

    pub fn push(&mut self, row: &R) {
        if self.in_memory_threshold < self.buf.len() {
            self.flush(None)
        }
        let key = row.key();
        self.keys.push(key);
        row.serialize(&mut self.buf);
        self.buf_offsets.push(self.buf.len());
    }

    fn flush(&mut self, mut maybe_to_buf: Option<&mut Vec<u8>>) {
        let mut file_offset = self.offsets.last().cloned().unwrap();
        let len = self.buf_offsets.len() - 1;
        let keys_offset = self.keys.len() - len;
        let keys = &mut self.keys[keys_offset..];

        let mut indexes: Vec<usize> = (0..len).collect();
        indexes.sort_by_key(|i| &keys[*i]);

        for i in indexes.iter().copied() {
            let beg = self.buf_offsets[i];
            let end = self.buf_offsets[i + 1];
            let bytes = &self.buf[beg..end];
            if let Some(ref mut buf) = maybe_to_buf {
                buf.extend_from_slice(bytes);
            } else {
                self.file.write_all(bytes).unwrap();
            }
            file_offset += end - beg;
            self.offsets.push(file_offset);
        }

        arrange(keys, indexes);

        self.chunks.push(self.keys.len());
        self.buf.clear();
        self.buf_offsets.clear();
        self.buf_offsets.push(0);
    }

    pub fn take_sorted(&mut self) -> SortedRows<R> {
        let chunks = self.take_chunks();
        let order = self.take_keys();
        let file = self.take_file();

        let offsets = self.offsets.clone();
        self.offsets.clear();
        self.offsets.push(0);

        SortedRows {
            file,
            order,
            pos: 0,
            offsets,
            chunks,
            row_phantom: PhantomData::default()
        }
    }

    fn take_chunks(&mut self) -> HashMap<usize, SortedChunk>{
        let mut chunks = HashMap::with_capacity(self.chunks.len());

        for i in 0..self.chunks.len() - 1 {
            chunks.insert(self.chunks[i], SortedChunk {
                buf: Vec::with_capacity(24 * 1024),
                pos: 0,
                limit: self.chunks[i+1]
            });
        }

        if self.buf.len() > 0 {
            let pos = self.chunks.last().copied().unwrap();

            let mut last_chunk = SortedChunk {
                buf: Vec::with_capacity(self.buf.len()),
                pos: 0,
                limit: self.keys.len()
            };

            self.flush(Some(&mut last_chunk.buf));

            chunks.insert(pos, last_chunk);
        }

        self.chunks.clear();

        chunks
    }

    fn take_keys(&mut self) -> Vec<usize> {
        let mut order: Vec<_> = (0..self.keys.len()).collect();
        order.sort_by_key(|i| &self.keys[*i]);
        self.keys.clear();
        order
    }

    fn take_file(&mut self) -> File {
        let file = std::mem::replace(
            &mut self.file,
            BufWriter::with_capacity(16 * 1024, tempfile::tempfile().unwrap())
        );
        file.into_inner().unwrap()
    }
}


struct SortedChunk {
    buf: Vec<u8>,
    pos: usize,
    limit: usize,
}


pub struct SortedRows<R: Row> {
    file: File,
    order: Vec<usize>,
    pos: usize,
    offsets: Vec<usize>,
    chunks: HashMap<usize, SortedChunk>,
    row_phantom: PhantomData<R>
}


impl<R: Row> SortedRows<R> {
    pub fn offset(&self) -> usize {
        if self.pos < self.offsets.len() {
            self.offsets[self.pos]
        } else {
            self.offsets.last().copied().unwrap()
        }
    }

    fn fetch_chunk(&mut self, mut pos: usize, chunk: &mut SortedChunk) {
        let mut fetch_size = 0;
        let offset = self.offsets[pos];
        while fetch_size < 16 * 1024 && pos < chunk.limit {
            pos += 1;
            fetch_size = self.offsets[pos] - offset;
        }
        assert!(fetch_size > 0);
        chunk.pos = 0;
        chunk.buf.clear();
        chunk.buf.reserve(fetch_size);
        unsafe { chunk.buf.set_len(fetch_size) }
        self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.file.read_exact(&mut chunk.buf).unwrap();
    }

    fn read_chunk(&mut self, pos: usize, chunk: &mut SortedChunk) -> R {
        let len = self.offsets[pos + 1] - self.offsets[pos];
        let bytes = &chunk.buf[chunk.pos..chunk.pos + len];
        chunk.pos += len;
        R::deserialize(bytes)
    }
}


impl <R: Row> Iterator for SortedRows<R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.order.len() {
            return None
        }
        let pos = self.order[self.pos];
        let mut chunk = self.chunks.remove(&pos).unwrap();
        if chunk.pos >= chunk.buf.len() {
            self.fetch_chunk(pos, &mut chunk)
        }
        let row = self.read_chunk(pos, &mut chunk);
        if pos + 1 < chunk.limit {
            self.chunks.insert(pos + 1, chunk);
        }
        self.pos += 1;
        Some(row)
    }
}


#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use crate::core::row::{Row};
    use crate::core::sorter::Sorter;

    impl Row for String {
        type Key = String;

        fn key(&self) -> Self::Key {
            self.clone()
        }
    }

    fn check_sort(mem_threshold: usize, mut values: Vec<String>) {
        // println!("threshold={}, values={:?}", mem_threshold, values);
        let mut sorter = Sorter::new(100, mem_threshold);
        for val in values.iter() {
            sorter.push(val);
        }
        let result: Vec<_> = sorter.take_sorted().collect();
        values.sort();
        assert_eq!(result, values)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn sorting(
            mem_threshold in 0..1000usize,
            values in prop::collection::vec("[a-z]*", 0..100usize)
        ) {
            check_sort(mem_threshold, values)
        }
    }
}