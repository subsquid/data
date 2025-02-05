use crate::fs::FSRef;
use anyhow::ensure;
use async_stream::try_stream;
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};
use regex::Regex;
use sqd_primitives::BlockNumber;
use std::pin::pin;
use std::sync::LazyLock;


#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct DataChunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_hash: String,
    pub top: BlockNumber,
}


impl DataChunk {
    pub fn path(&self) -> String {
        format!(
            "{:010}/{:010}-{:010}-{}",
            self.top,
            self.first_block,
            self.last_block,
            self.last_hash
        )
    }
}


fn format_block_number(block_number: BlockNumber) -> String {
    format!("{:010}", block_number)
}


fn parse_range(dirname: &str) -> Option<(BlockNumber, BlockNumber, String)> {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(\d+)-(\d+)-(\w+)$").unwrap()
    });

    RE.captures(dirname).map(|caps| {
        let beg = caps[1].parse::<u64>().unwrap();
        let end = caps[2].parse::<u64>().unwrap();
        let hash = caps[3].to_string();
        (beg, end, hash)
    })
}


pub struct Layout {
    fs: FSRef
}


impl Layout {
    pub fn new(fs: FSRef) -> Self {
        Self { fs }
    }

    pub async fn get_tops(&self) -> anyhow::Result<Vec<u64>> {
        let mut tops: Vec<u64> = self.fs.ls()
            .await?
            .into_iter()
            .filter(|s| s.parse::<u64>().is_ok())
            .map(|s| {
                let top: u64 = s.parse().unwrap();
                ensure!(
                    format_block_number(top) == s,
                    "item {} looks like a top dir, but its name is not canonical",
                    s
                );
                Ok(top)
            })
            .collect::<anyhow::Result<_>>()?;
        tops.sort();
        Ok(tops)
    }

    async fn get_top_chunks(&self, top: u64) -> anyhow::Result<Vec<DataChunk>> {
        self.fs.cd(&format_block_number(top))
            .ls()
            .await?
            .into_iter()
            .filter_map(|s| {
                parse_range(&s).map(|(first_block, last_block, last_hash)| {
                    (s, DataChunk {
                        first_block,
                        last_block,
                        last_hash,
                        top
                    })
                })
            })
            .map(|(s, chunk)| {
                let path = format!("{:010}/{}", top, s);
                let canonical = chunk.path();
                ensure!(
                    path == canonical,
                    "item {} looks like a chunk dir, but its name is not canonical",
                    s
                );
                Ok(chunk)
            })
            .collect::<anyhow::Result<Vec<DataChunk>>>()
            .map(|mut chunks| {
                chunks.sort();
                chunks
            })
    }

    pub fn get_chunks(
        &self,
        first_block: BlockNumber,
        last_block: Option<BlockNumber>
    ) -> impl Stream<Item=anyhow::Result<DataChunk>> + '_
    {
        try_stream! {
            let last_block = last_block.unwrap_or(u64::MAX);
            if first_block > last_block {
                return
            }

            let tops = self.get_tops().await?;

            for i in 0..tops.len() {
                let top = tops[i];
                if last_block < top {
                    return
                }
                if i + 1 < tops.len() && tops[i + 1] < first_block {
                    continue
                }
                for chunk in self.get_top_chunks(top).await? {
                    if last_block < chunk.first_block {
                        return
                    }
                    if first_block > chunk.last_block {
                        continue
                    }
                    yield chunk
                }
            }
        }
    }

    pub fn get_chunks_in_reversed_order(
        &self,
        first_block: BlockNumber,
        last_block: Option<BlockNumber>
    ) -> impl Stream<Item=anyhow::Result<DataChunk>> + '_
    {
        try_stream! {
            let last_block = last_block.unwrap_or(u64::MAX);
            if first_block > last_block {
                return
            }

            for top in self.get_tops().await?.into_iter().rev() {
                if top > last_block {
                    continue
                }
                for chunk in self.get_top_chunks(top).await?.into_iter().rev() {
                    if chunk.first_block > last_block {
                        continue
                    }
                    if chunk.last_block < first_block {
                        return
                    }
                    yield chunk
                }
            }
        }
    }

    pub async fn create_chunk_writer(
        &self,
        chunk_check: &dyn Fn(&[String]) -> bool,
        top_dir_size: usize,
        first_block: BlockNumber,
        last_block: Option<BlockNumber>,
    ) -> anyhow::Result<ChunkWriter>
    {
        ensure!(first_block <= last_block.unwrap_or(BlockNumber::MAX));

        let mut chunks = pin!(self.get_chunks(first_block, last_block));
        let mut reversed_chunks = pin!(self.get_chunks_in_reversed_order(first_block, last_block));

        let first_chunk = chunks.try_next().await?;

        if let Some(chunk) = &first_chunk {
            if chunk.first_block != first_block {
                anyhow::bail!(
                    "First chunk of the range {}-{:?} is {}. Perhaps part of the range is controlled by another writer.",
                    first_block, last_block, chunk.path()
                )
            }
        }

        let mut last_chunk = reversed_chunks.try_next().await?;
        if let Some(chunk) = &last_chunk {
            if let Some(last_block) = last_block {
                if chunk.last_block > last_block {
                    anyhow::bail!(
                        "Chunk {} is not aligned with range {}-{}. Perhaps part of the range is controlled by another writer",
                        chunk.path(), first_block, last_block
                    )
                }
            }

            let path = chunk.path();
            let filelist = self.fs.cd(&path).ls().await?;
            if !chunk_check(&filelist) {
                self.fs.delete(&path).await?;
                last_chunk = reversed_chunks.next().await.transpose()?;
            }
        }

        let (top, chunks, prev_hash) = if let Some(last_chunk) = last_chunk {
            let chunks = self.get_top_chunks(last_chunk.top).await?;
            if chunks.is_empty() {
                anyhow::bail!("Data is not supposed to be changed by external process during writing")
            }
            (last_chunk.top, chunks, Some(last_chunk.last_hash))
        } else {
            (first_block, vec![], None)
        };

        Ok(ChunkWriter {
            top_dir_size,
            base_chunk_hash: prev_hash,
            last_block_limit: last_block.unwrap_or(BlockNumber::MAX),
            top,
            chunks
        })
    }
}


pub struct ChunkWriter {
    top_dir_size: usize,
    base_chunk_hash: Option<String>,
    last_block_limit: BlockNumber,
    top: BlockNumber,
    chunks: Vec<DataChunk>,
}


impl ChunkWriter {
    pub fn prev_chunk_hash(&self) -> Option<&str> {
        self.chunks
            .last()
            .map(|c| c.last_hash.as_ref())
            .or_else(|| {
                self.base_chunk_hash.as_ref().map(|s| s.as_ref())
            })
    }

    pub fn next_block(&self) -> BlockNumber {
        self.chunks
            .last()
            .map(|c| c.last_block + 1)
            .unwrap_or(self.top)
    }

    pub fn next_chunk(
        &mut self,
        first_block: BlockNumber,
        last_block: BlockNumber,
        last_hash: String
    ) -> DataChunk
    {
        assert_eq!(self.next_block(), first_block);
        assert!(first_block <= last_block);
        assert!(last_block <= self.last_block_limit);

        if self.chunks.len() >= self.top_dir_size {
            self.top = first_block;
            self.chunks.clear();
        }

        let chunk = DataChunk {
            first_block,
            last_block,
            last_hash,
            top: self.top,
        };

        self.chunks.push(chunk.clone());
        chunk
    }
}
