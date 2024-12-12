use crate::fs::Fs;
use regex::Regex;


fn format_block(block_number: u64) -> String {
    format!("{:010}", block_number)
}


fn parse_range(dirname: &str) -> Option<(u64, u64, String)> {
    let re = Regex::new(r"^(\d{10})-(\d{10})-(\w+)$").unwrap();
    if let Some(caps) = re.captures(dirname) {
        let beg = caps[1].parse::<u64>().unwrap();
        let end = caps[2].parse::<u64>().unwrap();
        let hash = caps[3].to_string();
        return Some((beg, end, hash));
    }
    None
}


#[derive(Debug)]
pub struct DataChunk {
    first_block: u64,
    last_block: u64,
    last_hash: String,
    top: u64,
}


impl DataChunk {
    pub fn path(&self) -> String {
        format!(
            "{}/{:010}-{:010}-{}",
            format_block(self.top),
            format_block(self.first_block),
            format_block(self.last_block),
            self.last_hash
        )
    }
}


fn get_tops(fs: &Box<dyn Fs>) -> anyhow::Result<Vec<u64>> {
    let top_dirs = fs.ls(&[])?;
    let mut tops = top_dirs
        .iter()
        .map(|d| d.parse::<u64>())
        .collect::<Result<Vec<_>, _>>()?;
    tops.sort();
    Ok(tops)
}


fn get_top_ranges(fs: &Box<dyn Fs>, top: u64) -> Vec<(u64, u64, String)> {
    let ranges: Vec<(u64, u64, String)> = fs
        .ls(&[&format_block(top)])
        .unwrap()
        .iter()
        .map(|d| parse_range(d).unwrap())
        .collect();
    ranges
}


pub fn get_chunks<'a>(
    fs: &'a Box<dyn Fs>,
    first_block: u64,
    last_block: Option<u64>,
) -> anyhow::Result<impl Iterator<Item = DataChunk> + 'a> {
    if let Some(last_block) = last_block {
        assert!(first_block <= last_block);
    }

    let tops = get_tops(fs)?;

    Ok(tops.into_iter().flat_map(move |top| {
        if let Some(last_block) = last_block {
            if last_block < top {
                return vec![];
            }
        }

        let ranges = get_top_ranges(fs, top);
        return ranges
            .into_iter()
            .filter_map(|(beg, end, hash)| {
                if let Some(last_block) = last_block {
                    if last_block < beg {
                        return None;
                    }
                }

                if first_block > end {
                    return None;
                }

                Some(DataChunk {
                    first_block: beg,
                    last_block: end,
                    last_hash: hash,
                    top,
                })
            })
            .collect::<Vec<DataChunk>>();
    }))
}


pub fn get_chunks_in_reversed_order<'a>(
    fs: &'a Box<dyn Fs>,
    first_block: u64,
    last_block: Option<u64>,
) -> anyhow::Result<impl Iterator<Item = DataChunk> + 'a> {
    if let Some(last_block) = last_block {
        assert!(first_block <= last_block);
    }

    let tops = get_tops(fs)?;

    Ok(tops.into_iter().rev().flat_map(move |top| {
        if let Some(last_block) = last_block {
            if top > last_block {
                return vec![];
            }
        }

        let ranges = get_top_ranges(fs, top);
        return ranges
            .into_iter()
            .rev()
            .filter_map(|(beg, end, hash)| {
                if let Some(last_block) = last_block {
                    if beg > last_block {
                        return None;
                    }
                }

                if end < first_block {
                    return None;
                }

                Some(DataChunk {
                    first_block: beg,
                    last_block: end,
                    last_hash: hash,
                    top,
                })
            })
            .collect::<Vec<DataChunk>>();
    }))
}


pub struct ChunkWriter {
    last_block: Option<u64>,
    top_dir_size: usize,
    top: u64,
    ranges: Vec<(u64, u64, String)>,
}


impl ChunkWriter {
    pub fn new(
        fs: &Box<dyn Fs>,
        chunk_check: impl Fn(&[String]) -> bool,
        first_block: u64,
        last_block: Option<u64>,
        top_dir_size: usize,
    ) -> anyhow::Result<ChunkWriter> {
        if let Some(last_block) = last_block {
            assert!(last_block >= first_block)
        }

        let mut chunks = get_chunks(fs, first_block, last_block)?;
        let mut reversed_chunks = get_chunks_in_reversed_order(fs, first_block, last_block)?;

        let first_chunk = chunks.next();
        let mut last_chunk = reversed_chunks.next();

        if let Some(chunk) = &first_chunk {
            if chunk.first_block != first_block {
                anyhow::bail!("First chunk of the range {}-{:?} is {:?}. Perhaps part of the range {}-{:?} is controlled by another writer", first_block, last_block, chunk, first_block, last_block)
            }
        }

        if let Some(chunk) = &last_chunk {
            if let Some(last_block) = last_block {
                if chunk.last_block > last_block {
                    anyhow::bail!("Chunk {:?} is not aligned with the range {}-{}. Perhaps part of the range {}-{} is controlled by another writer", chunk, first_block, last_block, first_block, last_block)
                }
            }

            let filelist = fs.ls(&[&chunk.path()])?;
            if !chunk_check(&filelist) {
                fs.delete(&chunk.path())?;
                last_chunk = reversed_chunks.next();
            }
        }

        let (top, ranges) = if let Some(last_chunk) = &last_chunk {
            let ranges = get_top_ranges(fs, last_chunk.top);
            if ranges.is_empty() {
                anyhow::bail!(
                    "data is not supposed to be changed by external process during writing"
                )
            }
            (last_chunk.top, ranges)
        } else {
            (first_block, vec![])
        };

        drop(chunks);
        drop(reversed_chunks);

        Ok(Self {
            last_block,
            top_dir_size,
            top,
            ranges,
        })
    }

    pub fn next_block(&self) -> u64 {
        self.ranges
            .last()
            .map(|range| range.1 + 1)
            .unwrap_or(self.top)
    }

    pub fn next_chunk(
        &mut self,
        first_block: u64,
        last_block: u64,
        last_hash: &String,
    ) -> anyhow::Result<DataChunk> {
        assert!(self.next_block() <= first_block);
        assert!(first_block <= last_block);
        if let Some(value) = self.last_block {
            assert!(last_block <= value);
        }

        if self.ranges.len() < self.top_dir_size || self.last_block == Some(last_block) {
            self.top = self.top;
        } else {
            self.top = first_block;
            self.ranges.clear();
        }

        self.ranges.push((first_block, last_block, last_hash.clone()));

        Ok(DataChunk {
            first_block,
            last_block,
            last_hash: last_hash.clone(),
            top: self.top,
        })
    }
}
