use anyhow::ensure;
use sqd_primitives::range::RangeList;
use std::ops::Range;


pub struct PageRead {
    page_index: usize,
    write_offset: usize,
    ranges_slice: Range<usize>
}


pub enum Pagination<'a> {
    Ranged {
        pages: Vec<PageRead>,
        ranges: &'a RangeList<u32>,
        page_offsets: &'a [u32]
    },
    All {
        page_offsets: &'a [u32],
        ranges: [Range<u32>; 1]
    }
}


impl <'a> Pagination<'a> {
    pub fn new(page_offsets: &'a [u32], ranges: Option<&'a RangeList<u32>>) -> anyhow::Result<Self> {
        Ok(if let Some(ranges) = ranges {
            Pagination::Ranged {
                pages: build_page_reads(page_offsets, ranges)?,
                ranges,
                page_offsets
            }
        } else {
            Pagination::All {
                page_offsets,
                ranges: [page_offsets[0]..page_offsets.last().unwrap().clone()]
            }
        })
    }

    pub fn num_pages(&self) -> usize {
        match self {
            Pagination::Ranged { pages, .. } => pages.len(),
            Pagination::All { page_offsets, .. } => page_offsets.len() - 1
        }
    }

    pub fn num_items(&self) -> usize {
        match self {
            Pagination::Ranged { ranges, .. } => {
                ranges.iter().map(|r| r.len()).sum()
            },
            Pagination::All { ranges, .. } => {
                ranges[0].end as usize
            }
        }
    }

    pub fn page_index(&self, seq: usize) -> usize {
        match self {
            Pagination::Ranged { pages, .. } => pages[seq].page_index,
            Pagination::All { page_offsets, .. } => {
                assert!(seq < page_offsets.len() - 1);
                seq
            }
        }
    }

    pub fn page_range(&self, seq: usize) -> Range<usize> {
        let (offsets, idx) = match self {
            Pagination::Ranged { page_offsets, pages, .. } => {
                (page_offsets, pages[seq].page_index)
            },
            Pagination::All { page_offsets, .. } => {
                (page_offsets, seq)
            }
        };
        offsets[idx] as usize..offsets[idx + 1] as usize
    }

    pub fn page_write_offset(&self, seq: usize) -> usize {
        match self {
            Pagination::Ranged { pages, .. } => {
                pages[seq].write_offset
            },
            Pagination::All { page_offsets, .. } => {
                page_offsets[seq] as usize
            }
        }
    }

    pub fn iter_ranges(&self, seq: usize) -> impl Iterator<Item = Range<usize>> + '_ {
        let (ranges, page_range) = match self {
            Pagination::Ranged { pages, ranges, page_offsets } => {
                let page = &pages[seq];
                (
                    &ranges.as_slice()[page.ranges_slice.clone()],
                    page_offsets[page.page_index]..page_offsets[page.page_index + 1]
                )
            },
            Pagination::All { ranges, page_offsets } => {
                (
                    ranges.as_slice(),
                    page_offsets[seq]..page_offsets[seq + 1]
                )
            }
        };
        ranges.iter().map(move |r| {
            let beg = r.start.saturating_sub(page_range.start);
            let end = std::cmp::min(r.end, page_range.end) - page_range.start;
            beg as usize..end as usize
        })
    }
}


fn build_page_reads(page_offsets: &[u32], ranges: &RangeList<u32>) -> anyhow::Result<Vec<PageRead>> {
    ensure!(
        page_offsets.last().cloned().unwrap() >= ranges.end(),
        "range list is out of bounds: {} < {}",
        page_offsets.last().cloned().unwrap(),
        ranges.end()
    );

    let mut reads = Vec::new();
    let mut write_offset = 0;
    let mut ranges = ranges.iter().enumerate().peekable();

    for page_index in 0..page_offsets.len() - 1 {
        let page_range = page_offsets[page_index]..page_offsets[page_index + 1];

        match ranges.peek().cloned() {
            None => break,
            Some((idx, r)) if r.start < page_range.end => {
                let mut read = PageRead {
                    page_index,
                    write_offset,
                    ranges_slice: idx..idx + 1
                };

                while let Some((idx, r)) = ranges.peek().cloned() {
                    if r.start >= page_range.end {
                        break
                    }

                    let beg = std::cmp::max(page_range.start, r.start);
                    let end = std::cmp::min(page_range.end, r.end);
                    assert!(beg < end);

                    write_offset += (end - beg) as usize;
                    read.ranges_slice.end = idx + 1;

                    if end == r.end {
                        ranges.next();
                    }

                    if end == page_range.end {
                        break
                    }
                }

                reads.push(read);
            },
            _ => {}
        }
    }

    assert!(ranges.peek().is_none());

    Ok(reads)
}