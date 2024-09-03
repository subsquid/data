use std::ops::Range;
use sqd_primitives::range::RangeList;


struct PageRead {
    page_index: usize,
    write_slice: Range<usize>,
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
    pub fn new(page_offsets: &'a [u32], ranges: Option<&'a RangeList<u32>>) -> Self {
        if let Some(_ranges) = ranges {
            todo!()
        } else {
            Pagination::All {
                page_offsets,
                ranges: [page_offsets[0]..page_offsets.last().unwrap().clone()]
            }
        }
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
        let offsets = *match self {
            Pagination::Ranged { page_offsets, .. } => page_offsets,
            Pagination::All { page_offsets, .. } => page_offsets
        };
        offsets[seq] as usize..offsets[seq + 1] as usize
    }

    pub fn page_write_offset(&self, seq: usize) -> usize {
        match self {
            Pagination::Ranged { pages, .. } => {
                pages[seq].write_slice.start
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