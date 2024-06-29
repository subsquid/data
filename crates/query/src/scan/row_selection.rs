use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use std::cmp::Ordering;
use parquet::file::metadata::RowGroupMetaData;
use crate::scan::row_range::RowRange;
use crate::primitives::RowIndex;


pub fn from_row_ranges<I>(ranges: I) -> RowSelection
    where I: IntoIterator<Item = RowRange>
{
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut last_end = 0i64;
    for range in ranges {
        let len = (range.end - range.start) as usize;
        if len == 0 {
            continue;
        }

        match range.start.cmp(&last_end) {
            Ordering::Equal => match selectors.last_mut() {
                Some(last) => last.row_count = last.row_count.checked_add(len).unwrap(),
                None => selectors.push(RowSelector::select(len)),
            },
            Ordering::Greater => {
                selectors.push(RowSelector::skip((range.start - last_end) as usize));
                selectors.push(RowSelector::select(len))
            }
            Ordering::Less => panic!("out of order"),
        }
        last_end = range.end;
    }

    RowSelection::from(selectors)
}


pub fn from_row_indexes<I>(indexes: I) -> RowSelection
    where I: IntoIterator<Item = RowIndex>
{
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut it = indexes.into_iter();

    if let Some(idx) = it.next() {
        if idx > 0 {
            selectors.push(RowSelector::skip(idx as usize))
        }
        let mut row_count = 1;
        let mut pos = idx;
        while let Some(idx) = it.next() {
            assert!(idx > pos);
            if idx - pos == 1 {
                pos = idx;
                row_count += 1;
            } else {
                selectors.push(RowSelector::select(row_count));
                selectors.push(RowSelector::skip((idx - pos - 1) as usize));
                pos = idx;
                row_count = 1;
            }
        }
        selectors.push(RowSelector::select(row_count));
    }

    RowSelection::from(selectors)
}


pub fn intersect(a: Option<RowSelection>, b: Option<RowSelection>) -> Option<RowSelection> {
    match (a, b) {
        (Some(a), Some(b)) => Some(intersect_impl(&a, &b)),
        (a, None) => a,
        (None, b) => b
    }
}


// Stolen from parquet crate, except we stop selection
// when either left or right selection stops.
fn intersect_impl(left: &RowSelection, right: &RowSelection) -> RowSelection {
    let mut l_iter = left.iter().copied().peekable();
    let mut r_iter = right.iter().copied().peekable();

    let iter = std::iter::from_fn(move || {
        loop {
            let l = l_iter.peek_mut();
            let r = r_iter.peek_mut();

            match (l, r) {
                (Some(a), _) if a.row_count == 0 => {
                    l_iter.next().unwrap();
                }
                (_, Some(b)) if b.row_count == 0 => {
                    r_iter.next().unwrap();
                }
                (Some(l), Some(r)) => {
                    return match (l.skip, r.skip) {
                        // Keep both ranges
                        (false, false) => {
                            if l.row_count < r.row_count {
                                r.row_count -= l.row_count;
                                l_iter.next()
                            } else {
                                l.row_count -= r.row_count;
                                r_iter.next()
                            }
                        }
                        // skip at least one
                        _ => {
                            if l.row_count < r.row_count {
                                let skip = l.row_count;
                                r.row_count -= l.row_count;
                                l_iter.next();
                                Some(RowSelector::skip(skip))
                            } else {
                                let skip = r.row_count;
                                l.row_count -= skip;
                                r_iter.next();
                                Some(RowSelector::skip(skip))
                            }
                        }
                    };
                }
                (Some(_), None) => return None,
                (None, Some(_)) => return None,
                (None, None) => return None,
            }
        }
    });

    RowSelection::from_iter(iter)
}


pub(super) fn select_row_groups(
    row_groups: &[RowGroupMetaData],
    selection: &RowSelection
) -> Vec<(usize, Option<RowSelection>)> {
    let mut result = Vec::new();
    let mut idx = 0;
    let mut left = 0;
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut included = false;

    for sel in selection.iter() {
        let mut count = sel.row_count;
        while count > 0 {
            while left == 0 {
                if included {
                    push_row_group(&mut result, idx - 1, selectors);
                }
                left = row_groups[idx].num_rows() as usize;
                idx += 1;
                included = false;
                selectors = Vec::new();
            }
            let step = count.min(left);
            count -= step;
            left -= step;
            selectors.push(RowSelector {
                row_count: step,
                skip: sel.skip
            });
            included = included || !sel.skip;
        }
    }

    if included {
        push_row_group(&mut result, idx - 1, selectors);
    }

    result
}


fn push_row_group(
    row_group_selection: &mut Vec<(usize, Option<RowSelection>)>,
    idx: usize,
    mut selectors: Vec<RowSelector>
) {
    if selectors.last().map_or(false, |s| s.skip) {
        selectors.pop();
    }
    let selection = RowSelection::from(selectors);
    row_group_selection.push((idx, Some(selection)));
}