use std::{cmp::min, collections::BTreeMap, ops::Range};

use arrow::datatypes::SchemaRef;
use sqd_primitives::BlockNumber;

use crate::db::ops::schema_merge::can_merge_schemas;

/// Decides, in reverse block order, whether a chunk joins the run of the newer chunk.
#[derive(Default)]
pub(super) struct RunBoundary {
    last_schema_map: BTreeMap<String, SchemaRef>,
    next_first_block: Option<BlockNumber>
}

impl RunBoundary {
    pub(super) fn joins_next(
        &mut self,
        first_block: BlockNumber,
        last_block: BlockNumber,
        tables: &[(&str, SchemaRef)]
    ) -> bool {
        let mut schema_compatible = true;
        let mut data_continuous = true;
        let is_last_schema_empty = self.last_schema_map.is_empty();

        if let Some(next_first_block) = self.next_first_block {
            data_continuous = last_block + 1 == next_first_block;
        }
        self.next_first_block = Some(first_block);

        if !is_last_schema_empty && tables.len() != self.last_schema_map.len() {
            self.last_schema_map = Default::default();
            schema_compatible = false;
        }
        for (name, schema) in tables {
            if let Some(last_schema) = self.last_schema_map.get(*name) {
                schema_compatible &= can_merge_schemas(schema, last_schema);
            } else if !is_last_schema_empty {
                schema_compatible = false;
            }
            self.last_schema_map.insert(name.to_string(), schema.clone());
        }

        schema_compatible && data_continuous
    }
}

/// Selects a merge from chunk sizes and boundaries supplied in reverse block order.
pub(super) struct MergePlanner {
    chunk_data_sizes: Vec<Vec<usize>>,
    max_chunk_size: usize,
    write_amplification_limit: f64,
    compaction_len_limit: usize
}

impl MergePlanner {
    pub(super) fn new(max_chunk_size: usize, write_amplification_limit: f64, compaction_len_limit: usize) -> Self {
        Self {
            chunk_data_sizes: vec![Vec::new()],
            max_chunk_size,
            write_amplification_limit,
            compaction_len_limit
        }
    }

    pub(super) fn push_reversed(&mut self, rows: usize, joins_next: bool) {
        if joins_next && rows < self.max_chunk_size {
            self.chunk_data_sizes.last_mut().unwrap().push(rows);
        } else {
            self.chunk_data_sizes.push(vec![rows]);
        }
        if rows >= self.max_chunk_size {
            self.chunk_data_sizes.push(Vec::new());
        }
    }

    pub(super) fn finish(mut self) -> Range<usize> {
        self.chunk_data_sizes.iter_mut().for_each(|v| v.reverse());
        self.chunk_data_sizes.reverse();
        let open_tail = self.chunk_data_sizes.len() - 1;
        let mut skip_chunks = 0;

        for (idx, continuous_run) in self.chunk_data_sizes.iter().enumerate() {
            if continuous_run.len() < 2 {
                skip_chunks += continuous_run.len();
                continue;
            }

            if idx < open_tail {
                // Closed runs cannot receive new chunks, so finish them without waiting
                // for the write amplification threshold.
                let take_chunks = self.closed_run_merge_len(continuous_run);
                return skip_chunks..skip_chunks + take_chunks;
            }

            // The open tail must respect the write amplification threshold.
            let range = Self::find_range(
                continuous_run,
                0,
                continuous_run.len(),
                self.max_chunk_size,
                self.write_amplification_limit,
                self.compaction_len_limit
            );
            if let Some(range) = range {
                return skip_chunks + range.start..skip_chunks + range.end;
            }
            skip_chunks += continuous_run.len();
        }

        skip_chunks..skip_chunks
    }

    /// Returns the shortest prefix that reaches the maximum chunk size, or the whole run.
    fn closed_run_merge_len(&self, run: &[usize]) -> usize {
        let mut rows = 0;
        for (idx, &size) in run.iter().enumerate() {
            rows += size;
            if rows >= self.max_chunk_size {
                return idx + 1;
            }
        }
        run.len()
    }

    fn score_merge(chunk_sizes: &[usize], wa_limit: f64) -> usize {
        let max_size = *chunk_sizes.iter().max().unwrap_or(&0) as f64;
        let full_size = chunk_sizes.iter().sum();
        if max_size == 0.0 {
            return 1;
        }

        let wa = full_size as f64 / max_size;
        if wa < wa_limit {
            return 0;
        }
        full_size
    }

    fn find_range(
        chunk_sizes: &[usize],
        start: usize,
        end: usize,
        chunk_size_threshold: usize,
        wa_threshold: f64,
        len_limit: usize
    ) -> Option<Range<usize>> {
        if start + 1 >= end {
            return None;
        }

        let left = start;
        let mut right = left + 1;
        let right_limit = min(end, start + len_limit);
        loop {
            let score = Self::score_merge(&chunk_sizes[left..right], wa_threshold);
            if score >= chunk_size_threshold {
                return Some(left..right);
            }
            if right < right_limit {
                right += 1;
                continue;
            }
            if score > 0 {
                return Some(left..right);
            }
            break;
        }

        let max_el = chunk_sizes[left..right].iter().max().unwrap();
        let max_idx = chunk_sizes[left..right]
            .iter()
            .position(|element| element == max_el)
            .unwrap();
        let left_range = Self::find_range(chunk_sizes, start, start + max_idx, *max_el, wa_threshold, len_limit);
        if left_range.is_some() {
            return left_range;
        }
        Self::find_range(chunk_sizes, start + max_idx + 1, end, *max_el, wa_threshold, len_limit)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use proptest::{
        prelude::{prop, ProptestConfig},
        prop_oneof, proptest
    };
    use sqd_primitives::BlockNumber;

    use super::{MergePlanner, RunBoundary};
    use crate::db::ops::{compaction::COMPACTION_LEN_LIMIT, schema_merge::can_merge_schemas};

    /// What separates a group from the group before it.
    #[derive(Clone, Copy, Debug)]
    enum GroupStart {
        SchemaChange,
        Gap,
        SchemaChangeAndGap
    }

    const GROUP_STARTS: [GroupStart; 3] = [
        GroupStart::SchemaChange,
        GroupStart::Gap,
        GroupStart::SchemaChangeAndGap
    ];

    /// Holds one row per block, so `rows` is also the block count.
    #[derive(Clone, Debug)]
    struct TestChunk {
        first_block: BlockNumber,
        rows: usize,
        group: usize,
        schema: SchemaRef
    }

    impl TestChunk {
        fn last_block(&self) -> BlockNumber {
            self.first_block + self.rows as BlockNumber - 1
        }
    }

    fn schema(first: DataType, second: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("first", first, false),
            Field::new("second", second, false),
        ]))
    }

    fn build_chunks(groups: &[Vec<usize>], starts: &[GroupStart]) -> Vec<TestChunk> {
        let schemas = [
            schema(DataType::UInt32, DataType::Int32),
            schema(DataType::Int32, DataType::UInt32)
        ];
        assert!(!can_merge_schemas(&schemas[0], &schemas[1]));

        let mut chunks = Vec::new();
        let mut schema_idx = 0;
        let mut next_block = 0;
        for (group, sizes) in groups.iter().enumerate() {
            if group > 0 {
                match starts[group - 1] {
                    GroupStart::SchemaChange => schema_idx ^= 1,
                    GroupStart::Gap => next_block += 1,
                    GroupStart::SchemaChangeAndGap => {
                        schema_idx ^= 1;
                        next_block += 1;
                    }
                }
            }

            for &rows in sizes {
                chunks.push(TestChunk {
                    first_block: next_block,
                    rows,
                    group,
                    schema: schemas[schema_idx].clone()
                });
                next_block += rows as BlockNumber;
            }
        }

        chunks
    }

    fn sizes(rows: &[usize]) -> Vec<TestChunk> {
        build_chunks(&[rows.to_vec()], &[])
    }

    fn plan(chunks: &[TestChunk], max_rows: usize, wa: f64, limit: usize) -> Range<usize> {
        let mut planner = MergePlanner::new(max_rows, wa, limit);
        let mut boundary = RunBoundary::default();
        let mut next_group = None;

        for chunk in chunks.iter().rev() {
            let tables = [("block", chunk.schema.clone())];
            let joins_next = boundary.joins_next(chunk.first_block, chunk.last_block(), &tables);
            let same_group = next_group.is_none_or(|group| group == chunk.group);
            assert_eq!(
                joins_next, same_group,
                "wrong run boundary at block {}",
                chunk.first_block
            );

            planner.push_reversed(chunk.rows, joins_next);
            next_group = Some(chunk.group);
        }

        planner.finish()
    }

    #[test]
    fn joins_only_continuous_chunks_with_mergeable_schemas() {
        let a = schema(DataType::UInt32, DataType::UInt16);
        let a_compatible = schema(DataType::UInt16, DataType::UInt32);
        let b = schema(DataType::Int32, DataType::UInt32);
        assert!(can_merge_schemas(&a, &a_compatible));
        assert!(!can_merge_schemas(&a, &b));

        let mut boundary = RunBoundary::default();
        let mut joins = |first_block: BlockNumber, last_block: BlockNumber, schema: &SchemaRef| {
            boundary.joins_next(first_block, last_block, &[("block", schema.clone())])
        };

        assert!(joins(50, 59, &a));
        assert!(joins(40, 49, &a_compatible));
        assert!(!joins(30, 38, &a), "block 39 is absent");
        assert!(!joins(20, 29, &b));
        assert!(!joins(10, 19, &a));
        assert!(joins(0, 9, &a));
    }

    #[test]
    fn leaves_a_dominant_chunk_until_enough_new_rows_arrive() {
        assert!(plan(&sizes(&[90, 10]), 100, 1.25, 50).is_empty());
        assert_eq!(plan(&sizes(&[80, 20]), 100, 1.25, 50), 0..2);
        assert!(plan(&sizes(&[80, 20]), 100, 1.50, 50).is_empty());
    }

    #[test]
    fn finds_small_chunks_on_either_side_of_a_dominant_chunk() {
        assert_eq!(plan(&sizes(&[4, 4, 90]), 100, 1.25, 50), 0..2);
        assert_eq!(plan(&sizes(&[90, 4, 4]), 100, 1.25, 50), 1..3);
    }

    #[test]
    fn finishes_a_closed_group_without_waiting_for_more_rows() {
        for start in GROUP_STARTS {
            let chunks = build_chunks(&[vec![19, 8, 1], vec![10]], &[start]);
            assert_eq!(plan(&chunks, 100, 1.50, 50), 0..3);
        }
        assert!(plan(&sizes(&[19, 8, 1]), 100, 1.50, 50).is_empty());
    }

    #[test]
    fn skips_large_chunks_and_respects_the_active_run_length_limit() {
        assert_eq!(plan(&sizes(&[100, 10, 10, 10]), 100, 1.25, 2), 1..3);
        assert_eq!(plan(&sizes(&[10, 10, 100, 10]), 100, 1.25, 50), 0..2);
        assert!(plan(&[], 100, 1.25, 50).is_empty());
        assert!(plan(&sizes(&[10]), 100, 1.25, 50).is_empty());
    }

    fn compact_once(chunks: &mut Vec<TestChunk>, max_rows: usize, wa: f64, limit: usize) -> bool {
        let range = plan(chunks, max_rows, wa, limit);
        assert!(range.end <= chunks.len());
        if range.len() < 2 {
            return false;
        }

        let selected = &chunks[range.clone()];
        let first = selected[0].clone();
        assert!(selected.iter().all(|chunk| chunk.group == first.group));
        assert!(selected.iter().all(|chunk| chunk.rows < max_rows));

        let rows = selected.iter().map(|chunk| chunk.rows).sum();
        let largest = selected.iter().map(|chunk| chunk.rows).max().unwrap();
        // Only the open tail waits for new data; closed runs can finish immediately.
        let in_open_tail = chunks[range.end..]
            .iter()
            .all(|chunk| chunk.group == first.group && chunk.rows < max_rows);
        if in_open_tail {
            assert!(rows as f64 >= wa * largest as f64);
            assert!(range.len() <= limit);
        }

        chunks.splice(range, [TestChunk { rows, ..first }]);
        true
    }

    fn check_scenario(
        groups: &[Vec<usize>],
        starts: &[GroupStart],
        max_rows: usize,
        wa: f64,
        limit: usize,
        online: bool
    ) {
        let mut chunks = Vec::new();
        for chunk in build_chunks(groups, starts) {
            chunks.push(chunk);
            if online {
                compact_once(&mut chunks, max_rows, wa, limit);
            }
        }

        // Every successful merge removes at least one chunk, so this also bounds convergence.
        let budget = chunks.len();
        for _ in 0..budget {
            if !compact_once(&mut chunks, max_rows, wa, limit) {
                break;
            }
        }
        assert!(!compact_once(&mut chunks, max_rows, wa, limit));

        for (group, original) in groups.iter().enumerate() {
            let sizes: Vec<_> = chunks
                .iter()
                .filter(|chunk| chunk.group == group)
                .map(|chunk| chunk.rows)
                .collect();
            assert_eq!(sizes.iter().sum::<usize>(), original.iter().sum::<usize>());

            let mut runs: Vec<_> = sizes.split(|&size| size >= max_rows).collect();
            let is_last_group = group + 1 == groups.len();
            let open_tail = if is_last_group { runs.pop() } else { None };
            assert!(
                runs.iter().all(|run| run.len() < 2),
                "closed run left unmerged: {sizes:?}"
            );

            // Past the length limit the planner searches bounded windows, so a longer
            // tail can keep a mergeable range.
            let Some(tail) = open_tail.filter(|tail| tail.len() <= limit) else {
                continue;
            };
            for start in 0..tail.len() {
                let mut rows = 0;
                let mut largest = 0;
                for &size in &tail[start..] {
                    rows += size;
                    largest = largest.max(size);
                    let ratio = rows as f64 / largest as f64;
                    assert!(ratio < wa, "mergeable range left in the open tail: {tail:?}");
                }
            }
        }
    }

    #[test]
    fn compaction_plan_converges_for_irregular_chunks() {
        let rows = prop_oneof![9 => 1..20usize, 1 => 100..200usize];
        let groups = prop::collection::vec(prop::collection::vec(rows, 1..60), 1..5);
        let starts = prop::collection::vec(prop::sample::select(GROUP_STARTS.to_vec()), 4);
        proptest!(ProptestConfig::with_cases(200), |(groups in groups, starts in starts)| {
            for (max_rows, wa, limit) in [
                (100, 1.25, COMPACTION_LEN_LIMIT),
                (150, 1.25, COMPACTION_LEN_LIMIT),
                (100, 1.50, COMPACTION_LEN_LIMIT),
                (100, 1.25, 3),
                (150, 1.50, 5)
            ] {
                for online in [false, true] {
                    check_scenario(&groups, &starts, max_rows, wa, limit, online);
                }
            }
        });
    }
}
