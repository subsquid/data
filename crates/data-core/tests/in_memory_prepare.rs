//! Differential tests: the in-memory prepare path must be observationally identical to the
//! temp-file spill path — same prepared schemas (incl. chunk-wide downcast), same rows in
//! the same order, for sorted/plain tables, strings, lists, nulls, and empty tables.

use sqd_array::{
    builder::{ListBuilder, StringBuilder, UInt32Builder, UInt64Builder},
    slice::Slice
};
use sqd_data_core::{chunk_builder, table_builder, PreparedChunk};

type TagsBuilder = ListBuilder<StringBuilder>;

table_builder! {
    SortedBuilder {
        block_number: UInt64Builder,
        item_index: UInt32Builder,
        name: StringBuilder,
        tags: TagsBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["item_index"];
        d.sort_key = vec!["name", "block_number", "item_index"];
    }
}

table_builder! {
    PlainBuilder {
        block_number: UInt64Builder,
        value: StringBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
    }
}

chunk_builder! {
    TestChunkBuilder {
        sorted: SortedBuilder,
        plain: PlainBuilder,
    }
}

/// Unsorted input with duplicate names and shuffled block numbers; the sort-key tail
/// (`item_index`) stays unique so both paths agree on a total order.
fn populate(b: &mut TestChunkBuilder, rows: u64, base_block: u64) {
    let mut x = 7u64;
    for i in 0..rows {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        b.sorted.block_number.append(base_block + x % rows);
        b.sorted.item_index.append(i as u32);
        match i % 5 {
            0 => b.sorted.name.append(""),
            1 => b.sorted.name.append_null(),
            _ => b.sorted.name.append(&format!("name-{}", x % 7))
        }
        if i % 4 == 3 {
            b.sorted.tags.append_null();
        } else {
            for t in 0..i % 3 {
                b.sorted.tags.values().append(&format!("tag-{t}-{}", x % 5));
            }
            b.sorted.tags.append();
        }
        b.plain.block_number.append(base_block + i);
        b.plain.value.append(&format!("v{i}"));
    }
}

fn disk_prepare(b: &TestChunkBuilder) -> PreparedChunk {
    let mut p = b.new_chunk_processor().unwrap();
    b.submit_to_processor(&mut p).unwrap();
    p.finish().unwrap()
}

fn assert_chunks_equal(mut disk: PreparedChunk, mut mem: PreparedChunk) {
    assert_eq!(disk.len(), mem.len());
    for ((dn, d), (mn, m)) in disk.iter_mut().zip(mem.iter_mut()) {
        assert_eq!(dn, mn);
        assert_eq!(d.schema(), m.schema(), "schema mismatch for table {dn}");
        assert_eq!(d.num_rows(), m.num_rows(), "row count mismatch for table {dn}");
        let rows = d.num_rows();
        assert_eq!(
            d.read_record_batch(0, rows).unwrap(),
            m.read_record_batch(0, rows).unwrap(),
            "full read mismatch for table {dn}"
        );
        for (offset, len) in [
            (0, rows.min(1)),
            (rows / 2, rows - rows / 2),
            (rows.saturating_sub(1), rows.min(1))
        ] {
            assert_eq!(
                d.read_record_batch(offset, len).unwrap(),
                m.read_record_batch(offset, len).unwrap(),
                "window ({offset}, {len}) mismatch for table {dn}"
            );
        }
    }
}

#[test]
fn in_memory_prepare_matches_spill_path() {
    let mut b = TestChunkBuilder::new();
    populate(&mut b, 57, 1_000);
    let disk = disk_prepare(&b);
    let mem = b.prepare_in_memory().unwrap();
    assert_chunks_equal(disk, mem);
    assert_eq!(b.max_num_rows(), 0, "prepare_in_memory must clear the builder");
}

#[test]
fn duplicate_sort_keys_match_spill_path() {
    let mut b = TestChunkBuilder::new();
    for i in 0..1_003u64 {
        b.sorted.block_number.append(1_000 + (i.wrapping_mul(17) % 5));
        b.sorted.item_index.append((i.wrapping_mul(7) % 11) as u32);
        b.sorted.name.append(&format!("key-{}", i.wrapping_mul(13) % 3));
        b.sorted.tags.values().append(&format!("payload-{i}"));
        b.sorted.tags.append();

        b.plain.block_number.append(1_000);
        b.plain.value.append(&format!("value-{i}"));
    }

    let disk = disk_prepare(&b);
    let mem = b.prepare_in_memory().unwrap();
    assert_chunks_equal(disk, mem);
}

#[test]
fn small_block_numbers_downcast_identically() {
    // max block number fits u32 → both paths must downcast the u64 column to UInt32,
    // exercising the cast-on-read path
    let mut b = TestChunkBuilder::new();
    populate(&mut b, 23, 1_000);
    let disk = disk_prepare(&b);
    let mem = b.prepare_in_memory().unwrap();
    for t in disk.values() {
        let f = t.schema().field_with_name("block_number").unwrap().data_type().clone();
        assert_eq!(f, arrow::datatypes::DataType::UInt32);
    }
    assert_chunks_equal(disk, mem);
}

#[test]
fn shared_downcast_is_consistent_across_tables() {
    // huge block numbers only in `sorted` — `plain` must still widen to UInt64 in both
    // paths, because the downcast is chunk-wide
    let mut b = TestChunkBuilder::new();
    populate(&mut b, 11, u32::MAX as u64 + 100);
    let disk = disk_prepare(&b);
    let mem = b.prepare_in_memory().unwrap();
    for t in mem.values() {
        let f = t.schema().field_with_name("block_number").unwrap().data_type().clone();
        assert_eq!(f, arrow::datatypes::DataType::UInt64);
    }
    assert_chunks_equal(disk, mem);
}

#[test]
fn empty_chunk() {
    let mut b = TestChunkBuilder::new();
    let disk = disk_prepare(&b);
    let mem = b.prepare_in_memory().unwrap();
    assert_chunks_equal(disk, mem);
}

table_builder! {
    TiedBuilder {
        name: StringBuilder,
        id: UInt64Builder,
    }

    description(d) {
        d.sort_key = vec!["name"];
    }
}

chunk_builder! {
    TiedChunkBuilder {
        tied: TiedBuilder,
    }
}

/// Duplicate full sort keys: intra-group row order is unspecified (unstable sort applied a
/// different number of times per path), but groups must hold the same rows.
#[test]
fn duplicate_sort_keys_keep_group_contents() {
    use arrow::{array::AsArray, datatypes::UInt64Type};

    let mut b = TiedChunkBuilder::new();
    for i in 0..40u64 {
        b.tied.name.append(&format!("k{}", i % 4));
        b.tied.id.append(i);
    }

    let mut p = b.new_chunk_processor().unwrap();
    b.submit_to_processor(&mut p).unwrap();
    let mut disk = p.finish().unwrap();
    let mut mem = b.prepare_in_memory().unwrap();

    let rows = |chunk: &mut PreparedChunk| -> Vec<(String, u64)> {
        let t = chunk.get_mut("tied").unwrap();
        let batch = t.read_record_batch(0, t.num_rows()).unwrap();
        let names = batch.column(0).as_string::<i32>();
        let ids = batch.column(1).as_primitive::<UInt64Type>();
        (0..batch.num_rows())
            .map(|i| (names.value(i).to_string(), ids.value(i)))
            .collect()
    };

    let d = rows(&mut disk);
    let m = rows(&mut mem);

    assert!(d.windows(2).all(|w| w[0].0 <= w[1].0), "disk output not key-sorted");
    assert!(m.windows(2).all(|w| w[0].0 <= w[1].0), "mem output not key-sorted");

    let mut ds = d.clone();
    let mut ms = m.clone();
    ds.sort();
    ms.sort();
    assert_eq!(ds, ms, "row multisets differ");
}

#[test]
fn multiple_spill_batches_match_one_in_memory_batch() {
    let mut disk_builder = TestChunkBuilder::new();
    let mut processor = disk_builder.new_chunk_processor().unwrap();

    populate(&mut disk_builder, 31, 1_000);
    disk_builder.submit_to_processor(&mut processor).unwrap();
    disk_builder.clear();

    populate(&mut disk_builder, 29, 2_000);
    disk_builder.submit_to_processor(&mut processor).unwrap();
    disk_builder.clear();
    let disk = processor.finish().unwrap();

    let mut mem_builder = TestChunkBuilder::new();
    populate(&mut mem_builder, 31, 1_000);
    populate(&mut mem_builder, 29, 2_000);
    let mem = mem_builder.prepare_in_memory().unwrap();

    assert_chunks_equal(disk, mem);
}

table_builder! {
    ValidBuilder {
        value: UInt64Builder,
    }

    description(_d) {}
}

table_builder! {
    InvalidBuilder {
        value: UInt64Builder,
    }

    description(d) {
        d.sort_key = vec!["column_that_does_not_exist"];
    }
}

chunk_builder! {
    FailingChunkBuilder {
        valid: ValidBuilder,
        invalid: InvalidBuilder,
    }
}

#[test]
fn preparation_error_does_not_clear_buffered_rows() {
    let mut b = FailingChunkBuilder::new();
    b.valid.value.append(10);
    b.invalid.value.append(20);

    let err = b.prepare_in_memory().err().expect("invalid sort key must fail");
    assert!(err.to_string().contains("column_that_does_not_exist"));
    assert_eq!(b.max_num_rows(), 1, "failed prepare discarded buffered rows");

    let slices = b.as_slice_map();
    assert_eq!(slices["valid"].len(), 1);
    assert_eq!(slices["invalid"].len(), 1);
}

#[test]
fn in_memory_prepared_table_does_not_support_reuse() {
    let mut b = TestChunkBuilder::new();
    populate(&mut b, 3, 10);
    let mut mem = b.prepare_in_memory().unwrap();
    let (_, table) = mem.pop_first().unwrap();
    assert!(table.into_processor().is_err());
}
