use anyhow::{anyhow, bail, ensure};
use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{DataType, UInt32Type, UInt64Type}
};
use sqd_array::{
    builder::{AnyBuilder, ArrayBuilder},
    reader::ArrayReader
};
use sqd_primitives::BlockNumber;

use crate::{kv::KvRead, table::read::TableReader};

pub fn get_parent_block_hash<S: KvRead + Sync>(
    blocks_table: &TableReader<S>,
    block_number: BlockNumber
) -> anyhow::Result<String> {
    let numbers = read_block_numbers(blocks_table)?;

    let maybe_row_idx = match numbers.data_type() {
        DataType::UInt32 => find_block_row(numbers.as_primitive::<UInt32Type>().values(), block_number as u32),
        DataType::UInt64 => find_block_row(numbers.as_primitive::<UInt64Type>().values(), block_number),
        ty => bail!("'number' column has unexpected data type - {}", ty)
    };

    let row_index = maybe_row_idx.ok_or_else(|| anyhow!("block {} was not found in the given table", block_number))?;

    read_string_cell(blocks_table, "parent_hash", row_index)
}

/// The hash `blocks_table` carries for `block_number`, `None` when it holds no such block. Unlike
/// [`get_parent_block_hash`] the match is exact: a skipped number is an absence, not the next up.
pub fn find_block_hash<S: KvRead + Sync>(
    blocks_table: &TableReader<S>,
    block_number: BlockNumber
) -> anyhow::Result<Option<String>> {
    let numbers = read_block_numbers(blocks_table)?;

    let maybe_row_idx = match numbers.data_type() {
        DataType::UInt32 => u32::try_from(block_number)
            .ok()
            .and_then(|n| find_exact_block_row(numbers.as_primitive::<UInt32Type>().values(), n)),
        DataType::UInt64 => find_exact_block_row(numbers.as_primitive::<UInt64Type>().values(), block_number),
        ty => bail!("'number' column has unexpected data type - {}", ty)
    };

    maybe_row_idx
        .map(|row_index| read_string_cell(blocks_table, "hash", row_index))
        .transpose()
}

fn read_block_numbers<S: KvRead + Sync>(blocks_table: &TableReader<S>) -> anyhow::Result<ArrayRef> {
    let col_idx = blocks_table.schema().index_of("number")?;
    let mut builder = AnyBuilder::new(blocks_table.schema().field(col_idx).data_type());
    blocks_table.create_column_reader(col_idx)?.read(&mut builder)?;
    Ok(builder.finish())
}

fn read_string_cell<S: KvRead + Sync>(
    blocks_table: &TableReader<S>,
    column: &str,
    row_index: usize
) -> anyhow::Result<String> {
    let col_idx = blocks_table.schema().index_of(column)?;
    let mut builder = AnyBuilder::new(blocks_table.schema().field(col_idx).data_type());
    blocks_table
        .create_column_reader(col_idx)?
        .read_slice(&mut builder, row_index, 1)?;
    let values = builder.finish();

    Ok(match values.data_type() {
        DataType::Utf8 => values.as_string::<i32>().value(0).to_string(),
        ty => bail!("'{}' column has unexpected data type - {}", column, ty)
    })
}

fn find_block_row<BN: Copy + Ord>(numbers: &[BN], block: BN) -> Option<usize> {
    numbers
        .iter()
        .copied()
        .enumerate()
        .filter(|e| e.1 >= block)
        .min_by_key(|e| e.1)
        .map(|e| e.0)
}

// Linear like `find_block_row`: nothing guarantees the column is sorted.
fn find_exact_block_row<BN: Copy + Eq>(numbers: &[BN], block: BN) -> Option<usize> {
    numbers.iter().position(|n| *n == block)
}

/// Streams all `(block number, hash)` pairs of a `blocks` table, reading the
/// columns in batches so peak memory stays `O(batch)` even for large compacted
/// chunks. `number` must be `UInt32`/`UInt64`, `hash` must be `Utf8`, and both
/// columns must be non-null; anything else is a hard error rather than
/// silently indexing incomplete data.
pub fn for_each_block_hash<S: KvRead + Sync>(
    blocks_table: &TableReader<S>,
    mut visit: impl FnMut(BlockNumber, &str) -> anyhow::Result<()>
) -> anyhow::Result<()> {
    const BLOCK_HASH_BATCH_SIZE: usize = 4096;

    let schema = blocks_table.schema();

    let number_idx = schema.index_of("number")?;
    let number_type = schema.field(number_idx).data_type().clone();
    match number_type {
        DataType::UInt32 | DataType::UInt64 => {}
        ref ty => bail!("'number' column has unexpected data type - {}", ty)
    }

    let hash_idx = schema.index_of("hash")?;
    let hash_type = schema.field(hash_idx).data_type().clone();
    if hash_type != DataType::Utf8 {
        bail!("'hash' column has unexpected data type - {}", hash_type)
    }

    let num_rows = blocks_table.num_rows();
    let mut number_reader = blocks_table.create_column_reader(number_idx)?;
    let mut hash_reader = blocks_table.create_column_reader(hash_idx)?;

    let mut offset = 0;
    while offset < num_rows {
        let len = std::cmp::min(BLOCK_HASH_BATCH_SIZE, num_rows - offset);

        let numbers = {
            let mut builder = AnyBuilder::new(&number_type);
            number_reader.read_slice(&mut builder, offset, len)?;
            builder.finish()
        };

        let hashes = {
            let mut builder = AnyBuilder::new(&hash_type);
            hash_reader.read_slice(&mut builder, offset, len)?;
            builder.finish()
        };

        ensure!(
            numbers.null_count() == 0 && hashes.null_count() == 0,
            "block number and hash columns must not contain nulls"
        );
        let hashes = hashes.as_string::<i32>();

        match numbers.data_type() {
            DataType::UInt32 => {
                let numbers = numbers.as_primitive::<UInt32Type>().values();
                for i in 0..len {
                    visit(numbers[i] as BlockNumber, hashes.value(i))?;
                }
            }
            DataType::UInt64 => {
                let numbers = numbers.as_primitive::<UInt64Type>().values();
                for i in 0..len {
                    visit(numbers[i], hashes.value(i))?;
                }
            }
            _ => unreachable!("'number' column type was validated above")
        }

        offset += len;
    }

    Ok(())
}
