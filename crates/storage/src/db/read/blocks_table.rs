use anyhow::{anyhow, bail};
use arrow::{
    array::{Array, AsArray},
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
    let numbers = {
        let col_idx = blocks_table.schema().index_of("number")?;
        let mut builder = AnyBuilder::new(blocks_table.schema().field(col_idx).data_type());
        blocks_table.create_column_reader(col_idx)?.read(&mut builder)?;
        builder.finish()
    };

    let maybe_row_idx = match numbers.data_type() {
        DataType::UInt32 => find_block_row(numbers.as_primitive::<UInt32Type>().values(), block_number as u32),
        DataType::UInt64 => find_block_row(numbers.as_primitive::<UInt64Type>().values(), block_number),
        ty => bail!("'number' column has unexpected data type - {}", ty)
    };

    let row_index = maybe_row_idx.ok_or_else(|| anyhow!("block {} was not found in the given table", block_number))?;

    let parent_hash = {
        let col_idx = blocks_table.schema().index_of("parent_hash")?;
        let mut builder = AnyBuilder::new(blocks_table.schema().field(col_idx).data_type());
        blocks_table
            .create_column_reader(col_idx)?
            .read_slice(&mut builder, row_index, 1)?;
        builder.finish()
    };

    Ok(match parent_hash.data_type() {
        DataType::Utf8 => parent_hash.as_string::<i32>().value(0).to_string(),
        ty => bail!("'parent_hash' column has unexpected data type - {}", ty)
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

/// Streams all `(block_number, hash)` pairs of a `blocks` table.
///
/// The `number` and `hash` columns are read in batches of [`BLOCK_HASH_BATCH_SIZE`]
/// rows so that peak memory stays `O(batch)` rather than `O(num_blocks)` even for
/// large compacted chunks. `visit` is called once per row.
///
/// Schema contract (mirrors [`get_parent_block_hash`]): `number` must be `UInt32`
/// or `UInt64` and `hash` must be `Utf8`; anything else is a hard error so that a
/// future schema change surfaces loudly instead of indexing garbage.
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
