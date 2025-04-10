use crate::kv::KvRead;
use crate::table::read::TableReader;
use anyhow::{anyhow, bail};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, UInt32Type, UInt64Type};
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::reader::ArrayReader;
use sqd_primitives::BlockNumber;


pub fn get_parent_block_hash<S: KvRead + Sync>(
    blocks_table: &TableReader<S>,
    block_number: BlockNumber
) -> anyhow::Result<String> 
{
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
    
    let row_index = maybe_row_idx.ok_or_else(|| {
        anyhow!("block {} was not found in the given table", block_number)
    })?;
    
    let parent_hash = {
        let col_idx = blocks_table.schema().index_of("parent_hash")?;
        let mut builder = AnyBuilder::new(blocks_table.schema().field(col_idx).data_type());
        blocks_table.create_column_reader(col_idx)?.read_slice(&mut builder, row_index, 1)?;
        builder.finish()
    };
    
    Ok(match parent_hash.data_type() {
        DataType::Utf8 => {
            parent_hash.as_string::<i32>().value(0).to_string()      
        },
        ty => bail!("'parent_hash' column has unexpected data type - {}", ty)
    })
}


fn find_block_row<BN: Copy + Ord>(numbers: &[BN], block: BN) -> Option<usize> {
    numbers.iter()
        .copied()
        .enumerate()
        .filter(|e| e.1 >= block)
        .min_by_key(|e| e.1)
        .map(|e| e.0)
}