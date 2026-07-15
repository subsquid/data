use anyhow::{bail, ensure};
use arrow::{
    array::{Array, AsArray},
    datatypes::{DataType, UInt16Type, UInt32Type, UInt64Type}
};
use sqd_array::{
    builder::{AnyBuilder, ArrayBuilder},
    reader::ArrayReader
};
use sqd_primitives::{BlockNumber, ItemIndex};

use crate::{kv::KvRead, table::read::TableReader};

/// Streams `(block number, transaction index, hash)` from an EVM transactions
/// table in bounded batches. Required position/hash columns must contain no
/// nulls, and each transaction index must fit [`ItemIndex`]; accepting an
/// invalid row would create an index entry that cannot be sound.
pub fn for_each_transaction_hash<S: KvRead + Sync>(
    transactions_table: &TableReader<S>,
    mut visit: impl FnMut(BlockNumber, ItemIndex, &str) -> anyhow::Result<()>
) -> anyhow::Result<()> {
    const TRANSACTION_HASH_BATCH_SIZE: usize = 4096;

    let schema = transactions_table.schema();

    let block_number_idx = schema.index_of("block_number")?;
    let block_number_type = schema.field(block_number_idx).data_type().clone();
    match block_number_type {
        DataType::UInt32 | DataType::UInt64 => {}
        ref ty => bail!("'block_number' column has unexpected data type - {}", ty)
    }

    let transaction_index_idx = schema.index_of("transaction_index")?;
    let transaction_index_type = schema.field(transaction_index_idx).data_type().clone();
    match transaction_index_type {
        DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {}
        ref ty => bail!("'transaction_index' column has unexpected data type - {}", ty)
    }

    let hash_idx = schema.index_of("hash")?;
    let hash_type = schema.field(hash_idx).data_type().clone();
    if hash_type != DataType::Utf8 {
        bail!("'hash' column has unexpected data type - {}", hash_type)
    }

    let num_rows = transactions_table.num_rows();
    let mut block_number_reader = transactions_table.create_column_reader(block_number_idx)?;
    let mut transaction_index_reader = transactions_table.create_column_reader(transaction_index_idx)?;
    let mut hash_reader = transactions_table.create_column_reader(hash_idx)?;

    let mut offset = 0;
    while offset < num_rows {
        let len = std::cmp::min(TRANSACTION_HASH_BATCH_SIZE, num_rows - offset);

        let block_numbers = {
            let mut builder = AnyBuilder::new(&block_number_type);
            block_number_reader.read_slice(&mut builder, offset, len)?;
            builder.finish()
        };
        let transaction_indexes = {
            let mut builder = AnyBuilder::new(&transaction_index_type);
            transaction_index_reader.read_slice(&mut builder, offset, len)?;
            builder.finish()
        };
        let hashes = {
            let mut builder = AnyBuilder::new(&hash_type);
            hash_reader.read_slice(&mut builder, offset, len)?;
            builder.finish()
        };

        ensure!(
            block_numbers.null_count() == 0 && transaction_indexes.null_count() == 0 && hashes.null_count() == 0,
            "transaction position and hash columns must not contain nulls"
        );

        let transaction_indexes = match transaction_indexes.data_type() {
            DataType::UInt16 => TransactionIndexes::UInt16(transaction_indexes.as_primitive::<UInt16Type>().values()),
            DataType::UInt32 => TransactionIndexes::UInt32(transaction_indexes.as_primitive::<UInt32Type>().values()),
            DataType::UInt64 => TransactionIndexes::UInt64(transaction_indexes.as_primitive::<UInt64Type>().values()),
            _ => unreachable!("'transaction_index' column type was validated above")
        };
        let hashes = hashes.as_string::<i32>();

        match block_numbers.data_type() {
            DataType::UInt32 => {
                let block_numbers = block_numbers.as_primitive::<UInt32Type>().values();
                for i in 0..len {
                    visit(
                        BlockNumber::from(block_numbers[i]),
                        transaction_indexes.value(i)?,
                        hashes.value(i)
                    )?;
                }
            }
            DataType::UInt64 => {
                let block_numbers = block_numbers.as_primitive::<UInt64Type>().values();
                for i in 0..len {
                    visit(block_numbers[i], transaction_indexes.value(i)?, hashes.value(i))?;
                }
            }
            _ => unreachable!("'block_number' column type was validated above")
        }

        offset += len;
    }

    Ok(())
}

enum TransactionIndexes<'a> {
    UInt16(&'a [u16]),
    UInt32(&'a [u32]),
    UInt64(&'a [u64])
}

impl TransactionIndexes<'_> {
    fn value(&self, index: usize) -> anyhow::Result<ItemIndex> {
        match self {
            Self::UInt16(values) => Ok(ItemIndex::from(values[index])),
            Self::UInt32(values) => Ok(values[index]),
            Self::UInt64(values) => ItemIndex::try_from(values[index])
                .map_err(|_| anyhow::anyhow!("transaction_index {} does not fit u32", values[index]))
        }
    }
}
