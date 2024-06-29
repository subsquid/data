use anyhow::{anyhow, Context};
use arrow::array::{AsArray, PrimitiveArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, UInt64Type};

use crate::json::encoder::{Encoder, EncoderObject};
use crate::json::encoder::util::{json_close, make_object_prop};
use crate::json::exp::Exp;
use crate::plan::sort::{compute_order, Position};
use crate::primitives::{BlockNumber, Name};


pub(super) struct DataItem {
    prop: Vec<u8>,
    block_numbers: Vec<PrimitiveArray<UInt64Type>>,
    encoders: Vec<EncoderObject>,
    order: Vec<Position>,
    size: usize,
    pos: usize,
    is_block_header: bool
}


impl DataItem {
    pub(super) fn new(
        name: &str,
        key: &[Name],
        records: Vec<RecordBatch>,
        exp: &Exp
    ) -> anyhow::Result<Self> {
        let block_number_column = key[0];

        let block_numbers = records.iter().map(|b| -> anyhow::Result<_> {
            let column = b.column_by_name(block_number_column).ok_or_else(|| {
                anyhow!(
                    "key column '{}' is not present in '{}' output",
                    block_number_column,
                    name
                )
            })?;

            let numbers = arrow::compute::cast(column, &DataType::UInt64).with_context(|| {
                format!("failed to cast '{}' to block number", block_number_column)
            })?;

            Ok(numbers.as_primitive::<UInt64Type>().clone())
        }).collect::<anyhow::Result<Vec<_>>>()?;

        let order = compute_order(&records, key)?;

        let size = records.iter().map(|b| b.get_array_memory_size()).sum();

        let encoders = records.into_iter().map(|b| {
            let struct_array = StructArray::from(b);
            exp.eval(&struct_array)
        }).collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            prop: make_object_prop(name),
            block_numbers,
            encoders,
            order,
            size,
            pos: 0,
            is_block_header: key.len() == 1
        })
    }

    fn get_current_block(&self) -> Option<BlockNumber> {
        self.get_block_at(self.pos)
    }

    fn get_block_at(&self, idx: usize) -> Option<BlockNumber> {
        self.order.get(idx).map(|pos| {
            self.block_numbers[pos.0].value(pos.1)
        })
    }

    fn write_header(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"\"header\":");
        let pos = self.order[self.pos];
        self.pos += 1;
        self.encoders[pos.0].encode(pos.1, out);
    }

    fn write_items(&mut self, block_number: BlockNumber, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.prop);
        out.push(b'[');
        while self.pos < self.order.len() {
            let pos = self.order[self.pos];
            if self.block_numbers[pos.0].value(pos.1) != block_number {
                break
            }
            self.pos += 1;
            self.encoders[pos.0].encode(pos.1, out);
            out.push(b',')
        }
        json_close(b']', out)
    }
}


pub struct BlockWriter {
    items: Vec<DataItem>
}


impl BlockWriter {
    pub(super) fn new(items: Vec<DataItem>) -> Self {
        assert!(items.len() > 0 && items[0].is_block_header);
        Self {
            items
        }
    }

    pub fn data_size(&self) -> usize {
        self.items.iter().map(|i| i.size).sum::<usize>()
    }

    pub fn num_blocks(&self) -> usize {
        self.items[0].order.len()
    }

    pub fn first_block(&self) -> BlockNumber {
        self.items[0].get_block_at(0).unwrap()
    }

    pub fn last_block(&self) -> BlockNumber {
        self.items[0].get_block_at(self.num_blocks() - 1).unwrap()
    }

    pub fn print_summary(&self) {
        println!("blocks: {}", self.num_blocks());
        println!("size: {} kb", self.data_size() / 1024)
    }

    pub fn has_next_block(&self) -> bool {
        self.items[0].get_current_block().is_some()
    }

    pub fn write_next_block(&mut self, out: &mut Vec<u8>) {
        let block_number = self.items[0].get_current_block().unwrap();
        out.push(b'{');
        self.items[0].write_header(out);
        out.push(b',');
        for item in self.items.iter_mut().skip(1) {
            item.write_items(block_number, out);
            out.push(b',')
        }
        json_close(b'}', out)
    }
}