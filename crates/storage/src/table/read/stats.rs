use anyhow::ensure;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, ScalarBuffer};
use crate::table::util::{add_null_mask, read_array_from_buffers, validate_offsets, BufferReader, FromBufferReader};


#[derive(Clone)]
pub struct Stats {
    pub offsets: ScalarBuffer<u32>,
    pub min: ArrayRef,
    pub max: ArrayRef
}


impl Stats {
    pub(super) fn read(data: &[u8], data_type: &DataType) -> anyhow::Result<Self> {
        let mut reader = BufferReader::new(data);

        let offsets = {
            let buf = ScalarBuffer::<u32>::from_buffer_reader(&mut reader)?;
            validate_offsets(&buf)?;
            buf
        };

        let len = offsets.len() - 1;

        let nulls = Option::<NullBuffer>::from_buffer_reader(&mut reader)?;
        if let Some(nulls) = nulls.as_ref() {
            ensure!(nulls.len() == len, "null mask length does not match offsets array");
        }

        let mut min = read_array_from_buffers(&mut reader, data_type)?;
        ensure!(min.len() == len, "min array length does not match offsets array");

        let mut max = read_array_from_buffers(&mut reader, data_type)?;
        ensure!(max.len() == len, "max array length does not match offsets array");

        reader.ensure_eof()?;

        if let Some(nulls) = nulls.as_ref() {
            min = add_null_mask(&min, nulls.clone());
            max = add_null_mask(&max, nulls.clone());
        }
        
        Ok(Self {
            offsets,
            min,
            max
        })
    }
}