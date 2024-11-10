use super::Stats;
use anyhow::{anyhow, ensure};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow_buffer::{MutableBuffer, OffsetBuffer, ScalarBuffer};
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::io::dense::{DenseReader, DenseWriter};
use sqd_array::io::writer::IOWriter;
use sqd_array::reader::{AnyReader, ArrayReader, NativeReader, ReaderFactory};
use sqd_array::slice::{AsSlice, Slice};
use sqd_array::util::validate_offsets;
use sqd_array::writer::{AnyArrayWriter, NativeWriter, WriterFactory};
use std::io::Write;


pub fn serialize_stats<W: Write>(out: &mut W, stats: &Stats) -> anyhow::Result<()> {
    let mut file = DenseWriter::new(out);
    
    let mut offsets_writer = file.native::<u32>()?;
    offsets_writer.write_slice(&stats.offsets)?;
    offsets_writer.into_write().finish();
    
    ser_array(&mut file, &stats.min)?;
    ser_array(&mut file, &stats.max)?;
    file.finish()?;
    Ok(())
}


fn ser_array<W: Write>(file: &mut DenseWriter<W>, array: &dyn Array) -> anyhow::Result<()> {
    let mut writer = AnyArrayWriter::from_factory(file, array.data_type())?;
    array.as_slice().write(&mut writer)?;
    for buf in writer.into_inner() {
        IOWriter::finish_any_writer(buf)?.finish();
    }
    Ok(())
}


pub fn deserialize_stats(input: &[u8], data_type: &DataType) -> anyhow::Result<Stats> {
    let mut reader = DenseReader::new(input)?;

    let offsets = {
        let mut builder = MutableBuffer::new(0);
        reader.native::<u32>()?.read(&mut builder)?;
        let offsets = ScalarBuffer::<u32>::from(builder);
        validate_offsets(&offsets, 0).map_err(|msg| anyhow!(msg))?;
        ensure!(offsets[0] == 0, "offsets array does not start with 0");
        unsafe {
            OffsetBuffer::new_unchecked(offsets)
        }
    };

    let min = de_array(&mut reader, data_type)?;
    let max = de_array(&mut reader, data_type)?;

    ensure!(
        offsets.len() - 1 == min.len(),
        "offsets array and min array lengths don't correspond each other"
    );

    ensure!(
        offsets.len() - 1 == max.len(),
        "offsets array and max array lengths don't correspond each other"
    );

    ensure!(
        min.nulls() == max.nulls(),
        "min null mask is not equal to max null mask"
    );

    Ok(Stats {
        offsets,
        min,
        max
    })
}


fn de_array(reader: &mut DenseReader<'_>, data_type: &DataType) -> anyhow::Result<ArrayRef> {
    let mut builder = AnyBuilder::new(data_type);
    AnyReader::from_factory(reader, data_type)?.read(&mut builder)?;
    let array = builder.finish();
    Ok(array)
}
