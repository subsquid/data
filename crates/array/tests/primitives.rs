use arrow_buffer::ToByteSlice;
use sqd_array::builder::offsets::OffsetsBuilder;
use sqd_array::io::reader::{IOByteReader, OffsetsIOReader};
use sqd_array::reader::OffsetsReader;
use std::io::Cursor;
use sqd_array::io::writer::OffsetsIOWriter;
use sqd_array::offsets::Offsets;
use sqd_array::writer::OffsetsWriter;


#[test]
fn io_offsets_read_all() -> anyhow::Result<()> {
    let offsets: Vec<i32> = (0..20001).collect();
    let cursor = Cursor::new(offsets.to_byte_slice());

    let mut reader = OffsetsIOReader::new(
        IOByteReader::new(
            offsets.to_byte_slice().len(), 
            cursor
        )
    )?;

    let mut builder = OffsetsBuilder::new(offsets.len());
    let range = reader.read_slice(&mut builder, 0, offsets.len() - 1)?;
    
    assert_eq!(range, 0..20000);
    assert_eq!(offsets.as_slice(), builder.finish().as_ref());
    
    Ok(())
}


#[test]
fn io_offsets_write_all() -> anyhow::Result<()> {
    let offsets: Vec<i32> = (0..20001).collect();
    
    let data = {
        let mut writer = OffsetsIOWriter::new(Vec::new());
        writer.write_slice(Offsets::new(&offsets))?;
        writer.finish()?
    };

    assert_eq!(data.as_slice(), offsets.to_byte_slice());
    
    Ok(())
}