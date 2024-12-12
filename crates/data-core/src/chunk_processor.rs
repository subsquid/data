use crate::{PreparedTable, TableProcessor};
use sqd_array::slice::AnyTableSlice;
use std::collections::BTreeMap;


type Name = &'static str;


pub struct ChunkProcessor {
    tables: BTreeMap<Name, TableProcessor>,
}


impl ChunkProcessor {
    pub fn new(tables: BTreeMap<Name, TableProcessor>) -> ChunkProcessor {
        ChunkProcessor { tables }
    }

    pub fn push(&mut self, slices: BTreeMap<Name, AnyTableSlice<'_>>) -> anyhow::Result<()> {
        for (name, slice) in slices {
            let processor = self.tables.get_mut(name).unwrap();
            processor.push_batch(&slice)?;
        }
        Ok(())
    }

    pub fn byte_size(&self) -> usize {
        let mut size = 0;
        for table in self.tables.values() {
            size += table.byte_size();
        }
        size
    }

    pub fn finish(self) -> anyhow::Result<BTreeMap<Name, PreparedTable>> {
        self.tables
            .into_iter()
            .map(|(name, table)| table.finish().map(|table| (name, table)))
            .collect()
    }
}
