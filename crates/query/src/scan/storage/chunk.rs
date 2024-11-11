use crate::scan::scan::Scan;
use crate::scan::Chunk;
use sqd_primitives::Name;
use sqd_storage::db::ChunkReader;


impl <'a> Chunk for ChunkReader<'a> {
    fn scan_table(&self, name: Name) -> anyhow::Result<Scan<'a>> {
        let table_reader = self.get_table_reader(name)?;
        let scan = Scan::new(table_reader);
        Ok(scan)
    }
}