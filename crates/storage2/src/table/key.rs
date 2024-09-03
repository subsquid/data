
enum TableKey {
    Schema,
    Statistic {
        column: u16
    },
    Offsets {
        column: u16,
        buffer: u16
    },
    Page {
        column: u16,
        buffer: u16,
        index: u32
    }
}


impl TableKey {
    fn serialize(&self, out: &mut Vec<u8>) {
        match self {
            TableKey::Schema => {
                out.push(0);
            },
            TableKey::Statistic { column } => {
                out.push(1);
                out.extend_from_slice(&column.to_be_bytes());
            },
            TableKey::Offsets { column, buffer } => {
                out.push(2);
                out.extend_from_slice(&column.to_be_bytes());
                out.extend_from_slice(&buffer.to_be_bytes())
            },
            TableKey::Page { column, buffer, index } => {
                out.push(3);
                out.extend_from_slice(&column.to_be_bytes());
                out.extend_from_slice(&buffer.to_be_bytes());
                out.extend_from_slice(&index.to_be_bytes());
            }
        }
    }
}


#[derive(Clone)]
pub struct TableKeyFactory {
    buf: Vec<u8>,
    name_len: usize
}


impl TableKeyFactory {
    pub fn new<N: AsRef<[u8]>>(table_name: N) -> Self {
        let name = table_name.as_ref();
        let mut buf = Vec::with_capacity(name.len() + 9);
        buf.extend_from_slice(name);
        Self {
            buf,
            name_len: name.len()
        }
    }

    fn make(&mut self, key: TableKey) -> &[u8] {
        unsafe {
            self.buf.set_len(self.name_len);
        }
        key.serialize(&mut self.buf);
        self.buf.as_slice()
    }

    pub fn schema(&mut self) -> &[u8] {
        self.make(TableKey::Schema)
    }

    pub fn statistic(&mut self, column_index: usize) -> &[u8] {
        self.make(TableKey::Statistic {
            column: column_index as u16
        })
    }

    pub fn offsets(&mut self, column: usize, buffer: usize) -> &[u8] {
        self.make(TableKey::Offsets {
            column: column as u16,
            buffer: buffer as u16
        })
    }

    pub fn page(&mut self, column: usize, buffer: usize, page: usize) -> &[u8] {
        self.make(TableKey::Page {
            column: column as u16,
            buffer: buffer as u16,
            index: page as u32
        })
    }
}