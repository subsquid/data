
enum TableKey {
    Schema,
    Statistic {
        column: u16
    },
    Offsets {
        buffer: u16
    },
    Page {
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
            TableKey::Offsets { buffer } => {
                out.push(2);
                out.extend_from_slice(&buffer.to_be_bytes())
            },
            TableKey::Page { buffer, index } => {
                out.push(3);
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
        let mut buf = Vec::with_capacity(name.len() + 7);
        buf.extend_from_slice(name);
        Self {
            buf,
            name_len: name.len()
        }
    }
    
    fn clear(&mut self) {
        unsafe {
            self.buf.set_len(self.name_len);
        }
    }
    
    pub fn start(&mut self) -> &[u8] {
        self.clear();
        self.buf.push(0);
        self.buf.as_slice()
    }
    
    pub fn end(&mut self) -> &[u8] {
        self.clear();
        self.buf.push(255);
        self.buf.as_slice()
    }

    fn make(&mut self, key: TableKey) -> &[u8] {
        self.clear();
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

    pub fn offsets(&mut self, buffer: usize) -> &[u8] {
        self.make(TableKey::Offsets {
            buffer: buffer as u16
        })
    }

    pub fn page(&mut self, buffer: usize, page: usize) -> &[u8] {
        self.make(TableKey::Page {
            buffer: buffer as u16,
            index: page as u32
        })
    }
}