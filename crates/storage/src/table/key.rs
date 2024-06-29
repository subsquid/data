use borsh::BorshSerialize;


#[derive(BorshSerialize, Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub enum Statistic {
    Offsets,
    NullCount,
    Min,
    Max
}


enum TableKey {
    Schema,
    RowGroupOffsets,
    Statistic0 {
        column: u16,
        kind: Statistic
    },
    Statistic1 {
        row_group: u16,
        column: u16,
        kind: Statistic
    },
    Page {
        row_group: u16,
        column: u16,
        page: u32
    }
}


impl TableKey {
    fn serialize(&self, out: &mut Vec<u8>) {
        match self {
            TableKey::Schema => {
                out.push(0)
            },
            TableKey::RowGroupOffsets => {
                out.push(1)
            },
            TableKey::Statistic0 { column, kind } => {
                out.push(2);
                out.extend_from_slice(&column.to_be_bytes());
                kind.serialize(out).unwrap()
            },
            TableKey::Statistic1 { row_group, column, kind } => {
                out.push(3);
                out.extend_from_slice(&row_group.to_be_bytes());
                out.extend_from_slice(&column.to_be_bytes());
                kind.serialize(out).unwrap()
            },
            TableKey::Page { row_group, column, page } => {
                out.push(4);
                out.extend_from_slice(&row_group.to_be_bytes());
                out.extend_from_slice(&column.to_be_bytes());
                out.extend_from_slice(&page.to_be_bytes())
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

    pub fn row_group_offsets(&mut self) -> &[u8] {
        self.make(TableKey::RowGroupOffsets)
    }

    pub fn statistic0(&mut self, kind: Statistic, column_index: usize) -> &[u8] {
        self.make(TableKey::Statistic0 {
            column: column_index as u16,
            kind
        })
    }

    pub fn statistic1(&mut self, kind: Statistic, row_group: usize, column_index: usize) -> &[u8] {
        self.make(TableKey::Statistic1 {
            row_group: row_group as u16,
            column: column_index as u16,
            kind
        })
    }

    pub fn page(&mut self, row_group: usize, column_index: usize, page_index: usize) -> &[u8] {
        self.make(TableKey::Page {
            row_group: row_group as u16,
            column: column_index as u16,
            page: page_index as u32
        })
    }
}