
pub struct ProjectionMask {
    columns: Vec<bool>
}


impl ProjectionMask {
    pub fn new(len: usize) -> Self {
        Self {
            columns: vec![false; len]
        }
    }

    pub fn include<I: IntoIterator<Item=usize>>(&mut self, columns: I) {
        for col_idx in columns {
            self.columns[col_idx] = true
        }
    }

    pub fn include_column(&mut self, column_index: usize) {
        self.columns[column_index] = true
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_included(&self, column_index: usize) -> bool {
        self.columns[column_index]
    }

    pub fn iter(&self) -> impl Iterator<Item=usize> + '_ {
        self.columns.iter().copied().enumerate().filter_map(|(idx, included)| {
            if included {
                Some(idx)
            } else {
                None
            }
        })
    }

    pub fn selection_len(&self) -> usize {
        let mut len = 0;
        for included in self.columns.iter() {
            len += *included as usize
        }
        len
    }
}