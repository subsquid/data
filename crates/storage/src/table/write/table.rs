use std::collections::HashMap;
use borsh::{BorshDeserialize, BorshSerialize};


#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct ColumnOptions {
    pub with_stats: bool
}


#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct TableOptions {
    pub column_options: HashMap<String, ColumnOptions>,
    pub default_page_size: usize
}


impl Default for TableOptions {
    fn default() -> Self {
        Self {
            column_options: HashMap::new(),
            default_page_size: 32 * 1024
        }
    }
}


impl TableOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stats<N: AsRef<str>>(&mut self, column_names: &[N]) {
        for col in column_names.iter() {
            let name = col.as_ref();
            if let Some(options) = self.column_options.get_mut(name) {
                options.with_stats = true
            } else {
                self.column_options.insert(name.to_string(), ColumnOptions {
                    with_stats: true
                });
            }
        }
    }

    pub fn has_stats(&self, column: &str) -> bool {
        self.column_options.get(column).map(|opts| opts.with_stats).unwrap_or(false)
    }
}