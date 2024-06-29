use std::error::Error;
use std::fmt::{Display, Formatter};


pub type Name = &'static str;

pub type BlockNumber = u64;

pub type RowIndex = u32;

pub type RowIndexArrowType = arrow::datatypes::UInt32Type;

pub type RowWeight = u64;

pub type RowWeightPolarsType = polars::datatypes::UInt64Type;


#[derive(Debug)]
pub struct SchemaError {
    pub path: Vec<String>,
    pub message: String
}


impl SchemaError {
    pub fn new<S: ToString>(message: S) -> Self {
        Self {
            path: Vec::new(),
            message: message.to_string()
        }
    }

    pub fn at<S: ToString>(mut self, column_name: S) -> Self {
        self.path.push(column_name.to_string());
        self
    }
}


impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.path.len() > 0 {
            let mut path = self.path.clone();
            path.reverse();
            write!(f, "SchemaError: column {}: {}", path.join("."), self.message)
        } else {
            write!(f, "SchemaError: {}", self.message)
        }
    }
}


impl Error for SchemaError {}


macro_rules! schema_error {
    ($($arg:tt)*) => {
        SchemaError::new(format!($($arg)*))
    };
}
pub(crate) use schema_error;