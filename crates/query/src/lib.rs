#![allow(dead_code)]

mod plan;
mod primitives;
mod scan;
mod util;
mod json;
mod json_writer;
mod query;


pub use json_writer::JsonArrayWriter;
pub use plan::{Plan, BlockWriter};
pub use primitives::{BlockNumber};
pub use query::*;
pub use util::set_polars_thread_pool_size;

#[cfg(feature = "parquet")]
pub use scan::parquet::ParquetChunk;
