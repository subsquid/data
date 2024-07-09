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

pub use scan::Chunk;

#[cfg(feature = "parquet")]
pub use scan::parquet::ParquetChunk;

#[cfg(feature = "storage")]
pub use scan::storage::StorageChunk;
