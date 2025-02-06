use crate::reader::native::{ChunkedNativeArrayReader, NativeArrayReader};
use crate::reader::{ChunkedListReader, ListReader, Reader};


pub type BinaryReader<R> = ListReader<R, NativeArrayReader<<R as Reader>::Native>>;
pub type ChunkedBinaryReader<R> = ChunkedListReader<R, ChunkedNativeArrayReader<<R as Reader>::Native>>;