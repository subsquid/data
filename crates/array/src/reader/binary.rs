use crate::reader::{
    native::{ChunkedNativeArrayReader, NativeArrayReader},
    ChunkedFixedSizeListReader, ChunkedListReader, FixedSizeListReader, ListReader, Reader
};

pub type BinaryReader<R> = ListReader<R, NativeArrayReader<<R as Reader>::Native>>;
pub type ChunkedBinaryReader<R> = ChunkedListReader<R, ChunkedNativeArrayReader<<R as Reader>::Native>>;

pub type FixedSizeBinaryReader<R> = FixedSizeListReader<R, NativeArrayReader<<R as Reader>::Native>>;
pub type ChunkedFixedSizeBinaryReader<R> =
    ChunkedFixedSizeListReader<R, ChunkedNativeArrayReader<<R as Reader>::Native>>;
