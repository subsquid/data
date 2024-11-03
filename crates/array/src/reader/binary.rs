use crate::reader::native::NativeArrayReader;
use crate::reader::{ListReader, Reader};


pub type BinaryReader<R> = ListReader<R, NativeArrayReader<<R as Reader>::Native>>;