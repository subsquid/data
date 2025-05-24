use std::cmp::Ordering;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int32Type, Int64Type, Schema, UInt16Type, UInt32Type, UInt64Type};

use crate::plan::key::{Key, PrimitiveKey, PrimitiveListKey};
use crate::primitives::{Name, schema_error, SchemaError};


pub type Position = (usize, usize);


pub fn compute_order(
    record_batches: &Vec<RecordBatch>,
    key: &[Name]
) -> Result<Vec<Position>, SchemaError>
{
    if record_batches.len() == 0 {
        return Ok(vec![])
    }

    let mut positions = Vec::with_capacity(
        record_batches.iter().map(|b| b.num_rows()).sum()
    );

    for (idx, batch) in record_batches.iter().enumerate() {
        positions.extend((0..batch.num_rows()).map(|pos| (idx, pos)))
    }

    let schema = record_batches[0].schema();
    let key = TableKey::new(&schema, key)?;

    match key.types.as_slice() {
        [data_type] => {
            let col = get_column(record_batches, key.indexes[0], data_type)?;
            positions.sort_unstable_by(|a, b| col.compare_positions(*a, *b))
        },
        [DataType::Int32, DataType::Int32] => {
            sort_by_2::<PrimitiveKey<Int32Type>, PrimitiveKey<Int32Type>>(
                &mut positions,
                record_batches,
                key.indexes[0],
                key.indexes[1]
            )
        },
        [DataType::Int32, DataType::Int32, DataType::List(field)] if field.data_type() == &DataType::Int32 => {
            sort_by_3::<
                PrimitiveKey<Int32Type>,
                PrimitiveKey<Int32Type>,
                PrimitiveListKey<Int32Type>
            >(
                &mut positions,
                record_batches,
                key.indexes[0],
                key.indexes[1],
                key.indexes[2]
            )
        },
        [DataType::Int32, DataType::Int32, DataType::List(field)] if field.data_type() == &DataType::UInt32 => {
            sort_by_3::<
                PrimitiveKey<Int32Type>,
                PrimitiveKey<Int32Type>,
                PrimitiveListKey<UInt32Type>
            >(
                &mut positions,
                record_batches,
                key.indexes[0],
                key.indexes[1],
                key.indexes[2]
            )
        },
        _ => {
            let column_list = ColumnList::new(record_batches, &key)?;
            positions.sort_unstable_by(|a, b| {
               column_list.compare_positions(*a, *b)
            });
        }
    }

    Ok(positions)
}


struct TableKey {
    names: Vec<Name>,
    indexes: Vec<usize>,
    types: Vec<DataType>
}


impl TableKey {
    fn new(schema: &Schema, key: &[Name]) -> Result<Self, SchemaError> {
        let mut indexes = Vec::with_capacity(key.len());
        let mut types = Vec::with_capacity(key.len());

        for &name in key.iter() {
            let (idx, field) = schema.column_with_name(name).ok_or_else(|| {
                schema_error!("key column `{}` is not found in result", name)
            })?;
            indexes.push(idx);
            types.push(field.data_type().clone());
        }

        Ok(Self {
            names: key.into(),
            indexes,
            types
        })
    }

    fn len(&self) -> usize {
        self.indexes.len()
    }
}


#[inline(never)]
fn sort_by_1<'a, K>(
    positions: &mut Vec<Position>,
    record_batches: &'a Vec<RecordBatch>,
    key_idx: usize
) where K: Cmp,
        K: From<&'a dyn Array>
{
    let comparators: Vec<K> = record_batches.iter().map(|b| {
        let array = b.column(key_idx).as_ref();
        K::from(array)
    }).collect();
    sort_positions(positions, &comparators)
}


#[inline(never)]
fn sort_by_2<'a, K1, K2>(
    positions: &mut Vec<(usize, usize)>,
    record_batches: &'a Vec<RecordBatch>,
    key1_idx: usize,
    key2_idx: usize
) where K1: Cmp, K1: From<&'a dyn Array>,
        K2: Cmp, K2: From<&'a dyn Array>
{
    let comparators: Vec<_> = record_batches.iter().map(|b| {
        let array1 = b.column(key1_idx).as_ref();
        let array2 = b.column(key2_idx).as_ref();
        let c1 = K1::from(array1);
        let c2 = K2::from(array2);
        Cons(c1, c2)
    }).collect();
    sort_positions(positions, &comparators)
}


#[inline(never)]
fn sort_by_3<'a, K1, K2, K3>(
    positions: &mut Vec<(usize, usize)>,
    record_batches: &'a Vec<RecordBatch>,
    key1_idx: usize,
    key2_idx: usize,
    key3_idx: usize
) where K1: Cmp, K1: From<&'a dyn Array>,
        K2: Cmp, K2: From<&'a dyn Array>,
        K3: Cmp, K3: From<&'a dyn Array>,
{
    let comparators: Vec<_> = record_batches.iter().map(|b| {
        let array1 = b.column(key1_idx).as_ref();
        let array2 = b.column(key2_idx).as_ref();
        let array3 = b.column(key3_idx).as_ref();
        let c1 = K1::from(array1);
        let c2 = K2::from(array2);
        let c3 = K3::from(array3);
        Cons(c1, Cons(c2, c3))
    }).collect();
    sort_positions(positions, &comparators)
}


fn sort_positions<C: Cmp>(
    positions: &mut Vec<(usize, usize)>,
    comparators: &Vec<C>
) {
    positions.sort_unstable_by(|a, b| {
        let ca = &comparators[a.0];
        let cb = &comparators[b.0];
        ca.compare(a.1, cb, b.1)
    })
}


trait Cmp {
    fn compare(&self, idx: usize, other: &Self, other_idx: usize) -> Ordering;
}


impl <T> Cmp for T where T: Key,
                         T::Item: Ord
{
    #[inline]
    fn compare(&self, idx: usize, other: &Self, other_idx: usize) -> Ordering {
        self.get(idx).cmp(other.get(other_idx))
    }
}


struct Cons<H, T>(H, T);


impl <H, T> Cmp for Cons<H, T> where H: Cmp, T: Cmp {
    fn compare(&self, idx: usize, other: &Self, other_idx: usize) -> Ordering {
        match self.0.compare(idx, &other.0, other_idx)  {
            Ordering::Equal => self.1.compare(idx, &other.1, other_idx),
            ord => ord
        }
    }
}


pub trait PosCmp {
    fn compare_positions(&self, a: Position, b: Position) -> Ordering;
}


struct Column<K> {
    batches: Vec<K>
}


impl <K> PosCmp for Column<K> where K: Key, K::Item: Ord {
    fn compare_positions(&self, a: Position, b: Position) -> Ordering {
        let a_batch = &self.batches[a.0];
        let b_batch = &self.batches[b.0];
        a_batch.get(a.1).cmp(b_batch.get(b.1))
    }
}


struct ColumnList {
    columns: Box<[Box<dyn PosCmp>]>
}


impl PosCmp for ColumnList {
    fn compare_positions(&self, a: Position, b: Position) -> Ordering {
        for col in self.columns.iter() {
            match col.compare_positions(a, b) {
                Ordering::Equal => {},
                ord => return ord
            }
        }
        Ordering::Equal
    }
}


impl ColumnList {
    fn new(record_batches: &Vec<RecordBatch>, key: &TableKey) -> Result<Self, SchemaError> {
        let columns = key.indexes.iter().cloned().enumerate().map(|(i, kix)| {
            get_column(record_batches, kix, &key.types[i]).map_err(|err| err.at(key.names[i]))
        }).collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            columns: columns.into_boxed_slice()
        })
    }
}


fn get_column(
    record_batches: &Vec<RecordBatch>,
    idx: usize,
    data_type: &DataType
) -> Result<Box<dyn PosCmp>, SchemaError> {
    macro_rules! make {
        ($array: ident, $exp:expr) => {{
            let batches = record_batches.iter().map(|batch| {
                let array = batch.column(idx);
                let $array = array.as_ref();
                $exp
            }).collect::<Vec<_>>();

            Ok(Box::new(Column {
                batches
            }))
        }};
    }
    match data_type {
        DataType::UInt16 => make!(array, PrimitiveKey::<UInt16Type>::from(array)),
        DataType::UInt32 => make!(array, PrimitiveKey::<UInt32Type>::from(array)),
        DataType::UInt64 => make!(array, PrimitiveKey::<UInt64Type>::from(array)),
        DataType::Int32 => make!(array, PrimitiveKey::<Int32Type>::from(array)),
        DataType::Int64 => make!(array, PrimitiveKey::<Int64Type>::from(array)),
        DataType::Utf8  => make!(array, array.as_string::<i32>().clone()),
        DataType::List(field) if field.data_type() == &DataType::Int32 => {
            make!(array, PrimitiveListKey::<Int32Type>::from(array))
        },
        DataType::List(field) if field.data_type() == &DataType::UInt32 => {
            make!(array, PrimitiveListKey::<UInt32Type>::from(array))
        },
        DataType::List(field) if field.data_type() == &DataType::UInt16 => {
            make!(array, PrimitiveListKey::<UInt16Type>::from(array))
        },
        _ => Err(
            schema_error!("unsupported key column type - {}", data_type)
        )
    }
}


pub fn make_pos_comparator(
    record_batches: &Vec<RecordBatch>,
    key: &[Name]
) -> Result<impl PosCmp, SchemaError>
{
    if record_batches.len() == 0 {
        return Err(schema_error!("got empty vector of record batches"))
    }
    let schema = record_batches[0].schema();
    let key = TableKey::new(&schema, key)?;
    ColumnList::new(record_batches, &key)
}