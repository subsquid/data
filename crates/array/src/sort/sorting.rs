use crate::access::Access;
use crate::slice::{AnySlice, AnyTableSlice, FixedSizeListSlice, ListSlice, Slice};
use crate::sort::order::{IgnoreNulls, Order, OrderList, OrderPair};


macro_rules! with_order {
    ($slice:expr, $order:ident, $cb: expr) => {
        match $slice {
            AnySlice::Boolean(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::UInt8(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::UInt16(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::UInt32(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::UInt64(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::Int8(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::Int16(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::Int32(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::Int64(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::Float32(_) => panic!("sorting on f32 is not supported"),
            AnySlice::Float64(_) => panic!("sorting on f64 is not supported"),
            AnySlice::Binary(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::FixedSizeBinary(s) => dispatch_nulls!(s, $order, $cb),
            AnySlice::List(list) => {
                match list.values().item() {
                    AnySlice::UInt16(it) if !it.has_nulls() => {
                        let slice = ListSlice::new(list.offsets(), it.values(), list.nulls().bitmask());
                        dispatch_nulls!(slice, $order, $cb)
                    },
                    AnySlice::UInt32(it) if !it.has_nulls() => {
                        let slice = ListSlice::new(list.offsets(), it.values(), list.nulls().bitmask());
                        dispatch_nulls!(slice, $order, $cb)
                    },
                    AnySlice::Int32(it) if !it.has_nulls() => {
                        let slice = ListSlice::new(list.offsets(), it.values(), list.nulls().bitmask());
                        dispatch_nulls!(slice, $order, $cb)
                    },
                    _ => panic!("only lists of non-nullable u16, u32 and i32 items are sortable")
                }
            },
            AnySlice::FixedSizeList(list) => {
                match list.values().item() {
                    AnySlice::UInt16(it) if !it.has_nulls() => {
                        let slice = FixedSizeListSlice::new(list.size(), it.values(), list.nulls().bitmask());
                        dispatch_nulls!(slice, $order, $cb)
                    },
                    AnySlice::UInt32(it) if !it.has_nulls() => {
                        let slice = FixedSizeListSlice::new(list.size(), it.values(), list.nulls().bitmask());
                        dispatch_nulls!(slice, $order, $cb)
                    },
                    AnySlice::Int32(it) if !it.has_nulls() => {
                        let slice = FixedSizeListSlice::new(list.size(), it.values(), list.nulls().bitmask());
                        dispatch_nulls!(slice, $order, $cb)
                    },
                    _ => panic!("only fixed size lists of non-nullable u16, u32 and i32 items are sortable")
                }
            },
            AnySlice::Struct(_) => panic!("sorting on structs is not supported"),
        }
    };
}


macro_rules! dispatch_nulls {
    ($slice:ident, $order:ident, $cb: expr) => {
        if $slice.has_nulls() {
            let $order = $slice;
            $cb
        } else {
            let $order = IgnoreNulls($slice);
            $cb
        }
    };
}

macro_rules! sort {
    ($indexes:ident, $order:ident) => {
        $indexes.sort_unstable_by(|&a, &b| $order.compare(a, b))
    };
}


fn to_dyn_order<'a>(slice: AnySlice<'a>) -> Box<dyn Order<usize> + 'a> {
    with_order!(slice, order, {
        Box::new(order)
    })
}


pub fn sort_to_indexes(array: &AnySlice<'_>) -> Vec<usize> {
    let mut indexes: Vec<usize> = (0..array.len()).collect();
    sort_1(&mut indexes, array);
    indexes
}


pub fn sort_table_to_indexes(
    table: &AnyTableSlice<'_>,
    columns: &[usize]
) -> Vec<usize>
{
    macro_rules! col {
        ($i:expr) => {
            table.column(columns[$i])
        };
    }

    let mut indexes: Vec<usize> = (0..table.len()).collect();

    match columns.len() {
        0 => panic!("no columns to sort on"),
        1 => sort_1(&mut indexes, &col!(0)),
        2 => sort_2(&mut indexes, col!(0), col!(1)),
        _ => sort_many(&mut indexes, table, columns)
    };

    indexes
}


fn sort_1(indexes: &mut [usize], col: &AnySlice<'_>) {
    with_order!(col, order, sort!(indexes, order))
}


fn sort_2(indexes: &mut [usize], c1: AnySlice<'_>, c2: AnySlice<'_>) {
    let order2 = to_dyn_order(c2);
    with_order!(c1, order1, {
        let order = OrderPair(order1, order2.as_ref());
        sort!(indexes, order)
    })
}


fn sort_many(indexes: &mut [usize], table: &AnyTableSlice<'_>, columns: &[usize]) {
    let order2 = OrderList::new(
        columns[1..].iter().copied()
            .map(|i| to_dyn_order(table.column(i)))
            .collect()
    );
    with_order!(table.column(columns[0]), order1, {
        let order = OrderPair(order1, order2);
        sort!(indexes, order)
    })
}