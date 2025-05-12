use crate::storage::{Block, BlockBatch, BlockHeader, BlockHeaderBatch};
use ouroboros::self_referencing;
use scylla::_macro_internal::DeserializeRow;
use scylla::response::query_result::QueryRowsResult;
use std::marker::PhantomData;


pub trait Row: 'static {
    type Type<'a>;
    type Tuple<'a>: DeserializeRow<'a, 'a>;

    fn convert(row: Self::Tuple<'_>) -> anyhow::Result<Self::Type<'_>>;

    fn reborrow<'a, 'this>(slice: &'a [Self::Type<'this>]) -> &'a [Self::Type<'a>];
}


pub struct RowBatch<R: Row> {
    inner: RowBatchInner<R>
}


#[self_referencing]
struct RowBatchInner<R: Row> {
    rows: QueryRowsResult,
    #[borrows(rows)]
    #[not_covariant]
    items: Box<[R::Type<'this>]>,
    phantom_data: PhantomData<R>
}


impl<R: Row> RowBatch<R> {
    pub fn new(rows: QueryRowsResult) -> anyhow::Result<Self> {
        Ok(Self {
            inner: RowBatchInnerTryBuilder {
                rows,
                items_builder: |rows| {
                    rows.rows::<'_, R::Tuple<'_>>()?.map(|row| {
                        R::convert(row?)
                    }).collect()
                },
                phantom_data: PhantomData::default()
            }.try_build()?
        })
    }
    
    pub fn items(&self) -> &[R::Type<'_>] {
        self.inner.with_items(|items| R::reborrow(items))
    }
}