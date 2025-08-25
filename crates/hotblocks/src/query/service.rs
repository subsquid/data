use super::executor::QueryExecutor;
use super::response::QueryResponse;
use crate::dataset_controller::DatasetController;
use crate::errors::{Busy, QueryIsAboveTheHead, QueryKindMismatch};
use crate::types::{DBRef, DatasetKind};
use anyhow::{bail, ensure};
use sqd_query::Query;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;


pub type QueryServiceRef = Arc<QueryService>;


pub struct QueryServiceBuilder {
    db: DBRef,
    max_data_waiters: usize,
    max_pending_tasks: usize,
    urgency: usize,
}


impl QueryServiceBuilder {
    pub fn new(db: DBRef) -> Self {
        Self {
            db,
            max_data_waiters: 64_000,
            max_pending_tasks: sqd_polars::POOL.current_num_threads() * 200,
            urgency: 500
        }
    }

    /// Max number of pending queries, waiting for new block arrival
    pub fn set_max_data_waiters(&mut self, count: usize) -> &mut Self {
        self.max_data_waiters = count;
        self
    }

    /// Max number of pending query tasks (work units).
    ///
    /// When exceeded, existing streams terminate and for new queries `Busy` error is returned.
    pub fn set_max_pending_query_tasks(&mut self, count: usize) -> &mut Self {
        self.max_pending_tasks = count;
        self
    }

    /// Roughly corresponds to a maximum time
    /// any query task can spend waiting in a work queue
    pub fn set_urgency(&mut self, ms: usize) -> &mut Self {
        self.urgency = ms;
        self
    }

    pub fn build(&self) -> QueryService {
        QueryService {
            db: self.db.clone(),
            executor: QueryExecutor::new(self.max_pending_tasks, self.urgency),
            wait_slots: WaitSlots {
                waiters: AtomicUsize::new(0),
                limit: self.max_data_waiters
            }
        }
    }
}


pub struct QueryService {
    db: DBRef,
    executor: QueryExecutor,
    wait_slots: WaitSlots
}


impl QueryService {
    pub fn builder(db: DBRef) -> QueryServiceBuilder {
        QueryServiceBuilder::new(db)
    }

    pub async fn query(&self, dataset: &DatasetController, query: Query) -> anyhow::Result<QueryResponse> {
        ensure!(
            dataset.dataset_kind() == DatasetKind::from_query(&query),
            QueryKindMismatch {
                query_kind: DatasetKind::from_query(&query).storage_kind(),
                dataset_kind: dataset.dataset_kind().storage_kind()
            }
        );

        let should_wait = match dataset.get_head() {
            Some(head) if head.number >= query.first_block() => false,
            Some(head) if head.number + 1 == query.first_block() => {
                if let Some(parent_hash) = query.parent_block_hash() {
                    ensure!(
                        head.hash == parent_hash,
                        sqd_query::UnexpectedBaseBlock {
                            prev_blocks: vec![head],
                            expected_hash: parent_hash.to_string()
                        }
                    );
                }
                true
            },
            Some(_) | None => true
        };

        if should_wait {
            let Some(_wait_slot) = self.wait_slots.get() else {
                bail!(Busy)
            };
            tokio::time::timeout(
                Duration::from_secs(5),
                dataset.wait_for_block(query.first_block())
            ).await.map_err(|_| {
                QueryIsAboveTheHead {
                    finalized_head: None
                }
            })?;
        }

        QueryResponse::new(
            self.executor.clone(),
            self.db.clone(),
            dataset.dataset_id(),
            query
        ).await
    }
}


struct WaitSlots {
    waiters: AtomicUsize,
    limit: usize
}


impl WaitSlots {
    fn get(&self) -> Option<WaitingSlot<'_>> {
        let previously_waiting = self.waiters.fetch_add(1, Ordering::SeqCst);
        let slot = WaitingSlot {
            waiters: &self.waiters
        };
        if previously_waiting < self.limit {
            Some(slot)
        } else {
            crate::metrics::report_query_too_many_data_waiters_error();
            None
        }
    }
}


struct WaitingSlot<'a> {
    waiters: &'a AtomicUsize
}


impl<'a> Drop for WaitingSlot<'a> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}