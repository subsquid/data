use crate::metrics::{ACTIVE_QUERIES, COMPLETED_QUERIES, report_query_too_many_tasks_error};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{self, Duration};

#[derive(Clone)]
pub struct QueryExecutor {
    // number of concurrent queries
    in_flight: Arc<AtomicUsize>,
    // limit for concurrent queries
    max_pending_tasks: usize,
    urgency: usize,
}

impl QueryExecutor {
    pub fn new(max_pending_tasks: usize, urgency: usize) -> Self {
        Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_pending_tasks,
            urgency,
        }
    }

    pub fn get_slot(&self) -> Option<QuerySlot> {
        let active_queries = self.in_flight.fetch_add(1, Ordering::SeqCst);
        if active_queries < self.max_pending_tasks {
            Some(QuerySlot {
                in_flight: self.in_flight.clone(),
                urgency: self.urgency,
            })
        } else {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            report_query_too_many_tasks_error();
            None
        }
    }

    pub fn spawn_metrics_reporter(&self, interval: Duration) {
        let active_count = self.in_flight.clone();

        tokio::spawn(async move {
            let mut ticker = time::interval(interval);

            loop {
                ticker.tick().await;
                let active_queries = active_count.load(Ordering::SeqCst);
                ACTIVE_QUERIES.set(active_queries as i64);
            }
        });
    }
}

pub struct QuerySlot {
    in_flight: Arc<AtomicUsize>,
    urgency: usize,
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        COMPLETED_QUERIES.inc();
    }
}

impl QuerySlot {
    pub fn time_limit(&self) -> usize {
        let in_flight = self.in_flight.load(Ordering::SeqCst);
        if in_flight == 0 {
            return 100;
        }
        let time = self.urgency * sqd_polars::POOL.current_num_threads() / in_flight;
        time.min(100)
    }

    pub async fn run<R, F>(self, task: F) -> R
    where
        F: FnOnce(&Self) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        sqd_polars::POOL.spawn(move || {
            let slot = self;
            let result = task(&slot);
            let _ = tx.send(result);
        });

        rx.await.expect("task panicked")
    }
}
