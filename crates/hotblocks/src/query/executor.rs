use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    },
    time::{Duration, Instant}
};

use crate::{
    errors::QueryTaskPanicked,
    metrics::{
        COMPLETED_QUERIES, QueryStep, report_query_task_finished, report_query_task_spawned, report_query_task_started,
        report_query_too_many_tasks_error
    }
};

thread_local! {
    /// Time of the tasks nested in the one this worker runs; `None` between tasks.
    static NESTED: Cell<Option<Duration>> = const { Cell::new(None) };
}

/// How long a query task held its worker.
#[derive(Copy, Clone, Debug)]
pub struct TaskTime {
    pub total: Duration,
    /// Other query tasks this worker ran inside the task while it waited for
    /// the task's parallel work. Tasks nested on other workers are not seen.
    pub nested: Duration
}

impl TaskTime {
    pub fn own(&self) -> Duration {
        self.total.saturating_sub(self.nested)
    }
}

#[derive(Clone)]
pub struct QueryExecutor {
    // number of concurrent queries
    in_flight: Arc<AtomicUsize>,
    // limit for concurrent queries
    max_pending_tasks: usize,
    urgency: usize
}

impl QueryExecutor {
    pub fn new(max_pending_tasks: usize, urgency: usize) -> Self {
        Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_pending_tasks,
            urgency
        }
    }

    pub fn get_slot(&self) -> Option<QuerySlot> {
        let active_queries = self.in_flight.fetch_add(1, Ordering::SeqCst);
        if active_queries < self.max_pending_tasks {
            Some(QuerySlot {
                in_flight: self.in_flight.clone(),
                urgency: self.urgency
            })
        } else {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            report_query_too_many_tasks_error();
            None
        }
    }

    pub fn metrics_collector(&self) -> QueryExecutorCollector {
        QueryExecutorCollector::new(self.in_flight.clone())
    }
}

pub struct QuerySlot {
    in_flight: Arc<AtomicUsize>,
    urgency: usize
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

    /// Run `task` on the query pool, then `report` its result and time on the
    /// same worker. A task that panics is not reported.
    pub async fn run<R, F, P>(self, step: QueryStep, task: F, report: P) -> Result<R, QueryTaskPanicked>
    where
        F: FnOnce(&Self) -> R + Send + 'static,
        P: FnOnce(&R, TaskTime) + Send + 'static,
        R: Send + 'static
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let spawned = Instant::now();
        report_query_task_spawned();
        sqd_polars::POOL.spawn(move || {
            let started = Instant::now();
            report_query_task_started(step, started.duration_since(spawned));
            let outer = NESTED.replace(Some(Duration::ZERO));

            let slot = self;
            let result = catch_unwind(AssertUnwindSafe(|| task(&slot))).map_err(|_| QueryTaskPanicked);

            let total = started.elapsed();
            let nested = NESTED.replace(outer.map(|nested| nested + total)).unwrap_or_default();
            report_query_task_finished(step, total);

            // Release before waking the caller: sending first lets it observe the slot still
            // taken and be refused admission for a query that has already finished.
            drop(slot);
            // Here and not in the caller, which is gone when the client disconnects.
            if let Ok(value) = &result {
                report(value, TaskTime { total, nested });
            }
            let _ = tx.send(result);
        });

        rx.await.unwrap_or(Err(QueryTaskPanicked))
    }
}

#[derive(Debug)]
pub struct QueryExecutorCollector {
    in_flight: Arc<AtomicUsize>
}

impl QueryExecutorCollector {
    pub fn new(in_flight: Arc<AtomicUsize>) -> Self {
        Self { in_flight }
    }

    pub fn get_active_queries(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn worker_panic_returns_an_error_and_releases_the_slot() {
        let executor = QueryExecutor::new(1, 1);
        let result = executor
            .get_slot()
            .expect("the first task must get a slot")
            .run(QueryStep::First, |_| panic!("injected query panic"), |_: &(), _| {})
            .await;

        assert!(result.is_err());
        assert!(executor.get_slot().is_some(), "the panicked task must release its slot");
    }

    #[tokio::test]
    async fn a_task_is_reported_after_its_caller_is_gone() {
        let executor = QueryExecutor::new(1, 1);
        let (start, started) = tokio::sync::oneshot::channel();
        let (finish, finished) = std::sync::mpsc::channel::<()>();
        let (report, reported) = std::sync::mpsc::channel();

        let run = executor.get_slot().expect("the first task must get a slot").run(
            QueryStep::Next,
            move |_| {
                start.send(()).unwrap();
                finished.recv().unwrap()
            },
            move |_, time| report.send(time).unwrap()
        );
        let caller = tokio::spawn(run);
        started.await.unwrap();
        caller.abort();
        assert!(
            matches!(caller.await, Err(e) if e.is_cancelled()),
            "the caller was not dropped while the task ran"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        finish.send(()).unwrap();
        let time = reported
            .recv_timeout(Duration::from_secs(10))
            .expect("the task was not reported");
        assert!(time.total >= Duration::from_millis(50), "{time:?}");
    }
}
