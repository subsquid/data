use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc
    },
    time::{Duration, Instant}
};

use parking_lot::Mutex;

use crate::{
    errors::QueryTaskPanicked,
    metrics::{
        COMPLETED_QUERIES, QueryStep, report_query_task_finished, report_query_task_spawned, report_query_task_started,
        report_query_too_many_tasks_error
    },
    settings::SettingsRef
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

type Job = Box<dyn FnOnce() + Send>;

/// Single-thread workers that take query tasks from one FIFO queue. Each is the
/// only thread of its own rayon pool, so the `par_iter` and `join` inside a task
/// run on the task's thread. In the shared pool they spread over every worker,
/// and a new task waits in the injector until a heavy query has no parallel work
/// left: rayon takes injected jobs last.
pub(crate) struct TaskQueue {
    sender: mpsc::Sender<Job>,
    _workers: Vec<rayon::ThreadPool>
}

impl TaskQueue {
    /// `threads` workers, named `<name>-<index>`.
    pub(crate) fn new(threads: usize, name: &'static str) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));

        let workers = (0..threads)
            .map(|i| {
                let worker = rayon::ThreadPoolBuilder::new()
                    .num_threads(1)
                    .thread_name(move |_| format!("{name}-{i}"))
                    .build()?;
                let receiver = receiver.clone();
                worker.spawn(move || {
                    loop {
                        // The guard must drop before the job runs, or the workers run one at a time.
                        let job = receiver.lock().recv();
                        let Ok(job) = job else { return };
                        job();
                    }
                });
                Ok(worker)
            })
            .collect::<anyhow::Result<_>>()?;

        Ok(Self {
            sender,
            _workers: workers
        })
    }

    pub(crate) fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        self.sender
            .send(Box::new(job))
            .expect("the workers outlive the queue's sender")
    }
}

/// Where a query task runs, chosen when its slot is taken.
#[derive(Clone)]
enum TaskPool {
    Shared,
    ThreadPerTask(Arc<TaskQueue>)
}

impl TaskPool {
    fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        match self {
            Self::Shared => sqd_polars::POOL.spawn(job),
            Self::ThreadPerTask(queue) => queue.spawn(job)
        }
    }
}

#[derive(Clone)]
pub struct QueryExecutor {
    // number of concurrent queries
    in_flight: Arc<AtomicUsize>,
    // limit for concurrent queries
    max_pending_tasks: usize,
    urgency: usize,
    settings: SettingsRef,
    task_queue: Arc<TaskQueue>
}

impl QueryExecutor {
    pub fn new(max_pending_tasks: usize, urgency: usize, settings: SettingsRef) -> anyhow::Result<Self> {
        Self::with_task_workers(
            max_pending_tasks,
            urgency,
            settings,
            sqd_polars::POOL.current_num_threads()
        )
    }

    fn with_task_workers(
        max_pending_tasks: usize,
        urgency: usize,
        settings: SettingsRef,
        task_workers: usize
    ) -> anyhow::Result<Self> {
        let task_queue = TaskQueue::new(task_workers, "query-task")?;
        Ok(Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_pending_tasks,
            urgency,
            settings,
            task_queue: Arc::new(task_queue)
        })
    }

    pub fn get_slot(&self) -> Option<QuerySlot> {
        let active_queries = self.in_flight.fetch_add(1, Ordering::SeqCst);
        if active_queries < self.max_pending_tasks {
            let pool = if self.settings.query().thread_per_task {
                TaskPool::ThreadPerTask(self.task_queue.clone())
            } else {
                TaskPool::Shared
            };
            Some(QuerySlot {
                in_flight: self.in_flight.clone(),
                urgency: self.urgency,
                pool
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
    urgency: usize,
    pool: TaskPool
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

    /// Run `task` on the query pool or a single-thread worker, then `report` its
    /// result and time on the same thread. A task that panics is not reported.
    pub async fn run<R, F, P>(self, step: QueryStep, task: F, report: P) -> Result<R, QueryTaskPanicked>
    where
        F: FnOnce(&Self) -> R + Send + 'static,
        P: FnOnce(&R, TaskTime) + Send + 'static,
        R: Send + 'static
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let spawned = Instant::now();
        report_query_task_spawned();
        let pool = self.pool.clone();
        pool.spawn(move || {
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
    use std::{collections::HashSet, thread};

    use rayon::prelude::*;

    use super::*;
    use crate::settings::{QuerySettings, RuntimeSettings, Settings, ShadowSettings, SlowTaskSettings};

    fn executor(max_pending_tasks: usize, thread_per_task: bool) -> QueryExecutor {
        let settings = RuntimeSettings::new(Settings {
            shadow: ShadowSettings {
                percent: 0.0,
                slow_ratio: 2.0,
                slow_margin_ms: 50,
                log_limit: 0
            },
            slow_tasks: SlowTaskSettings {
                threshold_ms: 500,
                log_limit: 0
            },
            query: QuerySettings { thread_per_task }
        })
        .unwrap();
        // A worker for every task a test admits, whatever the size of the polars pool.
        QueryExecutor::with_task_workers(max_pending_tasks, 1, Arc::new(settings), max_pending_tasks).unwrap()
    }

    #[tokio::test]
    async fn worker_panic_returns_an_error_and_releases_the_slot() {
        for thread_per_task in [false, true] {
            let executor = executor(1, thread_per_task);
            let result = executor
                .get_slot()
                .expect("the first task must get a slot")
                .run(QueryStep::First, |_| panic!("injected query panic"), |_: &(), _| {})
                .await;

            assert!(result.is_err());
            assert!(executor.get_slot().is_some(), "the panicked task must release its slot");
        }
    }

    #[tokio::test]
    async fn parallel_work_inside_a_task_stays_on_its_thread() {
        let executor = executor(1, true);
        let threads = executor
            .get_slot()
            .expect("the first task must get a slot")
            .run(
                QueryStep::First,
                |_| {
                    // Work long enough for idle workers to steal it.
                    (0..64)
                        .into_par_iter()
                        .map(|_| {
                            thread::sleep(Duration::from_millis(1));
                            thread::current().id()
                        })
                        .collect::<HashSet<_>>()
                },
                |_, _| {}
            )
            .await
            .unwrap();

        assert_eq!(threads.len(), 1);
    }

    #[tokio::test]
    async fn a_task_runs_while_another_holds_its_worker() {
        let executor = executor(2, true);
        let (start, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();

        let holder = executor.get_slot().expect("the first task must get a slot").run(
            QueryStep::First,
            move |_| {
                start.send(()).unwrap();
                held.recv().unwrap()
            },
            |_, _| {}
        );
        let holder = tokio::spawn(holder);
        started.await.unwrap();

        let other =
            executor
                .get_slot()
                .expect("the second task must get a slot")
                .run(QueryStep::First, |_| (), |_, _| {});
        tokio::time::timeout(Duration::from_secs(10), other)
            .await
            .expect("the task waited for the worker of another")
            .unwrap();

        release.send(()).unwrap();
        holder.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_task_is_reported_after_its_caller_is_gone() {
        let executor = executor(1, false);
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
