use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;


pub type QueryExecutorRef = Arc<QueryExecutor>;


pub struct QueryExecutor {
    in_flight: Arc<AtomicUsize>,
    max_pending_tasks: usize
}


impl QueryExecutor {
    pub fn new(max_pending_tasks: usize) -> Self {
        Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_pending_tasks
        }
    }

    pub async fn run<R, F>(&self, task: F) -> Option<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static
    {
        self.run_with_ctx((), move |_| task()).await.ok()
    }

    pub async fn run_with_ctx<C, R, F>(&self, ctx: C, task: F) -> Result<R, C>
    where
        F: FnOnce(C) -> R + Send + 'static,
        R: Send + 'static,
        C: Send + 'static
    {
        let pending = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        if pending > self.max_pending_tasks {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return Err(ctx)
        }

        let pending_guard = DecrementOnDrop::new(self.in_flight.clone());

        let (tx, rx) = tokio::sync::oneshot::channel();

        sqd_polars::POOL.spawn(move || {
            let _ = pending_guard;
            let result = task(ctx);
            let _ = tx.send(result);
        });
        
        // FIXME: how does this type check?
        println!("{:?}", pending_guard.counter);

        Ok(
            rx.await.expect("task panicked")
        )
    }
}


struct DecrementOnDrop {
    counter: Arc<AtomicUsize>
}


impl DecrementOnDrop {
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        Self {
            counter
        }
    }    
}


impl Drop for DecrementOnDrop {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}