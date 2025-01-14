use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;


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
        let pending = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        if pending > self.max_pending_tasks {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return None
        }

        let pending_guard = DecrementOnDrop::new(self.in_flight.clone());

        let (tx, rx) = tokio::sync::oneshot::channel();

        sqd_polars::POOL.spawn(move || {
            let _ = pending_guard;
            let result = task();
            let _ = tx.send(result);
        });
        
        // FIXME: how does this type check?
        println!("{:?}", pending_guard.counter);

        rx.await.ok()
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