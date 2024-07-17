pub mod prelude;
pub mod arrow;


pub use polars_core::POOL;


/// Safety: Call it in the main at the very beginning.
/// See https://doc.rust-lang.org/std/env/fn.set_var.html for more details
pub unsafe fn set_polars_thread_pool_size(n_threads: usize) {
    std::env::set_var("POLARS_MAX_THREADS", format!("{}", n_threads))
}
