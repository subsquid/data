#[cfg(feature = "_bench_query")]
mod query;

// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;


fn main() {
    // Workaround for scan benchmarks to appear
    let _ = sqd_query::Query::from_json_value(serde_json::json!({})).map(|q| q.compile());
    divan::main()
}