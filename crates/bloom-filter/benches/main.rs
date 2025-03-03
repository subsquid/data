use criterion::{criterion_group, criterion_main, Criterion};
use sqd_bloom_filter::BloomFilter;


fn bloom_filter_benchmark(c: &mut Criterion) {
    let mut bloom = BloomFilter::new(64, 7);
    c.bench_function("insert solana accounts", |bench| {
        bench.iter(|| {
            bloom.insert(&"3NgFNFJBp7GAZDgm9vinbowrEvj7f4wepKVzKeqhcDFN");
            bloom.insert(&"BRwHsJGf5Z2VLB2Gi57YMr4oRZ6FkfNbyUX9dtJW2hhY");
            bloom.insert(&"6YBhJ2Jr3QSMLkQcp6wsfKZFN1rW46mtyBwqeCciR6d5");
        })
    });
}


criterion_group!(benches, bloom_filter_benchmark);
criterion_main!(benches);
