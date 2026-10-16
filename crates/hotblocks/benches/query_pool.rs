//! Open-loop model of the query pool: how long light query tasks wait while a
//! rare heavy one runs, for each way to schedule them.
//!
//! Tasks arrive from a thread outside the pool, as `POOL.spawn` gets them from
//! tokio. Each splits its work like the legacy engine: `par_iter` over scans,
//! `par_iter` over columns and a `rayon::join` tree over pages, with a
//! sequential part around it. Work is a calibrated spin, so CPU contention
//! slows it as it would slow a real scan.
//!
//! Policies:
//!   shared           one pool of N threads (the default)
//!   groups           pools of N/G threads; a task goes to the group with the fewest tasks
//!   thread-per-task  N one-thread pools that take tasks from one FIFO queue
//!                    (`--query-thread-per-task`)
//!
//! A polars share sends that part of each task's parallel work to a separate
//! N-thread pool with `install`, as polars does with its global pool. With
//! `shared` that pool is the query pool itself.
//!
//!   cargo bench -p sqd-hotblocks --bench query_pool                # full sweep, ~25 min
//!   SQD_BENCH_SECS=20 cargo bench -p sqd-hotblocks --bench query_pool
//!
//! Needs N free cores: on a busy machine the spin calibration and every
//! result drift.

use std::{
    cell::Cell,
    hint::black_box,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
        mpsc
    },
    time::{Duration, Instant}
};

use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

const THREADS: usize = 8;
const LIGHT_RATE: f64 = 280.0;
const HEAVY_RATE: f64 = 0.3;
/// CPU milliseconds of a light task by quantile, log-linear in between.
const LIGHT_CPU_MS: &[(f64, f64)] = &[
    (0.0, 0.3),
    (0.05, 0.6),
    (0.5, 1.3),
    (0.75, 2.8),
    (0.9, 11.0),
    (0.95, 25.0),
    (0.99, 150.0),
    (0.999, 550.0),
    (1.0, 1500.0)
];
/// A heavy task's CPU milliseconds, log-uniform in this range.
const HEAVY_CPU_MS: (f64, f64) = (1000.0, 4000.0);

static ITERS_PER_US: OnceLock<f64> = OnceLock::new();

fn spin_iters(n: u64) -> u64 {
    let mut x = 1u64;
    for _ in 0..n {
        x = black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407));
    }
    x
}

fn calibrate() -> f64 {
    let n = 200_000_000;
    spin_iters(n / 10);
    let started = Instant::now();
    black_box(spin_iters(n));
    n as f64 / (started.elapsed().as_secs_f64() * 1e6)
}

fn spin(us: f64) {
    if us > 0.0 {
        black_box(spin_iters((us * ITERS_PER_US.get().unwrap()) as u64));
    }
}

#[derive(Clone, Copy)]
struct Shape {
    seq_us: f64,
    scans: usize,
    columns: usize,
    pages: usize,
    page_us: f64,
    polars_scans: usize
}

impl Shape {
    fn new(cpu_us: f64, seq_share: f64, scans: usize, columns: usize, page_us: f64, polars_share: f64) -> Self {
        let par_us = cpu_us * (1.0 - seq_share);
        let pages = ((par_us / (scans * columns) as f64 / page_us).round() as usize).max(1);
        Shape {
            seq_us: cpu_us * seq_share,
            scans,
            columns,
            pages,
            page_us: par_us / (scans * columns * pages) as f64,
            polars_scans: (scans as f64 * polars_share).round() as usize
        }
    }

    fn execute(&self, polars: Option<&ThreadPool>) {
        spin(self.seq_us / 2.0);

        let scan = |_| {
            (0..self.columns)
                .into_par_iter()
                .for_each(|_| pages(self.pages, self.page_us))
        };
        (0..self.scans - self.polars_scans).into_par_iter().for_each(scan);
        match polars {
            Some(pool) => pool.install(|| (0..self.polars_scans).into_par_iter().for_each(scan)),
            None => (0..self.polars_scans).into_par_iter().for_each(scan)
        }

        spin(self.seq_us / 2.0);
    }
}

fn pages(n: usize, page_us: f64) {
    if n <= 1 {
        spin(page_us)
    } else {
        let mid = n / 2 + n % 2;
        rayon::join(|| pages(mid, page_us), || pages(n - mid, page_us));
    }
}

#[derive(Clone, Copy, Debug)]
enum Policy {
    Shared,
    Groups(usize),
    ThreadPerTask
}

type Job = Box<dyn FnOnce() + Send>;

struct Group {
    pool: ThreadPool,
    in_flight: AtomicUsize
}

struct Scheduler {
    groups: Vec<Group>,
    queue: Option<mpsc::Sender<Job>>,
    polars: Option<ThreadPool>,
    next: AtomicUsize
}

fn pool(threads: usize) -> ThreadPool {
    ThreadPoolBuilder::new().num_threads(threads).build().unwrap()
}

impl Scheduler {
    fn new(policy: Policy) -> Self {
        match policy {
            Policy::Shared => Self::groups(1, THREADS, None),
            Policy::Groups(g) => Self::groups(g, THREADS / g, Some(pool(THREADS))),
            Policy::ThreadPerTask => {
                let (sender, receiver) = mpsc::channel::<Job>();
                let receiver = Arc::new(Mutex::new(receiver));
                let mut scheduler = Self::groups(THREADS, 1, Some(pool(THREADS)));
                for group in &scheduler.groups {
                    let receiver = receiver.clone();
                    group.pool.spawn(move || {
                        loop {
                            let job = receiver.lock().unwrap().recv();
                            let Ok(job) = job else { return };
                            job();
                        }
                    });
                }
                scheduler.queue = Some(sender);
                scheduler
            }
        }
    }

    fn groups(n: usize, threads: usize, polars: Option<ThreadPool>) -> Self {
        Self {
            groups: (0..n)
                .map(|_| Group {
                    pool: pool(threads),
                    in_flight: AtomicUsize::new(0)
                })
                .collect(),
            queue: None,
            polars,
            next: AtomicUsize::new(0)
        }
    }

    fn least_loaded(&self) -> usize {
        let n = self.groups.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        (0..n)
            .map(|i| (start + i) % n)
            .min_by_key(|&i| self.groups[i].in_flight.load(Ordering::Relaxed))
            .unwrap()
    }
}

thread_local! {
    static NESTED: Cell<Option<Duration>> = const { Cell::new(None) };
}

#[derive(Clone, Copy)]
struct Record {
    heavy: bool,
    spawned: Duration,
    queue: Duration,
    latency: Duration,
    /// Run time without the tasks nested in it.
    own: Duration
}

struct Rng(u64);

impl Rng {
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }

    fn exp(&mut self, rate: f64) -> f64 {
        -(1.0 - self.next_f64()).ln() / rate
    }
}

fn light_cpu_ms(u: f64) -> f64 {
    for w in LIGHT_CPU_MS.windows(2) {
        let ((q0, v0), (q1, v1)) = (w[0], w[1]);
        if u <= q1 {
            let f = (u - q0) / (q1 - q0);
            return (v0.ln() + f * (v1.ln() - v0.ln())).exp();
        }
    }
    LIGHT_CPU_MS.last().unwrap().1
}

struct Run {
    policy: Policy,
    heavy_rate: f64,
    polars_share: f64
}

fn run(case: &Run, secs: f64, warmup: f64) -> Vec<Record> {
    let scheduler = Arc::new(Scheduler::new(case.policy));
    let records = Arc::new(Mutex::new(Vec::new()));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let rate = LIGHT_RATE + case.heavy_rate;
    let begin = Instant::now();
    let end = Duration::from_secs_f64(secs + warmup);
    let mut next_at = Duration::ZERO;

    while next_at < end {
        let now = begin.elapsed();
        if next_at > now {
            let ahead = next_at - now;
            if ahead > Duration::from_micros(1500) {
                std::thread::sleep(ahead - Duration::from_micros(1000));
            } else {
                std::thread::yield_now();
            }
            continue;
        }

        let heavy = rng.next_f64() < case.heavy_rate / rate;
        let shape = if heavy {
            let (lo, hi) = HEAVY_CPU_MS;
            let ms = (lo.ln() + rng.next_f64() * (hi.ln() - lo.ln())).exp();
            Shape::new(ms * 1000.0, 0.1, 3, 10, 200.0, case.polars_share)
        } else {
            let ms = light_cpu_ms(rng.next_f64());
            Shape::new(ms * 1000.0, 0.5, 2, 4, 100.0, case.polars_share.min(0.5))
        };

        let group = scheduler.least_loaded();
        scheduler.groups[group].in_flight.fetch_add(1, Ordering::Relaxed);
        in_flight.fetch_add(1, Ordering::Relaxed);
        let (scheduler2, records2, in_flight2) = (scheduler.clone(), records.clone(), in_flight.clone());
        let spawned = begin.elapsed();
        let spawned_at = Instant::now();
        let job: Job = Box::new(move || {
            let started = Instant::now();
            let outer = NESTED.replace(Some(Duration::ZERO));
            shape.execute(scheduler2.polars.as_ref());
            let total = started.elapsed();
            let nested = NESTED.replace(outer.map(|n| n + total)).unwrap_or_default();

            scheduler2.groups[group].in_flight.fetch_sub(1, Ordering::Relaxed);
            records2.lock().unwrap().push(Record {
                heavy,
                spawned,
                queue: started - spawned_at,
                latency: started + total - spawned_at,
                own: total.saturating_sub(nested)
            });
            in_flight2.fetch_sub(1, Ordering::Relaxed);
        });
        match &scheduler.queue {
            Some(queue) => queue.send(job).unwrap(),
            None => scheduler.groups[group].pool.spawn(job)
        }

        next_at += Duration::from_secs_f64(rng.exp(rate));
    }

    while in_flight.load(Ordering::Relaxed) > 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    let warm = Duration::from_secs_f64(warmup);
    let records = records.lock().unwrap();
    records.iter().copied().filter(|r| r.spawned >= warm).collect()
}

struct Stats {
    mean: f64,
    p99: f64,
    p999: f64,
    max: f64
}

fn stats(records: &[Record], get: impl Fn(&Record) -> Duration) -> Stats {
    let mut ms: Vec<f64> = records.iter().map(|r| get(r).as_secs_f64() * 1000.0).collect();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let at = |p: f64| ms[((ms.len() as f64 * p) as usize).min(ms.len() - 1)];
    Stats {
        mean: ms.iter().sum::<f64>() / ms.len() as f64,
        p99: at(0.99),
        p999: at(0.999),
        max: *ms.last().unwrap()
    }
}

fn main() {
    let secs: f64 = std::env::var("SQD_BENCH_SECS").map_or(120.0, |s| s.parse().unwrap());
    let warmup = (secs / 24.0).max(1.0);
    ITERS_PER_US.set(calibrate()).unwrap();

    let cases = [
        (Policy::Shared, 0.0, 0.0),
        (Policy::ThreadPerTask, 0.0, 0.0),
        (Policy::Shared, HEAVY_RATE, 0.0),
        (Policy::Groups(2), HEAVY_RATE, 0.0),
        (Policy::Groups(8), HEAVY_RATE, 0.0),
        (Policy::ThreadPerTask, HEAVY_RATE, 0.0),
        (Policy::ThreadPerTask, HEAVY_RATE, 1.0 / 3.0),
        (Policy::ThreadPerTask, HEAVY_RATE, 2.0 / 3.0)
    ];

    println!(
        "query_pool: {THREADS} threads, {LIGHT_RATE} light tasks/s, {secs} s per run, {:.0} spin iterations/us",
        ITERS_PER_US.get().unwrap()
    );
    println!(
        "| heavy/s | policy | polars share | light queue mean / p99 / p99.9 ms | light latency mean / p99 / p99.9 ms | heavy own mean ms | heavy latency mean / max ms |"
    );
    println!("|---|---|---|---|---|---|---|");
    for (policy, heavy_rate, polars_share) in cases {
        let case = Run {
            policy,
            heavy_rate,
            polars_share
        };
        let records = run(&case, secs, warmup);
        let (heavy, light): (Vec<Record>, Vec<Record>) = records.into_iter().partition(|r| r.heavy);

        let queue = stats(&light, |r| r.queue);
        let latency = stats(&light, |r| r.latency);
        let (heavy_own, heavy_latency) = if heavy.is_empty() {
            ("-".to_string(), "-".to_string())
        } else {
            let own = stats(&heavy, |r| r.own);
            let latency = stats(&heavy, |r| r.latency);
            (
                format!("{:.0}", own.mean),
                format!("{:.0} / {:.0}", latency.mean, latency.max)
            )
        };
        println!(
            "| {heavy_rate} | {policy:?} | {polars_share:.2} | {:.1} / {:.0} / {:.0} | {:.1} / {:.0} / {:.0} | {heavy_own} | {heavy_latency} |",
            queue.mean, queue.p99, queue.p999, latency.mean, latency.p99, latency.p999
        );
    }
}
