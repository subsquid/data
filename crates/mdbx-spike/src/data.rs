//! Page generator with controlled LZ4 compressibility + percentile helper.

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

/// One fresh random word per `dup` words; LZ4 ratio lands near `dup`.
pub fn gen_page(rng: &mut Rng, len: usize, dup: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut word = rng.next();
    let mut i = 0usize;
    while out.len() < len {
        if i % dup.max(1) == 0 {
            word = rng.next();
        }
        out.extend_from_slice(&word.to_le_bytes());
        i += 1;
    }
    out.truncate(len);
    out
}

pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

pub struct LatencyReport {
    pub count: usize,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub max: u64
}

pub fn latency_report(mut micros: Vec<u64>) -> LatencyReport {
    micros.sort_unstable();
    LatencyReport {
        count: micros.len(),
        p50: percentile(&micros, 0.50),
        p90: percentile(&micros, 0.90),
        p99: percentile(&micros, 0.99),
        max: micros.last().copied().unwrap_or(0)
    }
}
