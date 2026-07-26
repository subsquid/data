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

/// Deterministic hash-index key for (dataset, counter): a 0xFF sentinel byte
/// (sorts after every 16-byte table prefix, so the hash range shares the mdbx
/// tree without colliding) + 31 uniformly random bytes — the block/tx-hash
/// insert pattern, reproducible so deletes can re-derive their keys.
pub fn hkey(ds: usize, i: u64) -> [u8; 32] {
    let seed = ((ds as u64) << 33).wrapping_add(i).wrapping_mul(0x9E3779B97F4A7C15);
    let mut rng = Rng::new(seed);
    let mut out = [0u8; 32];
    for c in out.chunks_exact_mut(8) {
        c.copy_from_slice(&rng.next().to_le_bytes());
    }
    out[0] = 0xFF;
    out
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
