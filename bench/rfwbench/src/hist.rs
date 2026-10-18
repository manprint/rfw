//! Log-linear latency histogram (nanoseconds), ~0.8% relative error.
//! Fixed size, mergeable, no allocation on the record path.

const SUB: usize = 128;
const MAX_IDX: usize = 58 * SUB;

#[derive(Clone)]
pub struct Hist {
    counts: Vec<u64>,
    pub n: u64,
    pub sum: u128,
    pub max: u64,
}

fn bucket(v: u64) -> usize {
    if v < SUB as u64 {
        return v as usize;
    }
    let e = 63 - v.leading_zeros() as usize; // >= 7
    let sub = ((v >> (e - 7)) & (SUB as u64 - 1)) as usize;
    ((e - 6) * SUB + sub).min(MAX_IDX - 1)
}

fn lower(idx: usize) -> u64 {
    if idx < SUB {
        return idx as u64;
    }
    let e = idx / SUB + 6;
    let sub = (idx % SUB) as u64;
    (SUB as u64 + sub) << (e - 7)
}

impl Hist {
    pub fn new() -> Self {
        Hist {
            counts: vec![0; MAX_IDX],
            n: 0,
            sum: 0,
            max: 0,
        }
    }
    #[inline]
    pub fn record(&mut self, ns: u64) {
        self.counts[bucket(ns)] += 1;
        self.n += 1;
        self.sum += ns as u128;
        if ns > self.max {
            self.max = ns;
        }
    }
    pub fn merge(&mut self, o: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(&o.counts) {
            *a += *b;
        }
        self.n += o.n;
        self.sum += o.sum;
        self.max = self.max.max(o.max);
    }
    /// Value (ns) at quantile `q` in 0..=1.
    pub fn pct(&self, q: f64) -> u64 {
        if self.n == 0 {
            return 0;
        }
        let target = ((self.n as f64) * q).ceil().max(1.0) as u64;
        let mut acc = 0;
        for (i, c) in self.counts.iter().enumerate() {
            acc += c;
            if acc >= target {
                return lower(i);
            }
        }
        self.max
    }
    pub fn mean(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.sum as f64 / self.n as f64
        }
    }
    /// JSON object with latency stats in microseconds.
    pub fn json_us(&self) -> String {
        let us = |ns: u64| ns as f64 / 1000.0;
        format!(
            "{{\"n\":{},\"mean\":{:.1},\"p50\":{:.1},\"p90\":{:.1},\"p99\":{:.1},\"p999\":{:.1},\"max\":{:.1}}}",
            self.n,
            self.mean() / 1000.0,
            us(self.pct(0.50)),
            us(self.pct(0.90)),
            us(self.pct(0.99)),
            us(self.pct(0.999)),
            us(self.max)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentiles_are_close() {
        let mut h = Hist::new();
        for v in 1..=100_000u64 {
            h.record(v * 10);
        }
        let p50 = h.pct(0.5) as f64;
        assert!((p50 - 500_000.0).abs() / 500_000.0 < 0.02, "{p50}");
        let p99 = h.pct(0.99) as f64;
        assert!((p99 - 990_000.0).abs() / 990_000.0 < 0.02, "{p99}");
    }
    #[test]
    fn bucket_roundtrip_monotonic() {
        let mut last = 0;
        for v in [0u64, 1, 127, 128, 129, 255, 256, 1000, 1 << 20, 1 << 40] {
            let b = bucket(v);
            assert!(b >= last);
            assert!(lower(b) <= v && v - lower(b) <= v / 64 + 1);
            last = b;
        }
    }
}
