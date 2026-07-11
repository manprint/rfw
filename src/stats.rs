use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::RwLock;

/// EWMA smoothing factor for rate derivation (0..1). Higher = more reactive.
const RATE_ALPHA: f64 = 0.3;

/// Per-forwarder traffic counters.
///
/// `bytes_sent` / `bytes_received` are **lifetime-monotonic**: they accumulate
/// for the whole process lifetime and are never reset. Instantaneous throughput
/// is derived separately by the rate sampler ([`ForwarderStats::update_rate`])
/// and stored in `rate_sent_bps` / `rate_recv_bps`.
///
/// Atomics so concurrent connections can increment without contention.
#[derive(Debug)]
pub struct ForwarderStats {
    label: String,
    /// Lifetime bytes client -> remote.
    pub bytes_sent: AtomicU64,
    /// Lifetime bytes remote -> client.
    pub bytes_received: AtomicU64,
    pub active_connections: AtomicU64,
    /// Current send throughput (bytes/sec), EWMA-smoothed. Written only by the sampler.
    pub rate_sent_bps: AtomicU64,
    /// Current receive throughput (bytes/sec), EWMA-smoothed. Written only by the sampler.
    pub rate_recv_bps: AtomicU64,
    /// Previous sampled value of `bytes_sent` (sampler-private).
    last_sample_sent: AtomicU64,
    /// Previous sampled value of `bytes_received` (sampler-private).
    last_sample_recv: AtomicU64,
}

impl ForwarderStats {
    pub fn new(label: String) -> Self {
        Self {
            label,
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            rate_sent_bps: AtomicU64::new(0),
            rate_recv_bps: AtomicU64::new(0),
            last_sample_sent: AtomicU64::new(0),
            last_sample_recv: AtomicU64::new(0),
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn add_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Read a consistent-ish snapshot of all counters. Never resets anything.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            label: self.label.clone(),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            rate_sent_bps: self.rate_sent_bps.load(Ordering::Relaxed),
            rate_recv_bps: self.rate_recv_bps.load(Ordering::Relaxed),
        }
    }

    /// Re-derive EWMA throughput rates from the delta since the previous sample.
    /// Single-writer (the rate sampler task) — Relaxed ordering is sufficient.
    pub fn update_rate(&self, dt_secs: f64) {
        let cur_sent = self.bytes_sent.load(Ordering::Relaxed);
        let cur_recv = self.bytes_received.load(Ordering::Relaxed);
        let prev_sent = self.last_sample_sent.swap(cur_sent, Ordering::Relaxed);
        let prev_recv = self.last_sample_recv.swap(cur_recv, Ordering::Relaxed);
        let new_sent = compute_rate(
            prev_sent,
            cur_sent,
            dt_secs,
            self.rate_sent_bps.load(Ordering::Relaxed),
        );
        let new_recv = compute_rate(
            prev_recv,
            cur_recv,
            dt_secs,
            self.rate_recv_bps.load(Ordering::Relaxed),
        );
        self.rate_sent_bps.store(new_sent, Ordering::Relaxed);
        self.rate_recv_bps.store(new_recv, Ordering::Relaxed);
    }
}

/// Compute an EWMA-smoothed byte rate (bytes/sec) from two cumulative samples.
/// Pure and timing-independent for testability.
pub fn compute_rate(prev: u64, cur: u64, dt_secs: f64, prev_ewma: u64) -> u64 {
    if dt_secs <= 0.0 {
        return prev_ewma;
    }
    let delta = cur.saturating_sub(prev);
    let inst = delta as f64 / dt_secs;
    let ewma = RATE_ALPHA * inst + (1.0 - RATE_ALPHA) * prev_ewma as f64;
    ewma.round() as u64
}

/// Immutable snapshot of a forwarder's counters at one instant.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub label: String,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub active_connections: u64,
    pub rate_sent_bps: u64,
    pub rate_recv_bps: u64,
}

/// Global registry of all forwarder stats.
pub struct StatsRegistry {
    stats: RwLock<Vec<Arc<ForwarderStats>>>,
}

impl StatsRegistry {
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(Vec::new()),
        }
    }

    /// Register a new forwarder and return its stats handle.
    pub async fn register(&self, label: &str) -> Arc<ForwarderStats> {
        let s = Arc::new(ForwarderStats::new(label.to_string()));
        self.stats.write().await.push(s.clone());
        s
    }

    /// Collect non-resetting snapshots of all forwarders for reporting/exposure.
    pub async fn collect(&self) -> Vec<StatsSnapshot> {
        let stats = self.stats.read().await;
        stats.iter().map(|s| s.snapshot()).collect()
    }

    /// Re-derive throughput rates for all forwarders given the elapsed time.
    pub async fn sample_rates(&self, dt_secs: f64) {
        let stats = self.stats.read().await;
        for s in stats.iter() {
            s.update_rate(dt_secs);
        }
    }

    pub async fn remove(&self, label: &str) {
        let mut stats = self.stats.write().await;
        stats.retain(|s| s.label() != label);
    }
}

impl Default for StatsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Human-readable byte formatting (1024-based, 3 decimal places).
pub fn format_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = n as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} B", n)
    } else {
        format!("{:.3} {}", size, UNITS[unit_idx])
    }
}

/// Wraps a stream and counts bytes read and written at the poll level into the
/// shared [`ForwarderStats`]. Byte counts accumulate regardless of whether I/O
/// completes or errors.
pub struct CountingStream<S> {
    inner: S,
    stats: Arc<ForwarderStats>,
}

impl<S> CountingStream<S> {
    pub fn new(inner: S, stats: Arc<ForwarderStats>) -> Self {
        Self { inner, stats }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountingStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(poll, Poll::Ready(Ok(()))) {
            let n = buf.filled().len().saturating_sub(before);
            if n > 0 {
                self.stats.add_sent(n as u64);
            }
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            if *n > 0 {
                self.stats.add_received(*n as u64);
            }
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn counting_stream_accumulates_cumulatively() {
        let stats = Arc::new(ForwarderStats::new("test".to_string()));
        stats.inc_connections();

        let (mut peer, inner) = tokio::io::duplex(64);
        let mut counting = CountingStream::new(inner, stats.clone());

        peer.write_all(b"ping").await.unwrap();

        let mut inbound = [0_u8; 4];
        counting.read_exact(&mut inbound).await.unwrap();

        let snap = stats.snapshot();
        assert_eq!(snap.bytes_sent, 4);
        assert_eq!(snap.bytes_received, 0);
        assert_eq!(snap.active_connections, 1);

        counting.write_all(b"pong").await.unwrap();

        let mut outbound = [0_u8; 4];
        peer.read_exact(&mut outbound).await.unwrap();

        // Cumulative: sent stays at 4 (NOT reset between snapshots), recv now 4.
        let snap = stats.snapshot();
        assert_eq!(snap.bytes_sent, 4);
        assert_eq!(snap.bytes_received, 4);
        assert_eq!(snap.active_connections, 1);
    }

    #[test]
    fn stats_counters_are_cumulative_not_reset() {
        let stats = ForwarderStats::new("t".to_string());
        stats.add_sent(100);
        assert_eq!(stats.snapshot().bytes_sent, 100);
        stats.add_sent(50);
        // Proves snapshot does not reset: second read includes the first.
        assert_eq!(stats.snapshot().bytes_sent, 150);
    }

    #[test]
    fn compute_rate_basic() {
        // 1000 bytes over 1s, no prior EWMA -> 0.3 * 1000 = 300.
        assert_eq!(compute_rate(0, 1000, 1.0, 0), 300);
        // Zero/negative dt returns the prior EWMA unchanged.
        assert_eq!(compute_rate(0, 1000, 0.0, 42), 42);
        // No new traffic decays toward 0: 0.3*0 + 0.7*1000 = 700.
        assert_eq!(compute_rate(500, 500, 1.0, 1000), 700);
    }

    #[test]
    fn update_rate_writes_gauges() {
        let stats = ForwarderStats::new("t".to_string());
        stats.add_sent(2000);
        stats.update_rate(1.0);
        // 0.3 * 2000 = 600.
        assert_eq!(stats.snapshot().rate_sent_bps, 600);
        // No further traffic -> decays.
        stats.update_rate(1.0);
        assert_eq!(stats.snapshot().rate_sent_bps, 420); // 0.7 * 600
    }

    #[test]
    fn format_bytes_edges() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.000 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.000 MB");
        assert_eq!(format_bytes(1024_u64.pow(4)), "1.000 TB");
    }
}
