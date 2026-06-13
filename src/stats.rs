use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::RwLock;

/// Per-forwarder traffic counters.
/// Atomics so concurrent connections can increment without contention.
#[derive(Debug)]
pub struct ForwarderStats {
    label: String,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub active_connections: AtomicU64,
}

impl ForwarderStats {
    pub fn new(label: String) -> Self {
        Self {
            label,
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
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

    /// Take a snapshot of current counters and reset them to zero.
    /// Returns (label, bytes_sent, bytes_received, active_connections).
    pub fn snapshot_and_reset(&self) -> (String, u64, u64, u64) {
        let sent = self.bytes_sent.swap(0, Ordering::Relaxed);
        let recv = self.bytes_received.swap(0, Ordering::Relaxed);
        let active = self.active_connections.load(Ordering::Relaxed);
        (self.label.clone(), sent, recv, active)
    }
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

    /// Collect and reset all stats for periodic reporting.
    /// Returns (label, bytes_sent, bytes_received, active_connections).
    pub async fn collect_and_reset(&self) -> Vec<(String, u64, u64, u64)> {
        let stats = self.stats.read().await;
        stats.iter().map(|s| s.snapshot_and_reset()).collect()
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

/// Wraps a stream and counts bytes read and written at the poll level.
/// Byte counts accumulate regardless of whether I/O completes or errors.
pub struct CountingStream<S> {
    inner: S,
    bytes_read: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
}

impl<S> CountingStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            bytes_read: Arc::new(AtomicU64::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
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
                self.bytes_read.fetch_add(n as u64, Ordering::Relaxed);
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
                self.bytes_written.fetch_add(*n as u64, Ordering::Relaxed);
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
