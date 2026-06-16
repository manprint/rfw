use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::TokioResolver;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::ForwarderConfig;
use crate::stats::{format_bytes, CountingStream, ForwarderStats, StatsRegistry};

/// Run a single forwarder: listen on local_addr, proxy each connection to remote.
///
/// Auto-reconnect: if bind fails, retry with backoff.
/// DNS resolving: fresh per connection (picks up DNS changes automatically).
pub async fn run_forwarder(
    cfg: ForwarderConfig,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    buffer_bytes: usize,
) {
    let resolver = TokioResolver::builder_tokio()
        .expect("Failed to init DNS resolver builder")
        .build()
        .expect("Failed to build DNS resolver");

    // Share config across connection tasks by refcount instead of deep-cloning
    // its strings per accepted connection.
    let cfg = Arc::new(cfg);
    let local_str = cfg.local_addr();
    let label = cfg.label();

    // Bind with auto-reconnect loop
    let listener = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(forwarder = %label, "Cancelled before bind");
                return;
            }
            result = TcpListener::bind(&local_str) => {
                match result {
                    Ok(l) => break l,
                    Err(e) => {
                        error!(forwarder = %label, error = %e, "Bind failed, retry in 5s");
                        if cancelled(&cancel, Duration::from_secs(5)).await {
                            return;
                        }
                    }
                }
            }
        }
    };

    info!(forwarder = %label, "Listening on {} -> {}:{}", local_str, cfg.remote_host, cfg.remote_port);

    // Accept loop
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(forwarder = %label, "Shutting down");
                return;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((inbound, peer_addr)) => {
                        let fwd_cfg = cfg.clone();
                        let resolver = resolver.clone();
                        let s = stats.clone();
                        tokio::spawn(async move {
                            // Guard ties inc/dec together: the count is restored on
                            // every exit path, including a panic in proxy_connection.
                            let _guard = ConnGuard::new(s.clone());
                            if let Err(e) = proxy_connection(inbound, peer_addr, &fwd_cfg, &resolver, s, buffer_bytes).await {
                                warn!(forwarder = %fwd_cfg.label(), peer = %peer_addr, error = %e, "Proxy error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(forwarder = %label, error = %e, "Accept failed");
                        if cancelled(&cancel, Duration::from_secs(1)).await {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Proxy a single TCP connection to remote destination.
/// Resolves hostname fresh each call for dynamic DNS support.
async fn proxy_connection(
    inbound: TcpStream,
    peer_addr: std::net::SocketAddr,
    cfg: &ForwarderConfig,
    resolver: &TokioResolver,
    stats: Arc<ForwarderStats>,
    buffer_bytes: usize,
) -> Result<(), anyhow::Error> {
    // Disable Nagle on the client side: forwarded traffic is often interactive,
    // and Nagle would add latency on small writes.
    if let Err(e) = inbound.set_nodelay(true) {
        warn!(forwarder = %cfg.label(), peer = %peer_addr, error = %e, "set_nodelay(inbound) failed");
    }

    // Resolve remote hostname
    let lookup = tokio::time::timeout(
        Duration::from_secs(10),
        resolver.lookup_ip(&cfg.remote_host),
    )
    .await
    .map_err(|_| anyhow::anyhow!("DNS lookup timeout for {}", cfg.remote_host))?;

    let remote_ips = lookup?;
    let remote_ip = remote_ips
        .iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No DNS records for {}", cfg.remote_host))?;

    let remote_addr = format!("{}:{}", remote_ip, cfg.remote_port);

    // Connect to remote
    let mut outbound =
        tokio::time::timeout(Duration::from_secs(15), TcpStream::connect(&remote_addr))
            .await
            .map_err(|_| anyhow::anyhow!("Connect timeout to {}", remote_addr))??;

    if let Err(e) = outbound.set_nodelay(true) {
        warn!(forwarder = %cfg.label(), peer = %peer_addr, error = %e, "set_nodelay(outbound) failed");
    }

    info!(
        forwarder = %cfg.label(),
        peer = %peer_addr,
        remote = %remote_addr,
        "Connected"
    );

    // Wrap inbound to count bytes at poll level — accurate even when copy returns Err
    // (broken pipe / connection reset would otherwise lose the byte counts).
    // read side  = client→server (counted as bytes_sent)
    // write side = server→client (counted as bytes_received)
    let mut counting = CountingStream::new(inbound, stats);

    match tokio::io::copy_bidirectional_with_sizes(
        &mut counting,
        &mut outbound,
        buffer_bytes,
        buffer_bytes,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            warn!(
                forwarder = %cfg.label(),
                peer = %peer_addr,
                error = %e,
                "Connection closed with error"
            );
        }
    }

    // Graceful shutdown: close both sides
    let _ = AsyncWriteExt::shutdown(&mut counting).await;
    let _ = outbound.shutdown().await;

    info!(
        forwarder = %cfg.label(),
        peer = %peer_addr,
        "Disconnected"
    );

    Ok(())
}

/// Sleep unless cancelled. Returns true if cancelled.
async fn cancelled(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

/// RAII guard: increments the active-connection count on creation and
/// decrements it on drop, guaranteeing the count is restored on every exit
/// path of the connection task, including a panic.
struct ConnGuard(Arc<ForwarderStats>);

impl ConnGuard {
    fn new(stats: Arc<ForwarderStats>) -> Self {
        stats.inc_connections();
        Self(stats)
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.dec_connections();
    }
}

/// Periodically re-derives the EWMA throughput rate for every forwarder.
/// Single writer of the rate gauges; the hot path is untouched.
pub async fn rate_sampler(
    stats_registry: Arc<StatsRegistry>,
    cancel: CancellationToken,
    sample_interval: Duration,
) {
    let mut interval = tokio::time::interval(sample_interval);
    interval.tick().await; // skip immediate first tick
    let mut last = std::time::Instant::now();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return;
            }
            _ = interval.tick() => {
                let now = std::time::Instant::now();
                let dt = now.duration_since(last).as_secs_f64();
                last = now;
                stats_registry.sample_rates(dt).await;
            }
        }
    }
}

/// Stats reporter: logs a traffic report every `report_interval`.
/// Shows **lifetime cumulative** totals and the **current throughput rate**
/// (counters are never reset — that is now the sampler's derived gauge).
pub async fn stats_reporter(
    stats_registry: Arc<StatsRegistry>,
    cancel: CancellationToken,
    report_interval: Duration,
) {
    let mut interval = tokio::time::interval(report_interval);
    interval.tick().await; // skip immediate first tick

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Stats reporter stopped");
                return;
            }
            _ = interval.tick() => {
                let snapshots = stats_registry.collect().await;
                let total_sent: u64 = snapshots.iter().map(|s| s.bytes_sent).sum();
                let total_recv: u64 = snapshots.iter().map(|s| s.bytes_received).sum();

                info!("─── Traffic Report ──────────────────────────────────");

                for s in &snapshots {
                    if s.bytes_sent > 0 || s.bytes_received > 0 || s.active_connections > 0 {
                        info!(
                            "  {:<45} sent={:>10}  recv={:>10}  tx={:>10}/s  rx={:>10}/s  active={}",
                            s.label,
                            format_bytes(s.bytes_sent),
                            format_bytes(s.bytes_received),
                            format_bytes(s.rate_sent_bps),
                            format_bytes(s.rate_recv_bps),
                            s.active_connections
                        );
                    }
                }

                info!(
                    "  TOTAL (all forwarders, lifetime)    sent={:>10}  recv={:>10}",
                    format_bytes(total_sent),
                    format_bytes(total_recv),
                );

                info!("──────────────────────────────────────────────────────");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn conn_guard_decrements_on_drop() {
        let stats = Arc::new(ForwarderStats::new("t".to_string()));
        {
            let _g = ConnGuard::new(stats.clone());
            assert_eq!(stats.active_connections.load(Ordering::Relaxed), 1);
        }
        assert_eq!(stats.active_connections.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn conn_guard_decrements_on_panic() {
        let stats = Arc::new(ForwarderStats::new("t".to_string()));
        let s2 = stats.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = ConnGuard::new(s2);
            panic!("boom");
        }));
        assert!(result.is_err());
        assert_eq!(stats.active_connections.load(Ordering::Relaxed), 0);
    }
}
