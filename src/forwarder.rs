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
) {
    let resolver = TokioResolver::builder_tokio()
        .expect("Failed to init DNS resolver builder")
        .build()
        .expect("Failed to build DNS resolver");

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
                        stats.inc_connections();
                        let fwd_cfg = cfg.clone();
                        let resolver = resolver.clone();
                        let s = stats.clone();
                        tokio::spawn(async move {
                            if let Err(e) = proxy_connection(inbound, peer_addr, &fwd_cfg, &resolver, &s).await {
                                warn!(forwarder = %fwd_cfg.label(), peer = %peer_addr, error = %e, "Proxy error");
                            }
                            s.dec_connections();
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
    stats: &ForwarderStats,
) -> Result<(), anyhow::Error> {
    // Resolve remote hostname
    let lookup = tokio::time::timeout(
        Duration::from_secs(10),
        resolver.lookup_ip(&cfg.remote_host),
    )
    .await
    .map_err(|_| anyhow::anyhow!("DNS lookup timeout for {}", cfg.remote_host))?;

    let remote_ips = lookup?;
    let remote_ip = remote_ips.iter().next().ok_or_else(|| {
        anyhow::anyhow!("No DNS records for {}", cfg.remote_host)
    })?;

    let remote_addr = format!("{}:{}", remote_ip, cfg.remote_port);

    // Connect to remote
    let mut outbound = tokio::time::timeout(
        Duration::from_secs(15),
        TcpStream::connect(&remote_addr),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Connect timeout to {}", remote_addr))??;

    info!(
        forwarder = %cfg.label(),
        peer = %peer_addr,
        remote = %remote_addr,
        "Connected"
    );

    // Wrap inbound to count bytes at poll level — accurate even when copy returns Err
    // (broken pipe / connection reset would otherwise lose the byte counts).
    // bytes_read  = client→server (sent to remote)
    // bytes_written = server→client (received from remote)
    let mut counting = CountingStream::new(inbound);

    match tokio::io::copy_bidirectional(&mut counting, &mut outbound).await {
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

    stats.add_sent(counting.bytes_read());
    stats.add_received(counting.bytes_written());

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

/// Stats reporter: logs traffic report every 60 seconds.
pub async fn stats_reporter(stats_registry: Arc<StatsRegistry>, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.tick().await; // skip immediate first tick

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Stats reporter stopped");
                return;
            }
            _ = interval.tick() => {
                let snapshots = stats_registry.collect_and_reset().await;
                let total_sent: u64 = snapshots.iter().map(|(_, s, _, _)| s).sum();
                let total_recv: u64 = snapshots.iter().map(|(_, _, r, _)| r).sum();

                info!("─── Traffic Report ──────────────────────────────────");

                for (label, sent, recv, active) in &snapshots {
                    if *sent > 0 || *recv > 0 || *active > 0 {
                        info!(
                            "  {:<45} send={:>10}  recv={:>10}  active={}",
                            label,
                            format_bytes(*sent),
                            format_bytes(*recv),
                            active
                        );
                    }
                }

                info!(
                    "  TOTAL (all forwarders)              send={:>10}  recv={:>10}",
                    format_bytes(total_sent),
                    format_bytes(total_recv),
                );

                info!("──────────────────────────────────────────────────────");
            }
        }
    }
}
