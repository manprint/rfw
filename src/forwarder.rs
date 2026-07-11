use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::TokioResolver;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::config::{ForwarderConfig, RuntimeKnobs};
use crate::stats::{format_bytes, CountingStream, ForwarderStats, StatsRegistry};

/// Apply per-socket tuning: disable Nagle and (optionally) size the kernel
/// send/receive buffers. Failures are logged but non-fatal.
fn tune_socket(
    stream: &TcpStream,
    socket_buffer_bytes: Option<usize>,
    label: &str,
    peer: std::net::SocketAddr,
    which: &str,
) {
    if let Err(e) = stream.set_nodelay(true) {
        warn!(forwarder = %label, peer = %peer, error = %e, "set_nodelay({which}) failed");
    }
    if let Some(sz) = socket_buffer_bytes {
        let sock = socket2::SockRef::from(stream);
        if let Err(e) = sock.set_recv_buffer_size(sz) {
            warn!(forwarder = %label, peer = %peer, error = %e, "set_recv_buffer_size({which}) failed");
        }
        if let Err(e) = sock.set_send_buffer_size(sz) {
            warn!(forwarder = %label, peer = %peer, error = %e, "set_send_buffer_size({which}) failed");
        }
    }
}

/// Run a single forwarder: listen on local_addr, proxy each connection to remote.
///
/// Auto-reconnect: if bind fails, retry with backoff.
/// DNS resolving: fresh per connection (picks up DNS changes automatically).
pub async fn run_forwarder(
    cfg: ForwarderConfig,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    conn_tracker: TaskTracker,
) {
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
                        // Snapshot the live data-plane knobs at accept time; in-flight
                        // connections keep their values across a hot-reload.
                        let buffer_bytes = knobs.buffer_bytes();
                        let socket_buffer_bytes = knobs.socket_buffer_bytes();
                        let use_splice = knobs.use_splice();
                        // Track the task so shutdown can drain in-flight transfers.
                        conn_tracker.spawn(async move {
                            // Guard ties inc/dec together: the count is restored on
                            // every exit path, including a panic in proxy_connection.
                            let _guard = ConnGuard::new(s.clone());
                            if let Err(e) = proxy_connection(
                                inbound, peer_addr, &fwd_cfg, &resolver, s,
                                buffer_bytes, socket_buffer_bytes, use_splice,
                            ).await {
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
#[allow(clippy::too_many_arguments)]
async fn proxy_connection(
    inbound: TcpStream,
    peer_addr: std::net::SocketAddr,
    cfg: &ForwarderConfig,
    resolver: &TokioResolver,
    stats: Arc<ForwarderStats>,
    buffer_bytes: usize,
    socket_buffer_bytes: Option<usize>,
    use_splice: bool,
) -> Result<(), anyhow::Error> {
    let label = cfg.label();

    // Tune the client-side socket: disable Nagle (forwarded traffic is often
    // interactive) and optionally size the kernel buffers.
    tune_socket(&inbound, socket_buffer_bytes, &label, peer_addr, "inbound");

    // Resolve remote hostname fresh (dynamic DNS).
    let lookup = tokio::time::timeout(
        Duration::from_secs(10),
        resolver.lookup_ip(&cfg.remote_host),
    )
    .await
    .map_err(|_| anyhow::anyhow!("DNS lookup timeout for {}", cfg.remote_host))?;
    let remote_ips = lookup?;

    // Connect to remote, trying each resolved address until one succeeds
    // (failover instead of giving up on a dead first record).
    let mut outbound = None;
    let mut remote_addr = None;
    let mut last_err: Option<anyhow::Error> = None;
    for ip in remote_ips.iter() {
        let addr = std::net::SocketAddr::new(ip, cfg.remote_port);
        match tokio::time::timeout(Duration::from_secs(15), TcpStream::connect(addr)).await {
            Ok(Ok(s)) => {
                outbound = Some(s);
                remote_addr = Some(addr);
                break;
            }
            Ok(Err(e)) => last_err = Some(anyhow::anyhow!("connect {addr}: {e}")),
            Err(_) => last_err = Some(anyhow::anyhow!("connect timeout to {addr}")),
        }
    }
    let mut outbound = outbound.ok_or_else(|| {
        last_err.unwrap_or_else(|| anyhow::anyhow!("No DNS records for {}", cfg.remote_host))
    })?;
    let remote_addr = remote_addr.unwrap();

    tune_socket(
        &outbound,
        socket_buffer_bytes,
        &label,
        peer_addr,
        "outbound",
    );

    info!(forwarder = %label, peer = %peer_addr, remote = %remote_addr, "Connected");

    // Linux zero-copy fast path (opt-in): splice bytes kernel-side, no userspace copy.
    if use_splice {
        #[cfg(target_os = "linux")]
        {
            let r = splice::copy_bidirectional_splice(inbound, outbound, &stats, buffer_bytes)
                .await
                .map_err(anyhow::Error::from);
            if let Err(e) = &r {
                warn!(forwarder = %label, peer = %peer_addr, error = %e, "splice closed with error");
            }
            info!(forwarder = %label, peer = %peer_addr, "Disconnected");
            return r;
        }
    }

    // Portable copy path. Wrap inbound to count bytes at poll level — accurate even
    // when copy returns Err (broken pipe / reset would otherwise lose the counts).
    // read side  = client→server (counted as bytes_sent)
    // write side = server→client (counted as bytes_received)
    let mut counting = CountingStream::new(inbound, stats);

    if let Err(e) = tokio::io::copy_bidirectional_with_sizes(
        &mut counting,
        &mut outbound,
        buffer_bytes,
        buffer_bytes,
    )
    .await
    {
        warn!(forwarder = %label, peer = %peer_addr, error = %e, "Connection closed with error");
    }

    // Graceful shutdown: close both sides
    let _ = AsyncWriteExt::shutdown(&mut counting).await;
    let _ = outbound.shutdown().await;

    info!(forwarder = %label, peer = %peer_addr, "Disconnected");

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

/// Linux `splice(2)` zero-copy data path.
///
/// Moves bytes directly between the two sockets through a kernel pipe, never
/// copying the payload into user space. Each direction uses its own pipe and
/// counts the bytes it moves; on EOF it half-closes the destination's write
/// side so the peer observes the shutdown, matching `copy_bidirectional`.
#[cfg(target_os = "linux")]
mod splice {
    use std::io;
    use std::net::Shutdown;
    use std::os::unix::io::{AsRawFd, RawFd};

    use tokio::io::Interest;
    use tokio::net::TcpStream;

    use crate::stats::ForwarderStats;

    /// Owns a `pipe2` pair and closes both ends on drop.
    struct Pipe {
        r: RawFd,
        w: RawFd,
    }

    impl Pipe {
        fn new(size: usize) -> io::Result<Self> {
            let mut fds = [0 as RawFd; 2];
            // O_NONBLOCK so the pipe side of splice never blocks the runtime;
            // O_CLOEXEC so the fds do not leak across exec.
            let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
            let pipe = Pipe {
                r: fds[0],
                w: fds[1],
            };
            // Best-effort: enlarge the pipe to the configured buffer size so each
            // splice moves a bigger chunk (fewer syscalls). Ignore failure.
            if size > 0 {
                unsafe { libc::fcntl(pipe.w, libc::F_SETPIPE_SZ, size as libc::c_int) };
            }
            Ok(pipe)
        }
    }

    impl Drop for Pipe {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.r);
                libc::close(self.w);
            }
        }
    }

    /// One `splice` call. Returns `WouldBlock` (mapped from `EAGAIN`) so the
    /// caller can await readiness and retry.
    fn splice_raw(fd_in: RawFd, fd_out: RawFd, len: usize) -> io::Result<usize> {
        let n = unsafe {
            libc::splice(
                fd_in,
                std::ptr::null_mut(),
                fd_out,
                std::ptr::null_mut(),
                len,
                (libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK) as libc::c_uint,
            )
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    /// Pump one direction `src -> dst` until `src` reaches EOF, then half-close
    /// `dst`'s write side. `on_bytes` is invoked with each moved chunk length.
    async fn splice_dir(
        src: &TcpStream,
        dst: &TcpStream,
        pipe_size: usize,
        on_bytes: impl Fn(u64),
    ) -> io::Result<()> {
        let pipe = Pipe::new(pipe_size)?;
        let src_fd = src.as_raw_fd();
        let dst_fd = dst.as_raw_fd();
        let chunk = pipe_size.max(1);

        loop {
            // src -> pipe
            let n = loop {
                src.readable().await?;
                match src.try_io(Interest::READABLE, || splice_raw(src_fd, pipe.w, chunk)) {
                    Ok(n) => break n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e),
                }
            };
            if n == 0 {
                break; // EOF on src
            }
            on_bytes(n as u64);

            // pipe -> dst, draining all n bytes before reading more (keeps the
            // pipe from ever filling, so it cannot deadlock).
            let mut left = n;
            while left > 0 {
                dst.writable().await?;
                match dst.try_io(Interest::WRITABLE, || splice_raw(pipe.r, dst_fd, left)) {
                    Ok(0) => break, // dst refused further writes
                    Ok(m) => left -= m,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        // Signal EOF downstream (best-effort).
        let _ = socket2::SockRef::from(dst).shutdown(Shutdown::Write);
        Ok(())
    }

    /// Bidirectional zero-copy forward. `inbound` is the client side, `outbound`
    /// the remote side. Counts client→remote as `bytes_sent` and remote→client
    /// as `bytes_received`, matching the portable `CountingStream` path.
    pub async fn copy_bidirectional_splice(
        inbound: TcpStream,
        outbound: TcpStream,
        stats: &ForwarderStats,
        pipe_size: usize,
    ) -> io::Result<()> {
        let client_to_remote = splice_dir(&inbound, &outbound, pipe_size, |n| stats.add_sent(n));
        let remote_to_client =
            splice_dir(&outbound, &inbound, pipe_size, |n| stats.add_received(n));
        tokio::try_join!(client_to_remote, remote_to_client)?;
        Ok(())
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
