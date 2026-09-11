use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use hickory_resolver::TokioResolver;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::config::{ForwarderConfig, KnobSnapshot, RuntimeKnobs};
use crate::stats::{format_bytes, CountingStream, ForwarderStats, StatsRegistry};

/// Stagger between parallel connect attempts to successive resolved addresses
/// (RFC 8305 "Happy Eyeballs" connection attempt delay). Small enough that a
/// dead first record costs ~250 ms instead of a full connect timeout, large
/// enough that a healthy first record almost always wins on its own.
const CONNECT_ATTEMPT_DELAY: Duration = Duration::from_millis(250);

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
#[allow(clippy::too_many_arguments)]
pub async fn run_forwarder(
    cfg: ForwarderConfig,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    conn_tracker: TaskTracker,
    conn_limit: Option<Arc<Semaphore>>,
) {
    // Share config across connection tasks by refcount instead of deep-cloning
    // its strings per accepted connection.
    let cfg = Arc::new(cfg);
    let local_str = cfg.local_addr();
    // Format the label once per forwarder, not once per connection: it is used
    // in every log record on the accept path.
    let label: Arc<str> = Arc::from(cfg.label());
    // An IP-literal remote never needs the resolver. Skipping it removes a
    // per-connection async hop (and its allocations) from the connect path.
    let remote_literal: Option<IpAddr> = cfg.remote_host.parse().ok();

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
                        // Take the concurrency permit *before* spawning, so a
                        // flood applies backpressure to the accept queue instead
                        // of piling up tasks that each hold two copy buffers.
                        let permit = match acquire_permit(&conn_limit, &stats) {
                            Admission::Admitted(p) => p,
                            Admission::Rejected => {
                                // Close immediately; the kernel backlog will
                                // shed the rest while we stay inside our budget.
                                drop(inbound);
                                debug!(
                                    forwarder = %label,
                                    peer = %peer_addr,
                                    "Connection rejected: max_connections reached"
                                );
                                continue;
                            }
                        };

                        let fwd_cfg = cfg.clone();
                        let resolver = resolver.clone();
                        let s = stats.clone();
                        let conn_label = label.clone();
                        // Snapshot the live data-plane knobs at accept time; in-flight
                        // connections keep their values across a hot-reload.
                        let knobs = knobs.snapshot();
                        // Track the task so shutdown can drain in-flight transfers.
                        conn_tracker.spawn(async move {
                            // Guard ties inc/dec together: the count is restored on
                            // every exit path, including a panic in proxy_connection.
                            let _guard = ConnGuard::new(s.clone());
                            // Released only once the transfer is fully done.
                            let _permit = permit;
                            if let Err(e) = proxy_connection(
                                inbound, peer_addr, &fwd_cfg, &conn_label, &resolver, s,
                                remote_literal, knobs,
                            ).await {
                                warn!(forwarder = %conn_label, peer = %peer_addr, error = %e, "Proxy error");
                            }
                        });
                    }
                    Err(e) => {
                        // Per-connection errors (ECONNABORTED: the peer gave up
                        // between SYN and accept) are normal and must not stall
                        // the loop. Resource exhaustion (EMFILE/ENFILE) is not
                        // self-clearing, so back off instead of spinning on it.
                        if is_transient_accept_error(&e) {
                            debug!(forwarder = %label, error = %e, "Accept failed (transient)");
                            continue;
                        }
                        error!(forwarder = %label, error = %e, "Accept failed, backing off 1s");
                        if cancelled(&cancel, Duration::from_secs(1)).await {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Outcome of asking the global connection limiter for a slot.
enum Admission {
    /// Proceed. Carries the permit to hold for the connection's lifetime
    /// (`None` when no limit is configured).
    Admitted(Option<OwnedSemaphorePermit>),
    /// The limit is saturated; the caller drops the accepted socket.
    Rejected,
}

/// Acquire a slot from the global connection limit, if one is configured.
///
/// Non-blocking on purpose: shedding at the edge keeps the process inside its
/// memory budget, whereas queueing would let an unbounded number of accepted
/// sockets wait while each still holds kernel buffers.
fn acquire_permit(limit: &Option<Arc<Semaphore>>, stats: &ForwarderStats) -> Admission {
    let Some(sem) = limit else {
        return Admission::Admitted(None);
    };
    match sem.clone().try_acquire_owned() {
        Ok(permit) => Admission::Admitted(Some(permit)),
        Err(_) => {
            stats.inc_rejected();
            Admission::Rejected
        }
    }
}

/// `true` for accept errors caused by one connection rather than by the
/// process running out of resources. Retrying these immediately is correct;
/// backing off would add latency to every other pending connection.
fn is_transient_accept_error(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        e.kind(),
        ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::Interrupted
            | ErrorKind::NotConnected
    )
}

/// Resolve `remote_host` to the addresses to try, in order.
///
/// An IP literal short-circuits the resolver entirely; anything else is looked
/// up fresh so DNS changes are picked up per connection (hickory caches
/// internally, so a repeated lookup is not a repeated network round trip).
async fn resolve_targets(
    remote_host: &str,
    remote_port: u16,
    literal: Option<IpAddr>,
    resolver: &TokioResolver,
    dns_timeout: Duration,
) -> Result<Vec<std::net::SocketAddr>, anyhow::Error> {
    if let Some(ip) = literal {
        return Ok(vec![std::net::SocketAddr::new(ip, remote_port)]);
    }

    let lookup = tokio::time::timeout(dns_timeout, resolver.lookup_ip(remote_host))
        .await
        .map_err(|_| anyhow::anyhow!("DNS lookup timeout for {remote_host}"))??;

    let addrs: Vec<_> = lookup
        .iter()
        .map(|ip| std::net::SocketAddr::new(ip, remote_port))
        .collect();
    if addrs.is_empty() {
        anyhow::bail!("No DNS records for {remote_host}");
    }
    Ok(addrs)
}

/// Connect to the first address that answers, RFC 8305 style.
///
/// Attempts are started `CONNECT_ATTEMPT_DELAY` apart and race each other, so a
/// dead first record costs one stagger instead of a whole connect timeout. The
/// losing attempts are dropped as soon as one succeeds.
async fn connect_racing(
    addrs: &[std::net::SocketAddr],
    connect_timeout: Duration,
) -> Result<(TcpStream, std::net::SocketAddr), anyhow::Error> {
    let mut attempts = tokio::task::JoinSet::new();
    let mut last_err: Option<anyhow::Error> = None;

    for (i, addr) in addrs.iter().copied().enumerate() {
        let stagger = CONNECT_ATTEMPT_DELAY * i as u32;
        attempts.spawn(async move {
            if !stagger.is_zero() {
                tokio::time::sleep(stagger).await;
            }
            let result = match tokio::time::timeout(connect_timeout, TcpStream::connect(addr)).await
            {
                Ok(Ok(s)) => Ok(s),
                Ok(Err(e)) => Err(anyhow::anyhow!("connect {addr}: {e}")),
                Err(_) => Err(anyhow::anyhow!("connect timeout to {addr}")),
            };
            (addr, result)
        });
    }

    while let Some(joined) = attempts.join_next().await {
        match joined {
            Ok((addr, Ok(stream))) => {
                // Dropping the set aborts the attempts still in flight.
                return Ok((stream, addr));
            }
            Ok((_, Err(e))) => last_err = Some(e),
            Err(e) => last_err = Some(anyhow::anyhow!("connect task failed: {e}")),
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no addresses to connect to")))
}

/// Proxy a single TCP connection to remote destination.
/// Resolves hostname fresh each call for dynamic DNS support.
#[allow(clippy::too_many_arguments)]
async fn proxy_connection(
    inbound: TcpStream,
    peer_addr: std::net::SocketAddr,
    cfg: &ForwarderConfig,
    label: &str,
    resolver: &TokioResolver,
    stats: Arc<ForwarderStats>,
    remote_literal: Option<IpAddr>,
    knobs: KnobSnapshot,
) -> Result<(), anyhow::Error> {
    // Tune the client-side socket: disable Nagle (forwarded traffic is often
    // interactive) and optionally size the kernel buffers.
    tune_socket(
        &inbound,
        knobs.socket_buffer_bytes,
        label,
        peer_addr,
        "inbound",
    );

    let targets = resolve_targets(
        &cfg.remote_host,
        cfg.remote_port,
        remote_literal,
        resolver,
        knobs.dns_timeout,
    )
    .await?;

    let (mut outbound, remote_addr) = connect_racing(&targets, knobs.connect_timeout).await?;

    tune_socket(
        &outbound,
        knobs.socket_buffer_bytes,
        label,
        peer_addr,
        "outbound",
    );

    // Per-connection lifecycle is `debug`: at a few thousand connections/sec an
    // `info` pair per connection dominates the connect path and fills the log
    // volume. Aggregate throughput stays visible via the periodic report and
    // the metrics endpoint.
    debug!(forwarder = %label, peer = %peer_addr, remote = %remote_addr, "Connected");

    // Linux zero-copy fast path (opt-in): splice bytes kernel-side, no userspace copy.
    if knobs.use_splice {
        #[cfg(target_os = "linux")]
        {
            let r =
                splice::copy_bidirectional_splice(inbound, outbound, &stats, knobs.buffer_bytes)
                    .await
                    .map_err(anyhow::Error::from);
            if let Err(e) = &r {
                warn!(forwarder = %label, peer = %peer_addr, error = %e, "splice closed with error");
            }
            debug!(forwarder = %label, peer = %peer_addr, "Disconnected");
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
        knobs.buffer_bytes,
        knobs.buffer_bytes,
    )
    .await
    {
        debug!(forwarder = %label, peer = %peer_addr, error = %e, "Connection closed with error");
    }

    // Graceful shutdown: close both sides
    let _ = AsyncWriteExt::shutdown(&mut counting).await;
    let _ = outbound.shutdown().await;

    debug!(forwarder = %label, peer = %peer_addr, "Disconnected");

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
    // `tokio::time::interval` panics on a zero period. Settings validation
    // rejects that at startup, but this task must not be one bad value away
    // from silently dying and taking every throughput gauge with it.
    let mut interval = tokio::time::interval(sample_interval.max(Duration::from_secs(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
    // Same zero-period guard as the sampler: never let a config value panic
    // the reporter task.
    let mut interval = tokio::time::interval(report_interval.max(Duration::from_secs(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
    ///
    /// `SPLICE_F_MORE` is deliberately *not* set: it corks the destination
    /// socket the way `MSG_MORE` does, which would undo the `TCP_NODELAY` this
    /// forwarder sets on both sides for interactive traffic.
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
    /// `dst`'s write side. `on_bytes` is invoked with each chunk actually
    /// delivered to `dst`.
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

            // pipe -> dst. Every byte read into the pipe must leave it before
            // the next read: a pipe left holding data can make the next
            // `splice(src -> pipe)` return EAGAIN while `src` is still
            // readable, and tokio's edge-triggered readiness would then never
            // re-arm — a silent stall. Bailing out with an error instead of
            // `break`ing keeps that invariant true on every path.
            let mut left = n;
            while left > 0 {
                dst.writable().await?;
                match dst.try_io(Interest::WRITABLE, || splice_raw(pipe.r, dst_fd, left)) {
                    Ok(0) => {
                        // The destination accepted nothing and reported no
                        // error: it cannot take the rest of this chunk, and the
                        // undrained pipe makes further reads unsafe.
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "splice wrote 0 bytes to the destination socket",
                        ));
                    }
                    Ok(m) => {
                        left -= m;
                        // Count on delivery, not on read-into-pipe, so the
                        // counters never credit bytes that never reached the peer.
                        on_bytes(m as u64);
                    }
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

        // `try_join!` short-circuits on the first error, and that is required,
        // not merely convenient.
        //
        // An error on one direction means the connection is already broken. If
        // the other direction were left running (plain `join!`), it would park
        // on `readable()` against a backend that is itself waiting for data or
        // EOF from us — and since neither socket is dropped until this function
        // returns, neither side ever moves. That deadlock leaks the connection,
        // its permit, and its active-connection count until the shutdown drain
        // times out. Returning here drops both sockets, which is what lets the
        // peer observe the close.
        //
        // A clean half-close is unaffected: `Ok` does not short-circuit, so both
        // directions still run to completion on the normal path.
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

    // ---- accept-error classification ---------------------------------------

    #[test]
    fn transient_accept_errors_are_retried_immediately() {
        use std::io::{Error, ErrorKind};
        // A peer that vanished between SYN and accept is one connection's
        // problem; backing off would add a second of latency for everyone else.
        for kind in [
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::Interrupted,
            ErrorKind::NotConnected,
        ] {
            assert!(
                is_transient_accept_error(&Error::new(kind, "x")),
                "{kind:?} should be transient"
            );
        }
    }

    #[test]
    fn resource_exhaustion_accept_errors_trigger_backoff() {
        use std::io::{Error, ErrorKind};
        // EMFILE/ENFILE do not clear on their own: retrying instantly is a
        // 100%-CPU busy loop.
        for kind in [ErrorKind::Other, ErrorKind::PermissionDenied] {
            assert!(
                !is_transient_accept_error(&Error::new(kind, "x")),
                "{kind:?} should back off"
            );
        }
    }

    // ---- connection admission ----------------------------------------------

    #[test]
    fn admission_is_unbounded_without_a_limit() {
        let stats = ForwarderStats::new("t".into());
        for _ in 0..1000 {
            assert!(matches!(
                acquire_permit(&None, &stats),
                Admission::Admitted(None)
            ));
        }
        assert_eq!(stats.snapshot().rejected_connections, 0);
    }

    #[test]
    fn admission_sheds_once_the_limit_is_saturated() {
        let stats = ForwarderStats::new("t".into());
        let limit = Some(Arc::new(Semaphore::new(2)));

        let a = acquire_permit(&limit, &stats);
        let b = acquire_permit(&limit, &stats);
        assert!(matches!(a, Admission::Admitted(Some(_))));
        assert!(matches!(b, Admission::Admitted(Some(_))));

        // Third connection is refused, and the rejection is observable.
        assert!(matches!(
            acquire_permit(&limit, &stats),
            Admission::Rejected
        ));
        assert_eq!(stats.snapshot().rejected_connections, 1);

        // Releasing a permit reopens exactly one slot. The permit must stay
        // bound: dropping it at the end of the statement would silently hand
        // the slot straight back and make this assertion meaningless.
        drop(a);
        let c = acquire_permit(&limit, &stats);
        assert!(matches!(c, Admission::Admitted(Some(_))));
        assert!(matches!(
            acquire_permit(&limit, &stats),
            Admission::Rejected
        ));
        assert_eq!(stats.snapshot().rejected_connections, 2);
        drop((b, c));
    }

    // ---- target resolution --------------------------------------------------

    fn test_resolver() -> TokioResolver {
        TokioResolver::builder_tokio().unwrap().build().unwrap()
    }

    #[tokio::test]
    async fn ip_literals_skip_the_resolver() {
        // An IP-literal remote must never touch DNS: that is both a per-connection
        // latency saving and the reason a literal keeps working with no resolver
        // reachable at all.
        for (host, expect) in [
            ("127.0.0.1", "127.0.0.1:443"),
            ("::1", "[::1]:443"),
            ("192.168.7.9", "192.168.7.9:443"),
        ] {
            let literal = host.parse().ok();
            assert!(literal.is_some(), "{host} should parse as an IP");
            let out = resolve_targets(
                host,
                443,
                literal,
                &test_resolver(),
                Duration::from_millis(1), // would time out if DNS were consulted
            )
            .await
            .unwrap_or_else(|e| panic!("{host} must resolve locally: {e}"));
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].to_string(), expect);
        }
    }

    #[tokio::test]
    async fn dns_timeout_is_reported_not_hung() {
        // `.invalid` is reserved by RFC 6761 and never resolves. Whether the
        // resolver refuses it or the deadline fires, the call must return an
        // error promptly rather than parking the connection task.
        let started = std::time::Instant::now();
        let result = resolve_targets(
            "nonexistent.invalid",
            80,
            None,
            &test_resolver(),
            Duration::from_secs(2),
        )
        .await;
        assert!(result.is_err(), "unresolvable host must fail");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "resolution must be bounded by the timeout"
        );
    }

    // ---- connect racing -----------------------------------------------------

    /// An address with nothing listening, on the loopback.
    async fn dead_addr() -> std::net::SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    }

    #[tokio::test]
    async fn connect_racing_picks_the_only_live_address() {
        let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_addr = live.local_addr().unwrap();
        let dead = dead_addr().await;

        // Dead record first: the old sequential loop waited a full connect
        // timeout on it before trying the next one.
        let (stream, chosen) = connect_racing(&[dead, live_addr], Duration::from_secs(5))
            .await
            .expect("failover must reach the live address");
        assert_eq!(chosen, live_addr);
        assert_eq!(stream.peer_addr().unwrap(), live_addr);
    }

    #[tokio::test]
    async fn connect_racing_fails_when_every_address_is_dead() {
        let a = dead_addr().await;
        let b = dead_addr().await;
        let err = connect_racing(&[a, b], Duration::from_secs(2))
            .await
            .expect_err("all-dead must be an error");
        assert!(err.to_string().contains("connect"), "{err}");
    }

    #[tokio::test]
    async fn connect_racing_rejects_an_empty_address_list() {
        let err = connect_racing(&[], Duration::from_secs(1))
            .await
            .expect_err("empty list must error rather than hang");
        assert!(err.to_string().contains("no addresses"), "{err}");
    }

    #[tokio::test]
    async fn connect_racing_does_not_wait_for_a_hung_first_address() {
        // 198.51.100.0/24 (TEST-NET-2) is unroutable, so the first attempt
        // stalls. Happy-Eyeballs staggering must still reach the live address
        // well inside the connect timeout.
        let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_addr = live.local_addr().unwrap();
        let blackhole: std::net::SocketAddr = "198.51.100.7:9".parse().unwrap();

        let started = std::time::Instant::now();
        let (_s, chosen) = connect_racing(&[blackhole, live_addr], Duration::from_secs(30))
            .await
            .expect("must fail over to the live address");
        assert_eq!(chosen, live_addr);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "failover took {:?}; staggering is not working",
            started.elapsed()
        );
    }
}
