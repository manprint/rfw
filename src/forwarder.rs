use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::TokioResolver;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::config::{ForwarderConfig, KeepaliveConfig, KnobSnapshot, RuntimeKnobs};
use crate::notify::{Event, Notifier};
use crate::stats::{format_bytes, CountingStream, ForwarderStats, StatsRegistry};

/// Stagger between parallel connect attempts to successive resolved addresses
/// (RFC 8305 "Happy Eyeballs" connection attempt delay). Small enough that a
/// dead first record costs ~250 ms instead of a full connect timeout, large
/// enough that a healthy first record almost always wins on its own.
const CONNECT_ATTEMPT_DELAY: Duration = Duration::from_millis(250);

/// Backoff between attempts to bind a local address that is taken.
const BIND_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Local gate on the "connection limit reached" alert: the accept loop is a
/// hot path, so the check is one `Instant` comparison, not a channel send per
/// rejected connection. The notifier throttles further downstream.
const LIMIT_ALERT_SPACING: Duration = Duration::from_secs(60);

/// Apply per-socket tuning: disable Nagle, (optionally) size the kernel
/// send/receive buffers, and (optionally) arm TCP keepalive. Failures are
/// logged but non-fatal.
fn tune_socket(
    stream: &TcpStream,
    socket_buffer_bytes: Option<usize>,
    keepalive: Option<KeepaliveConfig>,
    label: &str,
    peer: std::net::SocketAddr,
    which: &str,
) {
    if let Err(e) = stream.set_nodelay(true) {
        warn!(forwarder = %label, peer = %peer, error = %e, "set_nodelay({which}) failed");
    }
    let sock = socket2::SockRef::from(stream);
    if let Some(sz) = socket_buffer_bytes {
        if let Err(e) = sock.set_recv_buffer_size(sz) {
            warn!(forwarder = %label, peer = %peer, error = %e, "set_recv_buffer_size({which}) failed");
        }
        if let Err(e) = sock.set_send_buffer_size(sz) {
            warn!(forwarder = %label, peer = %peer, error = %e, "set_send_buffer_size({which}) failed");
        }
    }
    // Keepalive is what turns a half-open connection (peer gone without a
    // FIN/RST: crashed client, dropped VPN/NAT mapping, remote failover) into
    // an error the copy loop can see. Without it the connection lives until
    // the other side happens to write, which for an idle DB session can be
    // never, and it keeps holding its max_connections permit, its backend
    // session and its copy buffers the whole time.
    if let Some(ka) = keepalive {
        if let Err(e) = sock.set_tcp_keepalive(&keepalive_params(ka)) {
            warn!(forwarder = %label, peer = %peer, error = %e, "set_tcp_keepalive({which}) failed");
        }
    }
}

/// Translate the configured keepalive into socket2 parameters.
///
/// `TCP_KEEPINTVL` / `TCP_KEEPCNT` are not settable on every platform socket2
/// supports. Where they are not, only the idle time is applied and the OS
/// defaults govern the probe cadence; a dead peer is still detected, just
/// later.
fn keepalive_params(ka: KeepaliveConfig) -> socket2::TcpKeepalive {
    #[allow(unused_mut)]
    let mut params = socket2::TcpKeepalive::new().with_time(ka.idle);
    #[cfg(any(
        target_os = "android",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
    ))]
    {
        params = params.with_interval(ka.interval).with_retries(ka.retries);
    }
    params
}

/// RAII marker for "this forwarder's listener is bound and accepting".
///
/// Created right after a successful bind and dropped on every exit path of the
/// accept loop (cancel, panic), so the health route can never keep reporting a
/// listener that is gone.
struct ListeningGuard(Arc<ForwarderStats>);

impl ListeningGuard {
    fn new(stats: Arc<ForwarderStats>) -> Self {
        stats.set_listening(true);
        Self(stats)
    }
}

impl Drop for ListeningGuard {
    fn drop(&mut self) {
        self.0.set_listening(false);
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
    notifier: Notifier,
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
    let mut bind_failures: u32 = 0;
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
                        bind_failures = bind_failures.saturating_add(1);
                        error!(forwarder = %label, error = %e, attempt = bind_failures, "Bind failed, retry in 5s");
                        // Every failure is reported; the notifier folds repeats
                        // into one message per throttle window.
                        notifier.notify(Event::BindFailed {
                            label: label.to_string(),
                            local_addr: local_str.clone(),
                            error: e.to_string(),
                            attempts: bind_failures,
                        });
                        if cancelled(&cancel, BIND_RETRY_DELAY).await {
                            return;
                        }
                    }
                }
            }
        }
    };

    info!(forwarder = %label, "Listening on {} -> {}:{}", local_str, cfg.remote_host, cfg.remote_port);
    if bind_failures > 0 {
        notifier.notify(Event::BindRecovered {
            label: label.to_string(),
            local_addr: local_str.clone(),
            failed_attempts: bind_failures,
        });
    }
    let mut last_limit_alert: Option<Instant> = None;

    // Flip the health flag for as long as the accept loop below is alive.
    let _listening = ListeningGuard::new(stats.clone());

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
                                if last_limit_alert.is_none_or(|t| t.elapsed() >= LIMIT_ALERT_SPACING) {
                                    last_limit_alert = Some(Instant::now());
                                    warn!(forwarder = %label, "max_connections saturated; refusing new connections");
                                    notifier.notify(Event::ConnectionLimitReached {
                                        label: label.to_string(),
                                        rejected_total: stats.snapshot().rejected_connections,
                                    });
                                }
                                continue;
                            }
                        };

                        let fwd_cfg = cfg.clone();
                        let resolver = resolver.clone();
                        let s = stats.clone();
                        let conn_label = label.clone();
                        let conn_notifier = notifier.clone();
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
                                remote_literal, knobs, &conn_notifier,
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
pub(crate) async fn resolve_targets(
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
pub(crate) async fn connect_racing(
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
    notifier: &Notifier,
) -> Result<(), anyhow::Error> {
    // Tune the client-side socket: disable Nagle (forwarded traffic is often
    // interactive) and optionally size the kernel buffers.
    tune_socket(
        &inbound,
        knobs.socket_buffer_bytes,
        knobs.keepalive,
        label,
        peer_addr,
        "inbound",
    );

    let targets = match resolve_targets(
        &cfg.remote_host,
        cfg.remote_port,
        remote_literal,
        resolver,
        knobs.dns_timeout,
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            // Lets the monitor start its downtime clock now, not at its next tick.
            stats.mark_connect_failed();
            return Err(e);
        }
    };

    // Report a DNS change the moment real traffic sees it: the address set is
    // shared with the monitor, so exactly one of them raises the alert.
    if remote_literal.is_none() {
        let ips: Vec<IpAddr> = targets.iter().map(|a| a.ip()).collect();
        if let Some(old) = stats.observe_addrs(&ips) {
            let mut new = ips;
            new.sort_unstable();
            new.dedup();
            info!(forwarder = %label, host = %cfg.remote_host, old = ?old, new = ?new, "Target DNS answer changed");
            notifier.notify(Event::DnsChanged {
                label: label.to_string(),
                host: cfg.remote_host.clone(),
                old,
                new,
            });
        }
    }

    let (mut outbound, remote_addr) = match connect_racing(&targets, knobs.connect_timeout).await {
        Ok(c) => {
            stats.mark_connect_ok();
            c
        }
        Err(e) => {
            stats.mark_connect_failed();
            return Err(e);
        }
    };

    tune_socket(
        &outbound,
        knobs.socket_buffer_bytes,
        knobs.keepalive,
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
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::config::Settings;
    use crate::testutil::{fake_dns_response, resolver_for, spawn_fake_dns};

    // ---- socket tuning ------------------------------------------------------

    /// A connected loopback pair: (accepted server side, client side).
    async fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (server, client)
    }

    #[tokio::test]
    async fn keepalive_is_armed_on_the_socket_when_configured() {
        let (server, _client) = socket_pair().await;
        let peer = server.peer_addr().unwrap();
        let ka = KeepaliveConfig {
            idle: Duration::from_secs(45),
            interval: Duration::from_secs(7),
            retries: 4,
        };
        tune_socket(&server, None, Some(ka), "t", peer, "inbound");

        let sock = socket2::SockRef::from(&server);
        assert!(
            sock.keepalive().unwrap(),
            "SO_KEEPALIVE must be enabled: this is what reaps half-open peers"
        );
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            assert_eq!(sock.tcp_keepalive_time().unwrap(), Duration::from_secs(45));
            assert_eq!(
                sock.tcp_keepalive_interval().unwrap(),
                Duration::from_secs(7)
            );
            assert_eq!(sock.tcp_keepalive_retries().unwrap(), 4);
        }
        assert!(server.nodelay().unwrap(), "TCP_NODELAY must still be set");
    }

    #[tokio::test]
    async fn keepalive_stays_off_when_disabled() {
        // `keepalive_idle_secs: 0` is the documented off switch; the socket
        // must then be left at the OS default (off).
        let (server, _client) = socket_pair().await;
        let peer = server.peer_addr().unwrap();
        tune_socket(&server, None, None, "t", peer, "inbound");
        assert!(!socket2::SockRef::from(&server).keepalive().unwrap());
    }

    #[test]
    fn default_knobs_arm_keepalive() {
        // Production default: on. A forwarder in front of a database with
        // pooled, idle-for-hours clients cannot afford half-open leaks.
        let snap = RuntimeKnobs::from_settings(&Settings::default()).snapshot();
        assert!(snap.keepalive.is_some());
    }

    // ---- listening flag -----------------------------------------------------

    /// Everything `run_forwarder` needs besides the config, with the resolver
    /// left to the caller.
    fn forwarder_deps(settings: &Settings) -> (Arc<RuntimeKnobs>, TaskTracker, CancellationToken) {
        (
            Arc::new(RuntimeKnobs::from_settings(settings)),
            TaskTracker::new(),
            CancellationToken::new(),
        )
    }

    fn system_resolver() -> Arc<TokioResolver> {
        Arc::new(test_resolver())
    }

    /// Poll until the listening flag equals `want`, or fail after ~3s.
    async fn wait_listening(stats: &ForwarderStats, want: bool) {
        for _ in 0..150 {
            if stats.snapshot().listening == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("listening flag never became {want}");
    }

    #[tokio::test]
    async fn listening_flag_tracks_the_accept_loop_lifetime() {
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            p
        };
        let cfg = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port: port,
            remote_host: "127.0.0.1".into(),
            remote_port: 9,
        };
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        assert!(!stats.snapshot().listening, "not bound yet");

        let (knobs, tracker, cancel) = forwarder_deps(&Settings::default());
        let task = tokio::spawn(run_forwarder(
            cfg,
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs,
            tracker,
            None,
            Notifier::disabled(),
        ));

        wait_listening(&stats, true).await;

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("accept loop must exit on cancel")
            .unwrap();
        assert!(
            !stats.snapshot().listening,
            "the guard must clear the flag once the accept loop is gone"
        );
    }

    #[tokio::test]
    async fn listening_flag_stays_off_while_the_port_is_taken() {
        // The bind-retry loop is the "running but not serving" state the
        // health route exists to expose.
        let blocker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = blocker.local_addr().unwrap().port();
        let cfg = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port: port,
            remote_host: "127.0.0.1".into(),
            remote_port: 9,
        };
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (knobs, tracker, cancel) = forwarder_deps(&Settings::default());
        let task = tokio::spawn(run_forwarder(
            cfg,
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs,
            tracker,
            None,
            Notifier::disabled(),
        ));

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !stats.snapshot().listening,
            "must not claim to listen on a taken port"
        );

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("bind-retry loop must exit on cancel")
            .unwrap();
        drop(blocker);
    }

    // ---- DNS change propagation --------------------------------------------
    //
    // The production use case is a database behind a cloud DNS name whose IP
    // changes on failover (RDS-style, TTL of a few seconds). These tests stand
    // up a fake authoritative resolver inside the test, point a real
    // `TokioResolver` at it with the production `apply_resolver_policy`, and
    // drive `run_forwarder` against a hostname whose answer is flipped
    // underneath it.

    /// Backend that answers the first bytes of every connection with `tag`
    /// and then echoes each further chunk prefixed with `tag:`.
    fn spawn_tagged_backend(listener: TcpListener, tag: &'static str) {
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0_u8; 256];
                    if sock.read(&mut buf).await.is_err() {
                        return;
                    }
                    if sock.write_all(format!("{tag}\n").as_bytes()).await.is_err() {
                        return;
                    }
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                let mut out = format!("{tag}:").into_bytes();
                                out.extend_from_slice(&buf[..n]);
                                if sock.write_all(&out).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
    }

    /// Two backends on *different loopback IPs* but the *same port*, because a
    /// forwarder's remote port is fixed and only the DNS answer moves.
    /// Linux routes the whole 127/8 to loopback, so 127.0.0.2 is bindable.
    #[cfg(target_os = "linux")]
    async fn two_backends_same_port() -> (Ipv4Addr, Ipv4Addr, u16) {
        let a_ip = Ipv4Addr::new(127, 0, 0, 1);
        let b_ip = Ipv4Addr::new(127, 0, 0, 2);
        for _ in 0..50 {
            let b = TcpListener::bind((b_ip, 0)).await.unwrap();
            let port = b.local_addr().unwrap().port();
            if let Ok(a) = TcpListener::bind((a_ip, port)).await {
                spawn_tagged_backend(a, "SRV-A");
                spawn_tagged_backend(b, "SRV-B");
                return (a_ip, b_ip, port);
            }
            // Port taken on 127.0.0.1: try another.
        }
        panic!("could not find one port free on both loopback addresses");
    }

    /// Connect through the forwarder, send a greeting, return the backend tag.
    async fn probe_tag(port: u16) -> String {
        let mut c = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(3), c.read(&mut buf))
            .await
            .expect("backend must answer")
            .unwrap();
        String::from_utf8_lossy(&buf[..n]).trim().to_string()
    }

    #[tokio::test]
    async fn fake_dns_is_understood_by_the_real_resolver() {
        // Sanity check for the harness itself: hickory must accept the hand
        // rolled response and hand back the address it carries.
        let answer = Arc::new(AtomicU32::new(u32::from(Ipv4Addr::new(10, 1, 2, 3))));
        let count = Arc::new(AtomicUsize::new(0));
        let dns = spawn_fake_dns(answer.clone(), count.clone(), 300).await;
        let resolver = resolver_for(dns);

        let out = resolve_targets("db.test", 5432, None, &resolver, Duration::from_secs(3))
            .await
            .expect("fake DNS answer must resolve");
        assert_eq!(out, vec!["10.1.2.3:5432".parse::<SocketAddr>().unwrap()]);
        assert_eq!(count.load(Ordering::Relaxed), 1);

        // Second lookup inside the TTL is served from hickory's cache: per
        // connection resolution does not mean per connection network traffic.
        let _ = resolve_targets("db.test", 5432, None, &resolver, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(
            count.load(Ordering::Relaxed),
            1,
            "must hit the cache within TTL"
        );
    }

    #[tokio::test]
    async fn fake_dns_response_shape() {
        // 12-byte header + "db.test" QNAME + QTYPE A + QCLASS IN.
        let mut q = vec![0xAB, 0xCD, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
        q.extend_from_slice(&[2, b'd', b'b', 4, b't', b'e', b's', b't', 0]);
        q.extend_from_slice(&[0, 1, 0, 1]);
        let r = fake_dns_response(&q, Ipv4Addr::new(1, 2, 3, 4), 7).unwrap();
        assert_eq!(&r[..2], &[0xAB, 0xCD], "ID echoed");
        assert_eq!(r[7], 1, "one answer for an A question");
        assert_eq!(&r[r.len() - 4..], &[1, 2, 3, 4], "RDATA is the IP");
        assert_eq!(&r[r.len() - 10..r.len() - 6], &7_u32.to_be_bytes(), "TTL");

        // AAAA gets an empty NOERROR, never an A record.
        let l = q.len();
        q[l - 3] = 28;
        let r = fake_dns_response(&q, Ipv4Addr::new(1, 2, 3, 4), 7).unwrap();
        assert_eq!(r[7], 0);
        assert!(fake_dns_response(&q[..5], Ipv4Addr::LOCALHOST, 1).is_none());
    }

    /// The headline guarantee for the database-behind-DNS use case: when the
    /// record changes, the *next* connection goes to the new address without a
    /// restart, while connections already open stay pinned to the old one.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dns_change_is_followed_by_new_connections_without_restart() {
        let (a_ip, b_ip, backend_port) = two_backends_same_port().await;
        let answer = Arc::new(AtomicU32::new(u32::from(a_ip)));
        let a_queries = Arc::new(AtomicUsize::new(0));
        const TTL: u32 = 1;
        let dns = spawn_fake_dns(answer.clone(), a_queries.clone(), TTL).await;

        let local_port = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            p
        };
        let cfg = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port,
            remote_host: "db.test".into(),
            remote_port: backend_port,
        };
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (knobs, tracker, cancel) = forwarder_deps(&Settings {
            dns_timeout_secs: 2,
            connect_timeout_secs: 2,
            ..Settings::default()
        });
        let task = tokio::spawn(run_forwarder(
            cfg,
            cancel.clone(),
            stats.clone(),
            Arc::new(resolver_for(dns)),
            knobs,
            tracker,
            None,
            Notifier::disabled(),
        ));
        wait_listening(&stats, true).await;

        // Phase 1: record -> A. Several connections in a burst share one
        // lookup thanks to the TTL cache.
        for _ in 0..3 {
            assert_eq!(probe_tag(local_port).await, "SRV-A");
        }
        assert!(
            a_queries.load(Ordering::Relaxed) <= 2,
            "burst of 3 connections must not mean 3 DNS round trips (got {})",
            a_queries.load(Ordering::Relaxed)
        );

        // Open a connection and keep it: it must stay on A after the flip.
        let mut held = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
        held.write_all(b"hold").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = held.read(&mut buf).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&buf[..n]).trim(), "SRV-A");

        // Flip the record -> B. No restart, no reload, no signal.
        answer.store(u32::from(b_ip), Ordering::Relaxed);

        // New connections must move over once the cached record expires.
        let flipped_at = std::time::Instant::now();
        let mut switched = false;
        while flipped_at.elapsed() < Duration::from_secs(6) {
            if probe_tag(local_port).await == "SRV-B" {
                switched = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(switched, "new connections must follow the DNS change");
        assert!(
            flipped_at.elapsed() < Duration::from_secs(TTL as u64 + 3),
            "switch took {:?}; should be bounded by the record TTL",
            flipped_at.elapsed()
        );

        // The connection opened before the flip is still talking to A.
        held.write_all(b"still").await.unwrap();
        let n = held.read(&mut buf).await.unwrap();
        assert_eq!(
            &buf[..n],
            b"SRV-A:still",
            "in-flight connections stay pinned"
        );
        drop(held);

        // And back again: the path is symmetric, not a one-shot.
        answer.store(u32::from(a_ip), Ordering::Relaxed);
        let mut back = false;
        let t = std::time::Instant::now();
        while t.elapsed() < Duration::from_secs(6) {
            if probe_tag(local_port).await == "SRV-A" {
                back = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(back, "must follow the record back to A");

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    /// A record pointing at a dead address must fail the client within the
    /// connect timeout and recover as soon as the record is fixed, with no
    /// leftover active-connection count.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dns_pointing_at_a_dead_address_fails_fast_and_recovers() {
        let (a_ip, _b_ip, backend_port) = two_backends_same_port().await;
        // 127.0.0.3 has nothing listening on backend_port: connect is refused.
        let dead_ip = Ipv4Addr::new(127, 0, 0, 3);
        let answer = Arc::new(AtomicU32::new(u32::from(dead_ip)));
        let dns = spawn_fake_dns(answer.clone(), Arc::new(AtomicUsize::new(0)), 1).await;

        let local_port = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            p
        };
        let cfg = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port,
            remote_host: "db.test".into(),
            remote_port: backend_port,
        };
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (knobs, tracker, cancel) = forwarder_deps(&Settings {
            dns_timeout_secs: 2,
            connect_timeout_secs: 2,
            ..Settings::default()
        });
        let task = tokio::spawn(run_forwarder(
            cfg,
            cancel.clone(),
            stats.clone(),
            Arc::new(resolver_for(dns)),
            knobs,
            tracker,
            None,
            Notifier::disabled(),
        ));
        wait_listening(&stats, true).await;

        // Dead record: the client sees EOF quickly, not a hang.
        let started = std::time::Instant::now();
        let mut c = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
        let mut sink = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut sink))
            .await
            .expect("must not hang on a dead record")
            .ok();
        assert!(sink.is_empty());
        assert!(started.elapsed() < Duration::from_secs(4));

        // Record repaired: next connection works, nothing leaked.
        answer.store(u32::from(a_ip), Ordering::Relaxed);
        let mut ok = false;
        let t = std::time::Instant::now();
        while t.elapsed() < Duration::from_secs(6) {
            let mut c = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
            c.write_all(b"ping").await.unwrap();
            let mut buf = [0_u8; 64];
            if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(3), c.read(&mut buf)).await
            {
                if String::from_utf8_lossy(&buf[..n]).trim() == "SRV-A" {
                    ok = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(ok, "must recover once the record points somewhere alive");

        for _ in 0..50 {
            if stats.active_connections.load(Ordering::Relaxed) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(stats.active_connections.load(Ordering::Relaxed), 0);

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

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
