//! UDP forwarding: one listener per forwarder, one connected upstream socket
//! per client address (a "session").
//!
//! Client -> target runs inside the listener task and never awaits: datagrams
//! are drained in batches and written to the session's upstream socket with a
//! raw non-blocking send; a full socket buffer drops (and counts) the datagram
//! instead of stalling every other client. Target -> client runs in one task
//! per session. Sessions expire after `udp_session_timeout_secs` of silence.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, Read};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::TokioResolver;
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::config::{ForwarderConfig, RuntimeKnobs};
use crate::forwarder::{
    cancelled, resolve_targets, ConnGuard, ListeningGuard, BIND_RETRY_DELAY, LIMIT_ALERT_SPACING,
};
use crate::notify::{Event, Notifier};
use crate::stats::{ForwarderStats, IcmpKind};

/// Receive buffer: fits the largest UDP payload (65507 IPv4, 65527 IPv6).
const UDP_BUF_BYTES: usize = 65536;
/// Datagrams handled per listener wakeup before yielding to other tasks.
const LISTENER_BATCH: usize = 64;
/// Datagrams handled per session wakeup before yielding.
const SESSION_BATCH: usize = 64;
/// Cadence of the idle-session sweep (the timeout itself is read live).
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);
/// Cadence of the hostname refresh (hickory's cache answers until the TTL expires).
const DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// Upper bound on waiting for session tasks when a forwarder stops.
const SESSION_STOP_GRACE: Duration = Duration::from_secs(2);
/// At most one warning per this window for each kind of datagram drop.
const DROP_LOG_SPACING: Duration = Duration::from_secs(10);
/// At most one DNS-failure warning per this window.
const DNS_FAIL_LOG_SPACING: Duration = Duration::from_secs(30);
/// Pause after a non-transient listener receive error, so it cannot spin.
const RECV_ERROR_BACKOFF: Duration = Duration::from_millis(50);

thread_local! {
    /// Per-worker-thread reply buffer: sessions share it instead of each
    /// holding 64 KiB. Never borrowed across an `.await`.
    static REPLY_BUF: RefCell<Vec<u8>> = RefCell::new(vec![0_u8; UDP_BUF_BYTES]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpErr {
    WouldBlock,
    Transient,
    Icmp(IcmpKind),
    Fatal,
}

fn classify_udp_err(e: &io::Error) -> UdpErr {
    match e.kind() {
        io::ErrorKind::WouldBlock => UdpErr::WouldBlock,
        io::ErrorKind::Interrupted => UdpErr::Transient,
        io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset => {
            UdpErr::Icmp(IcmpKind::PortUnreachable)
        }
        io::ErrorKind::HostUnreachable => UdpErr::Icmp(IcmpKind::HostUnreachable),
        io::ErrorKind::NetworkUnreachable => UdpErr::Icmp(IcmpKind::NetworkUnreachable),
        _ => UdpErr::Fatal,
    }
}

/// Rate limit for one warning on a hot path.
#[derive(Debug, Default)]
struct LogGate {
    last: Option<Instant>,
    suppressed: u64,
}

impl LogGate {
    /// `Some(n)` = log now, `n` occurrences were suppressed since the last line.
    fn check(&mut self, spacing: Duration) -> Option<u64> {
        let now = Instant::now();
        if self.last.is_some_and(|t| now.duration_since(t) < spacing) {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

#[derive(Debug, Default)]
struct Gates {
    send_full: LogGate,
    no_target: LogGate,
    open_failed: LogGate,
    recv_failed: LogGate,
}

/// Counters accumulated over one batch, flushed with one atomic add each.
#[derive(Debug, Default)]
struct Batch {
    bytes: u64,
    datagrams: u64,
    dropped: u64,
}

/// State shared by the listener and a session's reply task.
struct SessionShared {
    client: SocketAddr,
    remote: SocketAddr,
    upstream: UdpSocket,
    /// `stats.now_ms()` of the last datagram in either direction.
    last_activity_ms: AtomicU64,
    /// The upstream socket failed for good; the listener reaps the session.
    dead: AtomicBool,
    /// Target -> client datagrams forwarded (close log only).
    rx_datagrams: AtomicU64,
    /// Keeps `active_connections` and the `udp_max_sessions` slot for exactly
    /// as long as the upstream socket exists.
    _guard: ConnGuard,
    _permit: OwnedSemaphorePermit,
}

impl SessionShared {
    fn touch(&self, now_ms: u64) {
        if self.last_activity_ms.load(Ordering::Relaxed) < now_ms {
            self.last_activity_ms.fetch_max(now_ms, Ordering::Relaxed);
        }
    }
}

/// Listener-owned bookkeeping for one session.
struct SessionEntry {
    shared: Arc<SessionShared>,
    /// Child of the forwarder token: cancelled on eviction or stop.
    cancel: CancellationToken,
    opened: Instant,
    /// Client -> target datagrams forwarded (close log only).
    tx_datagrams: u64,
}

fn close_entry(label: &str, e: &SessionEntry, reason: &'static str) {
    e.cancel.cancel();
    debug!(
        forwarder = %label,
        client = %e.shared.client,
        remote = %e.shared.remote,
        reason,
        duration_ms = u64::try_from(e.opened.elapsed().as_millis()).unwrap_or(u64::MAX),
        tx = e.tx_datagrams,
        rx = e.shared.rx_datagrams.load(Ordering::Relaxed),
        "UDP session closed"
    );
}

fn same_addr_set(a: &[SocketAddr], b: &[SocketAddr]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a.dedup();
    b.dedup();
    a == b
}

#[cfg(unix)]
fn is_fd_exhaustion(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
}
#[cfg(windows)]
fn is_fd_exhaustion(e: &io::Error) -> bool {
    e.raw_os_error() == Some(10024) // WSAEMFILE
}
#[cfg(not(any(unix, windows)))]
fn is_fd_exhaustion(_: &io::Error) -> bool {
    false
}

fn connect_upstream(remote: SocketAddr, sockbuf: Option<usize>) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(remote),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_nonblocking(true)?;
    if let Some(sz) = sockbuf {
        // Best effort, like the TCP path: the OS may cap or refuse the size.
        let _ = socket.set_recv_buffer_size(sz);
        let _ = socket.set_send_buffer_size(sz);
    }
    socket.connect(&remote.into())?;
    UdpSocket::from_std(socket.into())
}

fn tune_listener(socket: &UdpSocket, sockbuf: Option<usize>, label: &str) {
    let Some(want) = sockbuf else { return };
    let sock = socket2::SockRef::from(socket);
    if let Err(e) = sock.set_recv_buffer_size(want) {
        warn!(forwarder = %label, error = %e, "set_recv_buffer_size(udp listener) failed");
    }
    if let Err(e) = sock.set_send_buffer_size(want) {
        warn!(forwarder = %label, error = %e, "set_send_buffer_size(udp listener) failed");
    }
    if let Ok(got) = sock.recv_buffer_size() {
        if got < want {
            warn!(
                forwarder = %label,
                requested = want,
                effective = got,
                "UDP listener receive buffer capped by the kernel; raise net.core.rmem_max"
            );
        }
    }
}

/// Run one UDP forwarder until `cancel` fires. Returns only after every
/// session task has stopped, i.e. once the local port is free again.
pub async fn run_udp_forwarder(
    cfg: ForwarderConfig,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    udp_limit: Arc<Semaphore>,
    notifier: Notifier,
) {
    let cfg = Arc::new(cfg);
    let local_str = cfg.local_addr();
    let label: Arc<str> = Arc::from(cfg.label());
    let literal: Option<IpAddr> = cfg.remote_host.parse().ok();

    // Bind with auto-reconnect loop (same shape and messages as TCP).
    let mut bind_failures: u32 = 0;
    let socket = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(forwarder = %label, "Cancelled before bind");
                return;
            }
            result = UdpSocket::bind(&local_str) => {
                match result {
                    Ok(s) => break s,
                    Err(e) => {
                        bind_failures = bind_failures.saturating_add(1);
                        error!(forwarder = %label, error = %e, attempt = bind_failures, "Bind failed, retry in 5s");
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

    tune_listener(&socket, knobs.socket_buffer_bytes(), &label);
    info!(forwarder = %label, "Listening (udp) on {} -> {}:{}", local_str, cfg.remote_host, cfg.remote_port);
    if bind_failures > 0 {
        notifier.notify(Event::BindRecovered {
            label: label.to_string(),
            local_addr: local_str.clone(),
            failed_attempts: bind_failures,
        });
    }
    let _listening = ListeningGuard::new(stats.clone());

    let socket = Arc::new(socket);
    let tasks = TaskTracker::new();
    let initial: Arc<[SocketAddr]> = match literal {
        Some(ip) => Arc::from(vec![SocketAddr::new(ip, cfg.remote_port)]),
        None => Arc::from(Vec::new()),
    };
    let (tx, targets) = watch::channel(initial);
    let dns_watch = literal.is_none();
    if dns_watch {
        tasks.spawn(run_dns_refresher(
            cfg.clone(),
            label.clone(),
            stats.clone(),
            resolver,
            knobs.clone(),
            notifier.clone(),
            tx,
            cancel.clone(),
        ));
    } else {
        drop(tx);
    }

    let mut engine = Engine {
        label: label.clone(),
        socket,
        stats,
        knobs,
        udp_limit,
        notifier,
        cancel,
        tasks: tasks.clone(),
        targets,
        dns_watch,
        sessions: HashMap::new(),
        gates: Gates::default(),
        last_limit_alert: None,
    };
    let mut buf = vec![0_u8; UDP_BUF_BYTES];
    engine.run(&mut buf).await;
    engine.close_all("shutdown");
    drop(engine);
    tasks.close();
    if tokio::time::timeout(SESSION_STOP_GRACE, tasks.wait())
        .await
        .is_err()
    {
        warn!(
            forwarder = %label,
            "UDP sessions did not stop within {}s; the port may stay bound briefly",
            SESSION_STOP_GRACE.as_secs()
        );
    }
    info!(forwarder = %label, "Shutting down");
}

enum ListenerDrain {
    Idle,
    Full,
    Failed,
}

struct Engine {
    label: Arc<str>,
    socket: Arc<UdpSocket>,
    stats: Arc<ForwarderStats>,
    knobs: Arc<RuntimeKnobs>,
    udp_limit: Arc<Semaphore>,
    notifier: Notifier,
    cancel: CancellationToken,
    tasks: TaskTracker,
    targets: watch::Receiver<Arc<[SocketAddr]>>,
    dns_watch: bool,
    sessions: HashMap<SocketAddr, SessionEntry>,
    gates: Gates,
    last_limit_alert: Option<Instant>,
}

impl Engine {
    async fn run(&mut self, buf: &mut [u8]) {
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        sweep.tick().await; // the first tick is immediate
        loop {
            // `select!` drops every branch future before running the chosen
            // handler, so handlers may borrow `self` mutably.
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return,
                _ = sweep.tick() => self.sweep(),
                changed = self.targets.changed(), if self.dns_watch => match changed {
                    Ok(()) => self.on_targets_changed(),
                    Err(_) => self.dns_watch = false, // refresher gone (shutdown)
                },
                ready = self.socket.readable() => match ready {
                    Ok(()) => match self.drain_listener(buf) {
                        ListenerDrain::Idle => {}
                        ListenerDrain::Full => tokio::task::yield_now().await,
                        ListenerDrain::Failed => {
                            if cancelled(&self.cancel, RECV_ERROR_BACKOFF).await { return; }
                        }
                    },
                    Err(e) => {
                        if let Some(s) = self.gates.recv_failed.check(DROP_LOG_SPACING) {
                            warn!(forwarder = %self.label, error = %e, suppressed = s, "UDP listener readiness failed");
                        }
                        if cancelled(&self.cancel, RECV_ERROR_BACKOFF).await { return; }
                    }
                },
            }
        }
    }

    fn drain_listener(&mut self, buf: &mut [u8]) -> ListenerDrain {
        let now_ms = self.stats.now_ms();
        let mut batch = Batch::default();
        let mut outcome = ListenerDrain::Full;
        for _ in 0..LISTENER_BATCH {
            match self.socket.try_recv_from(buf) {
                Ok((n, client)) => self.on_client_datagram(client, &buf[..n], now_ms, &mut batch),
                Err(e) => match classify_udp_err(&e) {
                    UdpErr::WouldBlock => {
                        outcome = ListenerDrain::Idle;
                        break;
                    }
                    UdpErr::Transient | UdpErr::Icmp(_) => {}
                    UdpErr::Fatal => {
                        if let Some(s) = self.gates.recv_failed.check(DROP_LOG_SPACING) {
                            warn!(forwarder = %self.label, error = %e, suppressed = s, "UDP listener receive failed");
                        }
                        outcome = ListenerDrain::Failed;
                        break;
                    }
                },
            }
        }
        if batch.datagrams > 0 {
            self.stats.add_sent(batch.bytes);
            self.stats.add_datagrams_sent(batch.datagrams);
        }
        if batch.dropped > 0 {
            self.stats.add_datagrams_dropped(batch.dropped);
        }
        outcome
    }

    fn on_client_datagram(
        &mut self,
        client: SocketAddr,
        data: &[u8],
        now_ms: u64,
        batch: &mut Batch,
    ) {
        if let Some(entry) = self.sessions.get_mut(&client) {
            if !entry.shared.dead.load(Ordering::Relaxed) {
                forward_upstream(
                    entry,
                    data,
                    now_ms,
                    batch,
                    &self.stats,
                    &mut self.gates.send_full,
                    &self.label,
                );
                return;
            }
        }
        if let Some(dead) = self.sessions.remove(&client) {
            close_entry(&self.label, &dead, "upstream error");
        }
        if let Some(entry) = self.open_session(client, now_ms, batch) {
            let entry = self.sessions.entry(client).or_insert(entry);
            forward_upstream(
                entry,
                data,
                now_ms,
                batch,
                &self.stats,
                &mut self.gates.send_full,
                &self.label,
            );
        }
    }

    /// Synchronous on purpose: about four syscalls, no await (I-4).
    fn open_session(
        &mut self,
        client: SocketAddr,
        now_ms: u64,
        batch: &mut Batch,
    ) -> Option<SessionEntry> {
        // Separate statement: the watch read guard drops here, not after the else block.
        let first = self.targets.borrow().first().copied();
        let Some(remote) = first else {
            batch.dropped += 1;
            self.stats.mark_connect_failed();
            if let Some(s) = self.gates.no_target.check(DROP_LOG_SPACING) {
                warn!(forwarder = %self.label, suppressed = s, "UDP target not resolved yet; datagram dropped");
            }
            return None;
        };
        let permit = match self.udp_limit.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                batch.dropped += 1;
                self.stats.inc_rejected();
                debug!(forwarder = %self.label, client = %client, "UDP session rejected: udp_max_sessions reached");
                if self
                    .last_limit_alert
                    .is_none_or(|t| t.elapsed() >= LIMIT_ALERT_SPACING)
                {
                    self.last_limit_alert = Some(Instant::now());
                    warn!(forwarder = %self.label, "udp_max_sessions saturated; dropping datagrams from new clients");
                    self.notifier.notify(Event::UdpSessionLimitReached {
                        label: self.label.to_string(),
                        rejected_total: self.stats.rejected_connections.load(Ordering::Relaxed),
                    });
                }
                return None;
            }
        };
        let upstream = match connect_upstream(remote, self.knobs.socket_buffer_bytes()) {
            Ok(s) => s,
            Err(e) => {
                batch.dropped += 1;
                self.stats.mark_connect_failed();
                if let Some(s) = self.gates.open_failed.check(DROP_LOG_SPACING) {
                    if is_fd_exhaustion(&e) {
                        error!(forwarder = %self.label, error = %e, suppressed = s,
                            "UDP session open failed: out of file descriptors (raise the fd limit or lower udp_max_sessions)");
                    } else {
                        warn!(forwarder = %self.label, remote = %remote, error = %e, suppressed = s,
                            "UDP session open failed; datagram dropped");
                    }
                }
                return None; // `permit` drops here: the slot is released
            }
        };
        let local = upstream.local_addr().ok();
        let shared = Arc::new(SessionShared {
            client,
            remote,
            upstream,
            last_activity_ms: AtomicU64::new(now_ms),
            dead: AtomicBool::new(false),
            rx_datagrams: AtomicU64::new(0),
            _guard: ConnGuard::new(self.stats.clone()),
            _permit: permit,
        });
        let cancel = self.cancel.child_token();
        self.tasks.spawn(run_session(
            shared.clone(),
            self.socket.clone(),
            self.stats.clone(),
            self.label.clone(),
            cancel.clone(),
        ));
        debug!(forwarder = %self.label, client = %client, local = ?local, remote = %remote, "UDP session opened");
        Some(SessionEntry {
            shared,
            cancel,
            opened: Instant::now(),
            tx_datagrams: 0,
        })
    }

    fn sweep(&mut self) {
        if self.sessions.is_empty() {
            return;
        }
        let timeout_ms =
            u64::try_from(self.knobs.udp_session_timeout().as_millis()).unwrap_or(u64::MAX);
        let now_ms = self.stats.now_ms();
        let label = &self.label;
        self.sessions.retain(|_, e| {
            let reason = if e.shared.dead.load(Ordering::Relaxed) {
                Some("upstream error")
            } else if now_ms.saturating_sub(e.shared.last_activity_ms.load(Ordering::Relaxed))
                >= timeout_ms
            {
                Some("idle timeout")
            } else {
                None
            };
            match reason {
                Some(r) => {
                    close_entry(label, e, r);
                    false
                }
                None => true,
            }
        });
    }

    fn on_targets_changed(&mut self) {
        let set: Arc<[SocketAddr]> = self.targets.borrow_and_update().clone();
        let before = self.sessions.len();
        let label = &self.label;
        self.sessions.retain(|_, e| {
            if set.contains(&e.shared.remote) {
                return true;
            }
            close_entry(label, e, "target address left DNS");
            false
        });
        let closed = before - self.sessions.len();
        if closed > 0 {
            info!(forwarder = %self.label, closed, new = ?set,
                "UDP target DNS changed; closed sessions to addresses no longer published");
        }
    }

    fn close_all(&mut self, reason: &'static str) {
        for (_, e) in self.sessions.drain() {
            close_entry(&self.label, &e, reason);
        }
    }
}

/// Client -> target for one datagram. Never awaits (I-4); the raw send also
/// works on a socket registered a moment ago (I-14).
fn forward_upstream(
    entry: &mut SessionEntry,
    data: &[u8],
    now_ms: u64,
    batch: &mut Batch,
    stats: &ForwarderStats,
    gate: &mut LogGate,
    label: &str,
) {
    let shared = &entry.shared;
    match socket2::SockRef::from(&shared.upstream).send(data) {
        Ok(_) => {
            entry.tx_datagrams += 1;
            batch.datagrams += 1;
            batch.bytes += data.len() as u64;
            shared.touch(now_ms);
        }
        Err(e) => {
            batch.dropped += 1;
            match classify_udp_err(&e) {
                UdpErr::WouldBlock => {
                    if let Some(s) = gate.check(DROP_LOG_SPACING) {
                        warn!(forwarder = %label, client = %shared.client, suppressed = s,
                            "UDP upstream send buffer full; datagram dropped (consider socket_buffer_bytes)");
                    }
                }
                UdpErr::Icmp(kind) => {
                    stats.mark_udp_icmp_error(now_ms, kind);
                    debug!(forwarder = %label, client = %shared.client, remote = %shared.remote, error = %e,
                        "UDP send reported an ICMP error from the target; datagram dropped");
                }
                UdpErr::Transient | UdpErr::Fatal => {
                    debug!(forwarder = %label, client = %shared.client, error = %e,
                        "UDP upstream send failed; datagram dropped");
                }
            }
        }
    }
}

enum SessionDrain {
    Idle,
    Full,
    Blocked(Vec<u8>),
    Dead,
}

async fn run_session(
    shared: Arc<SessionShared>,
    listener: Arc<UdpSocket>,
    stats: Arc<ForwarderStats>,
    label: Arc<str>,
    cancel: CancellationToken,
) {
    loop {
        let ready = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            r = shared.upstream.ready(Interest::READABLE | Interest::ERROR) => r,
        };
        if let Err(e) = ready {
            shared.dead.store(true, Ordering::Relaxed);
            debug!(forwarder = %label, client = %shared.client, error = %e, "UDP session readiness failed; session marked dead");
            return;
        }
        match drain_upstream(&shared, &listener, &stats, &label) {
            SessionDrain::Idle => {}
            SessionDrain::Full => tokio::task::yield_now().await,
            SessionDrain::Dead => return,
            SessionDrain::Blocked(pending) => {
                let sent = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    r = listener.send_to(&pending, shared.client) => r,
                };
                match sent {
                    Ok(_) => {
                        shared.rx_datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.add_received(pending.len() as u64);
                        stats.add_datagrams_received(1);
                    }
                    Err(e) => {
                        stats.add_datagrams_dropped(1);
                        debug!(forwarder = %label, client = %shared.client, error = %e, "UDP reply send failed; datagram dropped");
                    }
                }
            }
        }
    }
}

/// One non-blocking upstream receive. An ICMP error is reported as EPOLLERR
/// alone and must be consumed by a receive, or `ready()` would spin. tokio's
/// `try_io` only honours single-flag interests (a combined interest maps to an
/// empty readiness mask and never runs the closure), so READABLE is tried
/// first and ERROR second; each clears its own readiness on `WouldBlock`.
fn recv_upstream(
    upstream: &UdpSocket,
    sock: &socket2::SockRef<'_>,
    buf: &mut [u8],
) -> io::Result<usize> {
    match upstream.try_io(Interest::READABLE, || (&**sock).read(&mut *buf)) {
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            upstream.try_io(Interest::ERROR, || (&**sock).read(buf))
        }
        res => res,
    }
}

/// Target -> client, synchronous: the thread-local buffer is never held
/// across an await.
fn drain_upstream(
    shared: &SessionShared,
    listener: &UdpSocket,
    stats: &ForwarderStats,
    label: &str,
) -> SessionDrain {
    REPLY_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        let now_ms = stats.now_ms();
        let sock = socket2::SockRef::from(&shared.upstream);
        let mut batch = Batch::default();
        let mut replied = false;
        let mut outcome = SessionDrain::Full;
        for _ in 0..SESSION_BATCH {
            let res = recv_upstream(&shared.upstream, &sock, &mut buf[..]);
            match res {
                Ok(n) => {
                    replied = true;
                    match listener.try_send_to(&buf[..n], shared.client) {
                        Ok(_) => { batch.datagrams += 1; batch.bytes += n as u64; }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            outcome = SessionDrain::Blocked(buf[..n].to_vec());
                            break;
                        }
                        Err(e) => {
                            batch.dropped += 1;
                            debug!(forwarder = %label, client = %shared.client, error = %e, "UDP reply send failed; datagram dropped");
                        }
                    }
                }
                Err(e) => match classify_udp_err(&e) {
                    UdpErr::WouldBlock => { outcome = SessionDrain::Idle; break; }
                    UdpErr::Transient => {}
                    UdpErr::Icmp(kind) => {
                        stats.mark_udp_icmp_error(now_ms, kind);
                        debug!(forwarder = %label, client = %shared.client, remote = %shared.remote, error = %e,
                            "UDP target answered with an ICMP error");
                    }
                    UdpErr::Fatal => {
                        shared.dead.store(true, Ordering::Relaxed);
                        debug!(forwarder = %label, client = %shared.client, error = %e,
                            "UDP upstream receive failed; session marked dead");
                        outcome = SessionDrain::Dead;
                        break;
                    }
                },
            }
        }
        if replied {
            shared.touch(now_ms);
            stats.mark_connect_ok_at(now_ms);
        }
        if batch.datagrams > 0 {
            shared.rx_datagrams.fetch_add(batch.datagrams, Ordering::Relaxed);
            stats.add_received(batch.bytes);
            stats.add_datagrams_received(batch.datagrams);
        }
        if batch.dropped > 0 {
            stats.add_datagrams_dropped(batch.dropped);
        }
        outcome
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_dns_refresher(
    cfg: Arc<ForwarderConfig>,
    label: Arc<str>,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    notifier: Notifier,
    tx: watch::Sender<Arc<[SocketAddr]>>,
    cancel: CancellationToken,
) {
    let mut fail_gate = LogGate::default();
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            r = resolve_targets(&cfg.remote_host, cfg.remote_port, None, &resolver, knobs.dns_timeout()) => r,
        };
        match result {
            Ok(addrs) => {
                let ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
                if let Some(old) = stats.observe_addrs(&ips) {
                    let mut new = ips.clone();
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
                tx.send_if_modified(|cur| {
                    if same_addr_set(cur, &addrs) {
                        return false;
                    }
                    *cur = Arc::from(addrs);
                    true
                });
            }
            Err(e) => {
                stats.mark_connect_failed();
                if let Some(s) = fail_gate.check(DNS_FAIL_LOG_SPACING) {
                    warn!(forwarder = %label, host = %cfg.remote_host, error = %e, suppressed = s,
                        "UDP target DNS resolution failed; keeping the last known addresses");
                }
            }
        }
        if cancelled(&cancel, DNS_REFRESH_INTERVAL).await {
            return;
        }
    }
}

/// Headroom for listeners, TCP connections, the log file, DNS and the metrics endpoint.
#[cfg(unix)]
const FD_HEADROOM: u64 = 256;

#[cfg(unix)]
#[derive(Debug, PartialEq, Eq)]
enum FdPlan {
    Enough,
    Raise { to: u64, sufficient: bool },
    Short,
}

#[cfg(unix)]
fn fd_plan(soft: u64, hard: u64, needed: u64) -> FdPlan {
    if soft >= needed {
        FdPlan::Enough
    } else if hard > soft {
        let to = hard.min(needed);
        FdPlan::Raise {
            to,
            sufficient: to >= needed,
        }
    } else {
        FdPlan::Short
    }
}

/// Make room for `udp_max_sessions` upstream sockets: raise the soft
/// RLIMIT_NOFILE when the hard limit allows, warn when it cannot suffice.
// `rlim_t` is not u64 on every unix target (32-bit, some BSDs): keep the casts.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
pub(crate) fn ensure_fd_budget(udp_max_sessions: usize) {
    let needed = udp_max_sessions as u64 + FD_HEADROOM;
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `rl` is a valid, writable `rlimit` for the duration of the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } != 0 {
        warn!(error = %io::Error::last_os_error(), "Could not read the open-file limit; not adjusting it");
        return;
    }
    let (soft, hard) = (rl.rlim_cur as u64, rl.rlim_max as u64);
    let mut current = soft;
    let sufficient = match fd_plan(soft, hard, needed) {
        FdPlan::Enough => return,
        FdPlan::Short => false,
        FdPlan::Raise { to, sufficient } => {
            let raised = libc::rlimit {
                rlim_cur: to as libc::rlim_t,
                rlim_max: rl.rlim_max,
            };
            // SAFETY: `raised` is a valid `rlimit`; the call only reads it.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
                info!("Raised the open-file soft limit from {soft} to {to} for udp_max_sessions");
                current = to;
                sufficient
            } else {
                warn!(error = %io::Error::last_os_error(), "Could not raise the open-file soft limit");
                false
            }
        }
    };
    if !sufficient {
        warn!(
            "Open-file limit {current} is below udp_max_sessions + {FD_HEADROOM} = {needed}; UDP sessions beyond it will fail to open (raise ulimit -n / LimitNOFILE, or lower udp_max_sessions)"
        );
    }
}

#[cfg(not(unix))]
pub(crate) fn ensure_fd_budget(_udp_max_sessions: usize) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Protocol, Settings};
    use crate::testutil::{resolver_for, spawn_fake_dns};
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU32, AtomicUsize};
    use std::sync::Mutex;

    /// Reserve a free localhost UDP port, never returning the same one twice
    /// (tests run in parallel; "bind 0, read, drop" alone would collide).
    fn free_udp_port() -> u16 {
        use std::sync::OnceLock;
        static CLAIMED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
        let claimed = CLAIMED.get_or_init(|| Mutex::new(HashSet::new()));
        for _ in 0..1000 {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let p = s.local_addr().unwrap().port();
            drop(s);
            if claimed.lock().unwrap().insert(p) {
                return p;
            }
        }
        panic!("could not reserve an unused ephemeral UDP port");
    }

    fn system_resolver() -> Arc<TokioResolver> {
        Arc::new(TokioResolver::builder_tokio().unwrap().build().unwrap())
    }

    struct Fwd {
        port: u16,
        stats: Arc<ForwarderStats>,
        knobs: Arc<RuntimeKnobs>,
        limit: Arc<Semaphore>,
        cancel: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }

    fn spawn_fwd_at(
        port: u16,
        remote_host: &str,
        remote_port: u16,
        settings: Settings,
        resolver: Arc<TokioResolver>,
    ) -> Fwd {
        let cfg = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port: port,
            remote_host: remote_host.into(),
            remote_port,
            protocol: Protocol::Udp,
        };
        let stats = Arc::new(ForwarderStats::with_protocol(cfg.label(), Protocol::Udp));
        let knobs = Arc::new(RuntimeKnobs::from_settings(&settings));
        let limit = Arc::new(Semaphore::new(settings.udp_max_sessions));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_udp_forwarder(
            cfg,
            cancel.clone(),
            stats.clone(),
            resolver,
            knobs.clone(),
            limit.clone(),
            Notifier::disabled(),
        ));
        Fwd {
            port,
            stats,
            knobs,
            limit,
            cancel,
            task,
        }
    }

    fn spawn_fwd(remote_host: &str, remote_port: u16, settings: Settings) -> Fwd {
        spawn_fwd_at(
            free_udp_port(),
            remote_host,
            remote_port,
            settings,
            system_resolver(),
        )
    }

    async fn start_fwd(remote_host: &str, remote_port: u16, settings: Settings) -> Fwd {
        let f = spawn_fwd(remote_host, remote_port, settings);
        wait_listening(&f).await;
        f
    }

    async fn wait_listening(f: &Fwd) {
        wait_until(
            || f.stats.snapshot().listening,
            Duration::from_secs(3),
            "listening",
        )
        .await;
    }

    async fn wait_until(mut cond: impl FnMut() -> bool, within: Duration, what: &str) {
        let deadline = tokio::time::Instant::now() + within;
        while !cond() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn load(a: &AtomicU64) -> u64 {
        a.load(Ordering::Relaxed)
    }

    /// Echo server recording every peer address it sees.
    async fn spawn_udp_echo(bind: &str) -> (u16, Arc<Mutex<HashSet<SocketAddr>>>) {
        let sock = UdpSocket::bind(bind).await.unwrap();
        let port = sock.local_addr().unwrap().port();
        let peers = Arc::new(Mutex::new(HashSet::new()));
        let seen = peers.clone();
        tokio::spawn(async move {
            let mut buf = vec![0_u8; 65536];
            while let Ok((n, peer)) = sock.recv_from(&mut buf).await {
                seen.lock().unwrap().insert(peer);
                let _ = sock.send_to(&buf[..n], peer).await;
            }
        });
        (port, peers)
    }

    /// Backend answering `tag + payload`.
    fn spawn_udp_tagged_on(addr: SocketAddr, tag: &'static [u8]) -> io::Result<()> {
        let std_sock = std::net::UdpSocket::bind(addr)?;
        std_sock.set_nonblocking(true)?;
        let sock = UdpSocket::from_std(std_sock)?;
        tokio::spawn(async move {
            let mut buf = vec![0_u8; 65536];
            while let Ok((n, peer)) = sock.recv_from(&mut buf).await {
                let mut out = tag.to_vec();
                out.extend_from_slice(&buf[..n]);
                let _ = sock.send_to(&out, peer).await;
            }
        });
        Ok(())
    }

    async fn roundtrip(
        c: &UdpSocket,
        port: u16,
        payload: &[u8],
        within: Duration,
    ) -> Option<Vec<u8>> {
        c.send_to(payload, ("127.0.0.1", port)).await.unwrap();
        let mut buf = vec![0_u8; 65536];
        match tokio::time::timeout(within, c.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => Some(buf[..n].to_vec()),
            _ => None,
        }
    }

    async fn client() -> UdpSocket {
        UdpSocket::bind("127.0.0.1:0").await.unwrap()
    }

    #[tokio::test]
    async fn forwards_a_datagram_and_its_reply() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let f = start_fwd("127.0.0.1", echo, Settings::default()).await;
        let c = client().await;
        let reply = roundtrip(&c, f.port, b"hello", Duration::from_secs(2)).await;
        assert_eq!(reply.as_deref(), Some(&b"hello"[..]));
        wait_until(
            || load(&f.stats.datagrams_received) == 1,
            Duration::from_secs(1),
            "reply counters",
        )
        .await;
        assert_eq!(load(&f.stats.datagrams_sent), 1);
        assert_eq!(load(&f.stats.bytes_sent), 5);
        assert_eq!(load(&f.stats.bytes_received), 5);
        assert_eq!(load(&f.stats.active_connections), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_datagram_of_a_new_session_is_never_dropped() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let f = start_fwd("127.0.0.1", echo, Settings::default()).await;
        for i in 0..100 {
            let c = client().await;
            let msg = format!("first-{i}");
            let reply = roundtrip(&c, f.port, msg.as_bytes(), Duration::from_secs(1)).await;
            assert_eq!(reply.as_deref(), Some(msg.as_bytes()), "iteration {i}");
        }
        wait_until(
            || load(&f.stats.datagrams_sent) == 100,
            Duration::from_secs(1),
            "sent counter",
        )
        .await;
        assert_eq!(load(&f.stats.datagrams_dropped), 0);
    }

    #[tokio::test]
    async fn datagram_boundaries_are_preserved_for_all_sizes() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let f = start_fwd("127.0.0.1", echo, Settings::default()).await;
        let c = client().await;
        for size in [0_usize, 1, 100, 1472, 1473, 9000, 60000, 65507] {
            let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let reply = roundtrip(&c, f.port, &payload, Duration::from_secs(2)).await;
            assert_eq!(reply.as_deref(), Some(&payload[..]), "size {size}");
        }
        wait_until(
            || load(&f.stats.datagrams_received) == 8,
            Duration::from_secs(1),
            "received counter",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_client_gets_its_own_session_and_only_its_replies() {
        let (echo, peers) = spawn_udp_echo("127.0.0.1:0").await;
        let f = start_fwd("127.0.0.1", echo, Settings::default()).await;
        let port = f.port;
        let mut joins = Vec::new();
        for i in 0..8 {
            joins.push(tokio::spawn(async move {
                let c = client().await;
                for k in 0..10 {
                    c.send_to(format!("c{i}-{k}").as_bytes(), ("127.0.0.1", port))
                        .await
                        .unwrap();
                }
                let mut got = HashSet::new();
                let mut buf = vec![0_u8; 2048];
                for _ in 0..10 {
                    let (n, _) =
                        tokio::time::timeout(Duration::from_secs(2), c.recv_from(&mut buf))
                            .await
                            .expect("reply within 2s")
                            .unwrap();
                    got.insert(String::from_utf8(buf[..n].to_vec()).unwrap());
                }
                let want: HashSet<String> = (0..10).map(|k| format!("c{i}-{k}")).collect();
                assert_eq!(got, want, "client {i} must see exactly its own payloads");
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        assert_eq!(peers.lock().unwrap().len(), 8);
        assert_eq!(load(&f.stats.active_connections), 8);
    }

    #[tokio::test]
    async fn idle_sessions_expire_and_release_their_slot() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let settings = Settings {
            udp_session_timeout_secs: 1,
            udp_max_sessions: 1,
            ..Settings::default()
        };
        let f = start_fwd("127.0.0.1", echo, settings).await;
        let a = client().await;
        assert!(roundtrip(&a, f.port, b"a", Duration::from_secs(2))
            .await
            .is_some());
        wait_until(
            || load(&f.stats.active_connections) == 0 && f.limit.available_permits() == 1,
            Duration::from_secs(4),
            "session expiry",
        )
        .await;
        let b = client().await;
        assert_eq!(
            roundtrip(&b, f.port, b"b", Duration::from_secs(2))
                .await
                .as_deref(),
            Some(&b"b"[..])
        );
    }

    #[tokio::test]
    async fn session_cap_drops_and_counts_new_flows() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let settings = Settings {
            udp_max_sessions: 2,
            ..Settings::default()
        };
        let f = start_fwd("127.0.0.1", echo, settings).await;
        let (a, b, c) = (client().await, client().await, client().await);
        assert!(roundtrip(&a, f.port, b"a", Duration::from_secs(2))
            .await
            .is_some());
        assert!(roundtrip(&b, f.port, b"b", Duration::from_secs(2))
            .await
            .is_some());
        assert!(roundtrip(&c, f.port, b"c", Duration::from_millis(500))
            .await
            .is_none());
        assert_eq!(load(&f.stats.rejected_connections), 1);
        assert!(load(&f.stats.datagrams_dropped) >= 1);
        assert_eq!(
            roundtrip(&a, f.port, b"a2", Duration::from_secs(2))
                .await
                .as_deref(),
            Some(&b"a2"[..])
        );
    }

    #[tokio::test]
    async fn cancel_releases_the_port_before_returning() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let f = start_fwd("127.0.0.1", echo, Settings::default()).await;
        let mut clients = Vec::new();
        for _ in 0..5 {
            let c = client().await;
            assert!(roundtrip(&c, f.port, b"x", Duration::from_secs(2))
                .await
                .is_some());
            clients.push(c);
        }
        assert_eq!(load(&f.stats.active_connections), 5);
        f.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), f.task)
            .await
            .expect("the forwarder must stop within 3s")
            .unwrap();
        std::net::UdpSocket::bind(("127.0.0.1", f.port))
            .expect("the port must be free the moment the task returns");
        assert!(!f.stats.snapshot().listening);
        assert_eq!(load(&f.stats.active_connections), 0);
        assert_eq!(f.limit.available_permits(), 16384);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn sessions_follow_a_dns_change_and_old_ones_close() {
        let mut port = 0;
        for _ in 0..50 {
            let p = free_udp_port();
            if spawn_udp_tagged_on(SocketAddr::from(([127, 0, 0, 2], p)), b"two").is_err() {
                continue;
            }
            if spawn_udp_tagged_on(SocketAddr::from(([127, 0, 0, 1], p)), b"one").is_err() {
                continue;
            }
            port = p;
            break;
        }
        assert_ne!(port, 0, "could not place the two backends on one port");

        let answer = Arc::new(AtomicU32::new(u32::from(std::net::Ipv4Addr::new(
            127, 0, 0, 1,
        ))));
        let dns = spawn_fake_dns(answer.clone(), Arc::new(AtomicUsize::new(0)), 1).await;
        let settings = Settings {
            dns_timeout_secs: 2,
            ..Settings::default()
        };
        let f = spawn_fwd_at(
            free_udp_port(),
            "backend.test",
            port,
            settings,
            Arc::new(resolver_for(dns)),
        );
        wait_listening(&f).await;

        let c = client().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let r = roundtrip(&c, f.port, b"x", Duration::from_millis(200)).await;
            if r.is_some_and(|r| r.starts_with(b"one")) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "never reached backend one"
            );
        }

        answer.store(
            u32::from(std::net::Ipv4Addr::new(127, 0, 0, 2)),
            Ordering::Relaxed,
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let r = roundtrip(&c, f.port, b"x", Duration::from_millis(200)).await;
            if r.is_some_and(|r| r.starts_with(b"two")) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "never reached backend two"
            );
        }
        assert_eq!(
            f.stats.last_addrs(),
            Some(vec![IpAddr::from([127, 0, 0, 2])])
        );
    }

    #[test]
    fn classify_udp_err_covers_every_kind() {
        use io::ErrorKind as K;
        let c = |k: K| classify_udp_err(&io::Error::from(k));
        assert_eq!(c(K::WouldBlock), UdpErr::WouldBlock);
        assert_eq!(c(K::Interrupted), UdpErr::Transient);
        assert_eq!(
            c(K::ConnectionRefused),
            UdpErr::Icmp(IcmpKind::PortUnreachable)
        );
        assert_eq!(
            c(K::ConnectionReset),
            UdpErr::Icmp(IcmpKind::PortUnreachable)
        );
        assert_eq!(
            c(K::HostUnreachable),
            UdpErr::Icmp(IcmpKind::HostUnreachable)
        );
        assert_eq!(
            c(K::NetworkUnreachable),
            UdpErr::Icmp(IcmpKind::NetworkUnreachable)
        );
        assert_eq!(c(K::PermissionDenied), UdpErr::Fatal);
        assert_eq!(c(K::Other), UdpErr::Fatal);
    }

    #[tokio::test]
    async fn bind_conflict_retries_then_listens() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let occupant = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = occupant.local_addr().unwrap().port();
        let f = spawn_fwd_at(
            port,
            "127.0.0.1",
            echo,
            Settings::default(),
            system_resolver(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!f.stats.snapshot().listening);
        drop(occupant);
        wait_until(
            || f.stats.snapshot().listening,
            Duration::from_secs(8),
            "listening",
        )
        .await;
        let c = client().await;
        assert!(roundtrip(&c, f.port, b"x", Duration::from_secs(2))
            .await
            .is_some());
    }

    #[tokio::test]
    async fn session_timeout_is_read_live() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let settings = Settings {
            udp_session_timeout_secs: 60,
            ..Settings::default()
        };
        let f = start_fwd("127.0.0.1", echo, settings).await;
        let c = client().await;
        assert!(roundtrip(&c, f.port, b"x", Duration::from_secs(2))
            .await
            .is_some());
        assert_eq!(load(&f.stats.active_connections), 1);
        f.knobs.update(&Settings {
            udp_session_timeout_secs: 1,
            ..Settings::default()
        });
        wait_until(
            || load(&f.stats.active_connections) == 0,
            Duration::from_secs(4),
            "live timeout",
        )
        .await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn icmp_errors_are_recorded_and_kick_the_monitor() {
        let dead = free_udp_port();
        let f = start_fwd("127.0.0.1", dead, Settings::default()).await;
        let c = client().await;
        for _ in 0..3 {
            c.send_to(b"x", ("127.0.0.1", f.port)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::timeout(Duration::from_secs(3), f.stats.probe_kick().notified())
            .await
            .expect("an ICMP error must wake the monitor");
        assert_eq!(load(&f.stats.active_connections), 1);
    }

    #[test]
    fn log_gate_reports_suppressed_counts() {
        let mut g = LogGate::default();
        assert_eq!(g.check(Duration::from_secs(60)), Some(0));
        for _ in 0..3 {
            assert_eq!(g.check(Duration::from_secs(60)), None);
        }
        assert_eq!(g.check(Duration::ZERO), Some(3));
        assert_eq!(g.check(Duration::ZERO), Some(0));
    }

    #[test]
    fn same_addr_set_ignores_order() {
        let a: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let b: SocketAddr = "10.0.0.2:1".parse().unwrap();
        assert!(same_addr_set(&[a, b], &[b, a]));
        assert!(!same_addr_set(&[a], &[a, b]));
        assert!(same_addr_set(&[a, a], &[a]));
    }

    #[cfg(unix)]
    #[test]
    fn fd_budget_math() {
        assert_eq!(fd_plan(20000, 20000, 16640), FdPlan::Enough);
        assert_eq!(
            fd_plan(1024, 1048576, 16640),
            FdPlan::Raise {
                to: 16640,
                sufficient: true
            }
        );
        assert_eq!(
            fd_plan(1024, 4096, 16640),
            FdPlan::Raise {
                to: 4096,
                sufficient: false
            }
        );
        assert_eq!(fd_plan(1024, 1024, 16640), FdPlan::Short);
        assert_eq!(
            fd_plan(1024, u64::MAX, 16640),
            FdPlan::Raise {
                to: 16640,
                sufficient: true
            }
        );
    }
}
