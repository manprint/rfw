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
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
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

/// Listener receive buffer: room for a GRO-coalesced burst on Linux.
#[cfg(target_os = "linux")]
const LISTENER_BUF_BYTES: usize = offload::GRO_BUF_BYTES;
#[cfg(not(target_os = "linux"))]
const LISTENER_BUF_BYTES: usize = UDP_BUF_BYTES;

#[cfg(target_os = "linux")]
thread_local! {
    /// Reply buffer for sessions whose upstream socket has UDP_GRO (lazily
    /// allocated on first use by each worker thread).
    static GRO_REPLY_BUF: RefCell<Vec<u8>> = RefCell::new(vec![0_u8; offload::GRO_BUF_BYTES]);
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
    #[cfg(target_os = "linux")]
    gso_fallback: LogGate,
    #[cfg(target_os = "linux")]
    gro_trunc: LogGate,
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
    /// UDP_GRO is set on `upstream`: every receive must parse cmsgs (D32).
    #[cfg(target_os = "linux")]
    gro: bool,
    /// Shared with the listener: replies may still use GSO (D35).
    #[cfg(target_os = "linux")]
    listener_gso: Arc<AtomicBool>,
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
    /// Client -> target sends may still use GSO on this session (D35).
    #[cfg(target_os = "linux")]
    gso_ok: bool,
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
        #[cfg(target_os = "linux")]
        off: ListenerOffload {
            gro_set: false,
            enabled: false,
            gro_aware: false,
            unsupported: false,
            gso_ok: Arc::new(AtomicBool::new(true)),
        },
    };
    let mut buf = vec![0_u8; LISTENER_BUF_BYTES];
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

#[cfg(target_os = "linux")]
struct ListenerOffload {
    /// UDP_GRO is set on the listener socket. Only ever goes true (D32a).
    gro_set: bool,
    /// The knob state last applied (new sessions get UDP_GRO only while true).
    enabled: bool,
    /// Receives parse GRO cmsgs. Sticky (D32).
    gro_aware: bool,
    /// The kernel refused UDP_GRO (D37).
    unsupported: bool,
    /// Replies may use GSO; cleared for good on the first rejection.
    gso_ok: Arc<AtomicBool>,
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
    #[cfg(target_os = "linux")]
    off: ListenerOffload,
}

impl Engine {
    async fn run(&mut self, buf: &mut [u8]) {
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        sweep.tick().await; // the first tick is immediate
        #[cfg(target_os = "linux")]
        self.apply_gro_knob();
        loop {
            // `select!` drops every branch future before running the chosen
            // handler, so handlers may borrow `self` mutably.
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return,
                _ = sweep.tick() => {
                    self.sweep();
                    #[cfg(target_os = "linux")]
                    self.apply_gro_knob();
                }
                changed = self.targets.changed(), if self.dns_watch => match changed {
                    Ok(()) => self.on_targets_changed(),
                    Err(_) => self.dns_watch = false, // refresher gone (shutdown)
                },
                ready = self.socket.readable() => match ready {
                    Ok(()) => {
                        #[cfg(target_os = "linux")]
                        let drained = if self.off.gro_aware {
                            self.drain_listener_gro(buf)
                        } else {
                            self.drain_listener(buf)
                        };
                        #[cfg(not(target_os = "linux"))]
                        let drained = self.drain_listener(buf);
                        match drained {
                            ListenerDrain::Idle => {}
                            ListenerDrain::Full => tokio::task::yield_now().await,
                            ListenerDrain::Failed => {
                                if cancelled(&self.cancel, RECV_ERROR_BACKOFF).await { return; }
                            }
                        }
                    }
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
        if let Some(entry) = self.open_session(client, now_ms, batch, 1) {
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
        datagrams: u64,
    ) -> Option<SessionEntry> {
        // Separate statement: the watch read guard drops here, not after the else block.
        let first = self.targets.borrow().first().copied();
        let Some(remote) = first else {
            batch.dropped += datagrams;
            self.stats.mark_connect_failed();
            if let Some(s) = self.gates.no_target.check(DROP_LOG_SPACING) {
                warn!(forwarder = %self.label, suppressed = s, "UDP target not resolved yet; datagram dropped");
            }
            return None;
        };
        let permit = match self.udp_limit.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                batch.dropped += datagrams;
                self.stats.add_rejected(datagrams);
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
                batch.dropped += datagrams;
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
        #[cfg(target_os = "linux")]
        let gro = self.off.enabled
            && match offload::set_gro(upstream.as_raw_fd(), true) {
                Ok(()) => true,
                Err(e) => {
                    if e.raw_os_error() == Some(libc::ENOPROTOOPT) {
                        self.mark_gro_unsupported();
                    }
                    false
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
            #[cfg(target_os = "linux")]
            gro,
            #[cfg(target_os = "linux")]
            listener_gso: self.off.gso_ok.clone(),
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
            #[cfg(target_os = "linux")]
            gso_ok: true,
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

#[cfg(target_os = "linux")]
impl Engine {
    // D32a: UDP_GRO is never cleared from a socket. `enabled` is the knob state
    // last applied; `gro_set` is the socket state and only goes true.
    fn apply_gro_knob(&mut self) {
        let want = self.knobs.udp_gro_gso() && !self.off.unsupported;
        if want == self.off.enabled {
            return;
        }
        if !want {
            self.off.enabled = false;
            info!(forwarder = %self.label, "UDP GRO/GSO disabled on the listener for new sessions (UDP_GRO stays set on the listener socket until restart)");
            return;
        }
        if !self.off.gro_set {
            self.off.gro_aware = true; // before the sockopt: never read a burst with a plain recv
            match offload::set_gro(self.socket.as_raw_fd(), true) {
                Ok(()) => self.off.gro_set = true,
                Err(e) if e.raw_os_error() == Some(libc::ENOPROTOOPT) => {
                    return self.mark_gro_unsupported();
                }
                Err(e) => {
                    // latch: one warn, no retry every sweep
                    self.off.unsupported = true;
                    warn!(forwarder = %self.label, error = %e, "Setting UDP_GRO on the listener failed; udp_gro_gso has no effect on this forwarder until restart");
                    return;
                }
            }
        }
        self.off.enabled = true;
        info!(forwarder = %self.label, "UDP GRO/GSO enabled on the listener");
    }

    fn mark_gro_unsupported(&mut self) {
        if !self.off.unsupported {
            self.off.unsupported = true;
            warn!(forwarder = %self.label, "UDP GRO is not supported by this kernel (Linux 5.0+ required); udp_gro_gso has no effect");
        }
    }

    /// `drain_listener` with GRO-aware receives (D32).
    fn drain_listener_gro(&mut self, buf: &mut [u8]) -> ListenerDrain {
        let now_ms = self.stats.now_ms();
        let fd = self.socket.as_raw_fd();
        let mut batch = Batch::default();
        let mut outcome = ListenerDrain::Full;
        for _ in 0..LISTENER_BATCH {
            match self
                .socket
                .try_io(Interest::READABLE, || offload::recv(fd, buf, true))
            {
                Ok(r) => {
                    let Some(client) = r.from else { continue };
                    if r.truncated {
                        batch.dropped += offload::segments(r.len, r.segment);
                        if let Some(s) = self.gates.gro_trunc.check(DROP_LOG_SPACING) {
                            warn!(forwarder = %self.label, client = %client, suppressed = s,
                                "UDP GRO burst larger than the receive buffer; datagrams dropped");
                        }
                        continue;
                    }
                    self.on_client_buffer(client, &buf[..r.len], r.segment, now_ms, &mut batch);
                }
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

    /// `on_client_datagram` for a buffer that may be a GRO burst.
    fn on_client_buffer(
        &mut self,
        client: SocketAddr,
        data: &[u8],
        segment: usize,
        now_ms: u64,
        batch: &mut Batch,
    ) {
        if !offload::is_coalesced(data.len(), segment) {
            return self.on_client_datagram(client, data, now_ms, batch);
        }
        if let Some(entry) = self.sessions.get_mut(&client) {
            if !entry.shared.dead.load(Ordering::Relaxed) {
                forward_upstream_coalesced(
                    entry,
                    data,
                    segment,
                    now_ms,
                    batch,
                    &self.stats,
                    &mut self.gates,
                    &self.label,
                );
                return;
            }
        }
        if let Some(dead) = self.sessions.remove(&client) {
            close_entry(&self.label, &dead, "upstream error");
        }
        let n = offload::segments(data.len(), segment);
        if let Some(entry) = self.open_session(client, now_ms, batch, n) {
            let entry = self.sessions.entry(client).or_insert(entry);
            forward_upstream_coalesced(
                entry,
                data,
                segment,
                now_ms,
                batch,
                &self.stats,
                &mut self.gates,
                &self.label,
            );
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
            note_upstream_send_error(&e, shared, now_ms, stats, gate, label);
        }
    }
}

/// Log and account for a failed upstream send (the caller counts the drop).
fn note_upstream_send_error(
    e: &io::Error,
    shared: &SessionShared,
    now_ms: u64,
    stats: &ForwarderStats,
    gate: &mut LogGate,
    label: &str,
) {
    match classify_udp_err(e) {
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

/// Client -> target for a GRO burst. Synchronous (I-4); raw sends (I-14).
#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn forward_upstream_coalesced(
    entry: &mut SessionEntry,
    data: &[u8],
    segment: usize,
    now_ms: u64,
    batch: &mut Batch,
    stats: &ForwarderStats,
    gates: &mut Gates,
    label: &str,
) {
    let total = offload::segments(data.len(), segment);
    let mut from = 0;
    if entry.gso_ok {
        let fd = entry.shared.upstream.as_raw_fd();
        match offload::send_chunks(data, segment, |chunk, seg| {
            offload::send(fd, chunk, seg, None)
        }) {
            offload::GsoSend::Sent => {
                entry.tx_datagrams += total;
                batch.datagrams += total;
                batch.bytes += data.len() as u64;
                entry.shared.touch(now_ms);
                return;
            }
            offload::GsoSend::Stopped { from: f, error } => {
                let sent = (f / segment) as u64;
                entry.tx_datagrams += sent;
                batch.datagrams += sent;
                batch.bytes += f as u64;
                if sent > 0 {
                    entry.shared.touch(now_ms);
                }
                if !offload::is_fallback_error(&error) {
                    batch.dropped += total - sent;
                    note_upstream_send_error(
                        &error,
                        &entry.shared,
                        now_ms,
                        stats,
                        &mut gates.send_full,
                        label,
                    );
                    return;
                }
                entry.gso_ok = false;
                if let Some(s) = gates.gso_fallback.check(DROP_LOG_SPACING) {
                    warn!(forwarder = %label, client = %entry.shared.client, error = %error, suppressed = s,
                        "UDP GSO rejected by the kernel for a session; it now sends one datagram at a time");
                }
                from = f;
            }
        }
    }
    for piece in data[from..].chunks(segment) {
        forward_upstream(
            entry,
            piece,
            now_ms,
            batch,
            stats,
            &mut gates.send_full,
            label,
        );
    }
}

/// A reply the listener could not take yet. `segment > 0` = coalesced (D31).
struct Pending {
    data: Vec<u8>,
    segment: usize,
}

enum SessionDrain {
    Idle,
    Full,
    Blocked(Pending),
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
        #[cfg(target_os = "linux")]
        let drained = if shared.gro {
            drain_upstream_gro(&shared, &listener, &stats, &label)
        } else {
            drain_upstream(&shared, &listener, &stats, &label)
        };
        #[cfg(not(target_os = "linux"))]
        let drained = drain_upstream(&shared, &listener, &stats, &label);
        match drained {
            SessionDrain::Idle => {}
            SessionDrain::Full => tokio::task::yield_now().await,
            SessionDrain::Dead => return,
            SessionDrain::Blocked(p) => {
                let pieces: Vec<&[u8]> = if p.segment > 0 && p.segment < p.data.len() {
                    p.data.chunks(p.segment).collect()
                } else {
                    vec![&p.data[..]]
                };
                for piece in pieces {
                    let sent = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        r = listener.send_to(piece, shared.client) => r,
                    };
                    match sent {
                        Ok(_) => {
                            shared.rx_datagrams.fetch_add(1, Ordering::Relaxed);
                            stats.add_received(piece.len() as u64);
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
                            outcome = SessionDrain::Blocked(Pending {
                                data: buf[..n].to_vec(),
                                segment: 0,
                            });
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

/// `drain_upstream` for a session whose upstream socket has UDP_GRO (D32).
#[cfg(target_os = "linux")]
fn drain_upstream_gro(
    shared: &SessionShared,
    listener: &UdpSocket,
    stats: &ForwarderStats,
    label: &str,
) -> SessionDrain {
    GRO_REPLY_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        let now_ms = stats.now_ms();
        let fd = shared.upstream.as_raw_fd();
        let to = socket2::SockAddr::from(shared.client);
        let mut batch = Batch::default();
        let mut replied = false;
        let mut outcome = SessionDrain::Full;
        for _ in 0..SESSION_BATCH {
            // D7a: single-flag interests only; READABLE first, ERROR on WouldBlock.
            let res = match shared
                .upstream
                .try_io(Interest::READABLE, || offload::recv(fd, &mut buf[..], false))
            {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => shared
                    .upstream
                    .try_io(Interest::ERROR, || offload::recv(fd, &mut buf[..], false)),
                res => res,
            };
            match res {
                Ok(r) => {
                    replied = true;
                    if r.truncated {
                        batch.dropped += offload::segments(r.len, r.segment);
                        debug!(forwarder = %label, client = %shared.client, "UDP GRO reply larger than the buffer; datagrams dropped");
                        continue;
                    }
                    let data = &buf[..r.len];
                    if !offload::is_coalesced(r.len, r.segment) {
                        match listener.try_send_to(data, shared.client) {
                            Ok(_) => {
                                batch.datagrams += 1;
                                batch.bytes += r.len as u64;
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                outcome = SessionDrain::Blocked(Pending {
                                    data: data.to_vec(),
                                    segment: 0,
                                });
                                break;
                            }
                            Err(e) => {
                                batch.dropped += 1;
                                debug!(forwarder = %label, client = %shared.client, error = %e, "UDP reply send failed; datagram dropped");
                            }
                        }
                        continue;
                    }
                    if let Some(p) = reply_coalesced(
                        listener,
                        &to,
                        shared.client,
                        data,
                        r.segment,
                        &shared.listener_gso,
                        &mut batch,
                        label,
                    ) {
                        outcome = SessionDrain::Blocked(p);
                        break;
                    }
                }
                Err(e) => match classify_udp_err(&e) {
                    UdpErr::WouldBlock => {
                        outcome = SessionDrain::Idle;
                        break;
                    }
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

/// Target -> client for a GRO burst: GSO through the listener, or one
/// datagram at a time once the listener's GSO was rejected. Returns the
/// unsent tail when the listener's send buffer is full.
#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn reply_coalesced(
    listener: &UdpSocket,
    to: &socket2::SockAddr,
    client: SocketAddr,
    data: &[u8],
    segment: usize,
    gso_ok: &AtomicBool,
    batch: &mut Batch,
    label: &str,
) -> Option<Pending> {
    let mut from = 0;
    if gso_ok.load(Ordering::Relaxed) {
        let fd = listener.as_raw_fd();
        let r = offload::send_chunks(data, segment, |chunk, seg| {
            listener.try_io(Interest::WRITABLE, || {
                offload::send(fd, chunk, seg, Some(to))
            })
        });
        match r {
            offload::GsoSend::Sent => {
                batch.datagrams += offload::segments(data.len(), segment);
                batch.bytes += data.len() as u64;
                return None;
            }
            offload::GsoSend::Stopped { from: f, error } => {
                batch.datagrams += (f / segment) as u64;
                batch.bytes += f as u64;
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Some(Pending {
                        data: data[f..].to_vec(),
                        segment,
                    });
                }
                if !offload::is_fallback_error(&error) {
                    batch.dropped += offload::segments(data.len() - f, segment);
                    debug!(forwarder = %label, client = %client, error = %error, "UDP reply send failed; datagrams dropped");
                    return None;
                }
                if gso_ok.swap(false, Ordering::Relaxed) {
                    warn!(forwarder = %label, error = %error,
                        "UDP GSO rejected by the kernel on the listener; replies now go one datagram at a time");
                }
                from = f;
            }
        }
    }
    let rest = &data[from..];
    let mut off = 0;
    for piece in rest.chunks(segment) {
        match listener.try_send_to(piece, client) {
            Ok(_) => {
                batch.datagrams += 1;
                batch.bytes += piece.len() as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Some(Pending {
                    data: rest[off..].to_vec(),
                    segment,
                });
            }
            Err(e) => {
                batch.dropped += 1;
                debug!(forwarder = %label, client = %client, error = %e, "UDP reply send failed; datagram dropped");
            }
        }
        off += piece.len();
    }
    None
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

/// Linux UDP segmentation offload: GRO on receive, GSO on send.
#[cfg(target_os = "linux")]
mod offload {
    use std::io;
    use std::mem;
    use std::net::SocketAddr;
    use std::os::fd::RawFd;
    use std::ptr;

    /// Most segments per GSO send (64 on older kernels, 128 on newer ones).
    pub(super) const MAX_GSO_SEGMENTS: usize = 64;
    /// Largest UDP payload one send may carry (the IPv4 bound; IPv6 allows 65527).
    pub(super) const GSO_MAX_PAYLOAD: usize = 65_507;
    /// Receive buffer when GRO may coalesce (64 x 1472 = 94 208, plus headroom).
    pub(super) const GRO_BUF_BYTES: usize = 128 * 1024;

    /// Control-message buffer, aligned for `cmsghdr`.
    #[repr(C, align(8))]
    struct CmsgBuf([u8; 64]);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Recv {
        pub len: usize,
        /// gso_size from the UDP_GRO cmsg; 0 when absent.
        pub segment: usize,
        /// Source address (only when asked for).
        pub from: Option<SocketAddr>,
        /// The kernel cut the buffer short (MSG_TRUNC): its tail is lost.
        pub truncated: bool,
    }

    /// A buffer of `len` bytes is a GRO burst only when `0 < segment < len`.
    pub(super) fn is_coalesced(len: usize, segment: usize) -> bool {
        segment > 0 && segment < len
    }

    /// Datagrams carried by a buffer (a non-coalesced buffer, even empty, is one).
    pub(super) fn segments(len: usize, segment: usize) -> u64 {
        if is_coalesced(len, segment) {
            len.div_ceil(segment) as u64
        } else {
            1
        }
    }

    /// Bytes per GSO send: whole segments, at most MAX_GSO_SEGMENTS and GSO_MAX_PAYLOAD.
    pub(super) fn chunk_bytes(segment: usize) -> usize {
        segment * (GSO_MAX_PAYLOAD / segment).clamp(1, MAX_GSO_SEGMENTS)
    }

    /// EINVAL / EIO / EMSGSIZE from a segmented send: GSO is unusable on this
    /// socket or route; send one datagram at a time from now on.
    pub(super) fn is_fallback_error(e: &io::Error) -> bool {
        matches!(
            e.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::EIO) | Some(libc::EMSGSIZE)
        )
    }

    pub(super) fn set_gro(fd: RawFd, on: bool) -> io::Result<()> {
        let v: libc::c_int = on.into();
        // SAFETY: `v` outlives the call and the length matches its type.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_UDP,
                libc::UDP_GRO,
                (&v as *const libc::c_int).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// One `recvmsg`, parsing the UDP_GRO cmsg. Non-blocking sockets only;
    /// call it inside `try_io` so tokio clears readiness on WouldBlock.
    pub(super) fn recv(fd: RawFd, buf: &mut [u8], want_from: bool) -> io::Result<Recv> {
        let mut cmsg = CmsgBuf([0; 64]);
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        // SAFETY: `msghdr` is plain old data; all-zero is a valid empty header.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg.0.as_mut_ptr().cast();
        msg.msg_controllen = cmsg.0.len() as _;
        let (len, from) = if want_from {
            // SAFETY: `try_init` passes zeroed storage and its size; the kernel
            // writes at most `msg_namelen` bytes there and reports the length.
            let (n, addr) = unsafe {
                socket2::SockAddr::try_init(|storage, len| {
                    msg.msg_name = storage.cast();
                    msg.msg_namelen = *len;
                    let n = libc::recvmsg(fd, &mut msg, 0);
                    if n < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    *len = msg.msg_namelen;
                    Ok(n as usize)
                })
            }?;
            (n, addr.as_socket())
        } else {
            // SAFETY: every pointer in `msg` refers to a live local buffer.
            let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            (n as usize, None)
        };
        let mut segment = 0;
        // SAFETY: the CMSG_* walk stays within `msg_control`/`msg_controllen`
        // as updated by the kernel; the payload is read unaligned.
        unsafe {
            let mut c = libc::CMSG_FIRSTHDR(&msg);
            while !c.is_null() {
                if (*c).cmsg_level == libc::SOL_UDP && (*c).cmsg_type == libc::UDP_GRO {
                    let v = ptr::read_unaligned(libc::CMSG_DATA(c).cast::<libc::c_int>());
                    segment = usize::try_from(v).unwrap_or(0);
                }
                c = libc::CMSG_NXTHDR(&msg, c);
            }
        }
        Ok(Recv {
            len,
            segment,
            from,
            truncated: msg.msg_flags & libc::MSG_TRUNC != 0,
        })
    }

    /// One `sendmsg`. `segment > 0` adds a UDP_SEGMENT cmsg (GSO); `to` is
    /// `None` on a connected socket.
    pub(super) fn send(
        fd: RawFd,
        buf: &[u8],
        segment: u16,
        to: Option<&socket2::SockAddr>,
    ) -> io::Result<usize> {
        let mut cmsg = CmsgBuf([0; 64]);
        let mut iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        // SAFETY: see `recv`.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        if let Some(addr) = to {
            msg.msg_name = addr.as_ptr() as *mut libc::c_void;
            msg.msg_namelen = addr.len();
        }
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        if segment > 0 {
            // SAFETY: 64 aligned bytes hold CMSG_SPACE(2); the header pointer
            // comes from CMSG_FIRSTHDR on that buffer.
            unsafe {
                msg.msg_control = cmsg.0.as_mut_ptr().cast();
                msg.msg_controllen = libc::CMSG_SPACE(mem::size_of::<u16>() as u32) as _;
                let c = libc::CMSG_FIRSTHDR(&msg);
                (*c).cmsg_level = libc::SOL_UDP;
                (*c).cmsg_type = libc::UDP_SEGMENT;
                (*c).cmsg_len = libc::CMSG_LEN(mem::size_of::<u16>() as u32) as _;
                ptr::write_unaligned(libc::CMSG_DATA(c).cast::<u16>(), segment);
            }
        }
        // SAFETY: every pointer in `msg` refers to a live local buffer.
        let n = unsafe { libc::sendmsg(fd, &msg, 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    pub(super) enum GsoSend {
        Sent,
        /// `from` = offset of the first unsent byte: always a segment boundary.
        Stopped {
            from: usize,
            error: io::Error,
        },
    }

    /// Send a coalesced buffer in as few GSO sends as D34 allows, stopping at
    /// the first error. `send_one(chunk, seg)` performs one send (seg 0 = plain).
    pub(super) fn send_chunks(
        buf: &[u8],
        segment: usize,
        mut send_one: impl FnMut(&[u8], u16) -> io::Result<usize>,
    ) -> GsoSend {
        let Ok(seg16) = u16::try_from(segment) else {
            return GsoSend::Stopped {
                from: 0,
                error: io::Error::from_raw_os_error(libc::EINVAL),
            };
        };
        let step = chunk_bytes(segment);
        let mut off = 0;
        while off < buf.len() {
            let end = (off + step).min(buf.len());
            let chunk = &buf[off..end];
            let seg = if chunk.len() > segment { seg16 } else { 0 };
            if let Err(error) = send_one(chunk, seg) {
                return GsoSend::Stopped { from: off, error };
            }
            off = end;
        }
        GsoSend::Sent
    }
}

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

    // ---- Linux GRO/GSO offload (3.1) ---------------------------------------

    /// Non-blocking socket on 127.0.0.1 with UDP_GRO on; `None` on a kernel without it.
    #[cfg(target_os = "linux")]
    fn gro_socket() -> Option<std::net::UdpSocket> {
        use std::os::fd::AsRawFd;
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.set_nonblocking(true).unwrap();
        match offload::set_gro(s.as_raw_fd(), true) {
            Ok(()) => Some(s),
            Err(e) if e.raw_os_error() == Some(libc::ENOPROTOOPT) => {
                eprintln!("skip: no UDP_GRO");
                None
            }
            Err(e) => panic!("set_gro failed: {e}"),
        }
    }

    #[cfg(target_os = "linux")]
    fn gso_sender() -> std::net::UdpSocket {
        std::net::UdpSocket::bind("127.0.0.1:0").unwrap()
    }

    /// Plain (no GRO) receiver with a read timeout.
    #[cfg(target_os = "linux")]
    fn plain_receiver() -> std::net::UdpSocket {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        s
    }

    #[cfg(target_os = "linux")]
    fn set_no_check(fd: std::os::fd::RawFd) {
        let one: libc::c_int = 1;
        // SAFETY: `one` outlives the call and the length matches its type.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_NO_CHECK,
                (&one as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "SO_NO_CHECK: {}", io::Error::last_os_error());
    }

    /// `offload::recv` in a loop that sleeps 5 ms on WouldBlock, up to `within`.
    #[cfg(target_os = "linux")]
    fn poll_recv(
        fd: std::os::fd::RawFd,
        buf: &mut [u8],
        within: Duration,
    ) -> Option<offload::Recv> {
        let deadline = Instant::now() + within;
        loop {
            match offload::recv(fd, buf, true) {
                Ok(r) => return Some(r),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("recv failed: {e}"),
            }
        }
    }

    /// `n` segments of `seg` bytes, segment `i` filled with the byte `i`.
    #[cfg(target_os = "linux")]
    fn distinct_segments(n: usize, seg: usize) -> Vec<u8> {
        (0..n).flat_map(|i| vec![i as u8; seg]).collect()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_gso_send_arrives_coalesced_at_a_gro_socket() {
        use std::os::fd::AsRawFd;
        let Some(rx) = gro_socket() else { return };
        let tx = gso_sender();
        let to = socket2::SockAddr::from(rx.local_addr().unwrap());
        let payload = distinct_segments(10, 1000);
        let n = offload::send(tx.as_raw_fd(), &payload, 1000, Some(&to)).unwrap();
        assert_eq!(n, 10_000);
        let mut buf = vec![0_u8; offload::GRO_BUF_BYTES];
        let r = poll_recv(rx.as_raw_fd(), &mut buf, Duration::from_secs(2)).expect("a burst");
        assert_eq!(r.len, 10_000);
        assert_eq!(r.segment, 1000);
        assert_eq!(r.from, Some(tx.local_addr().unwrap()));
        assert!(!r.truncated);
        assert_eq!(&buf[..r.len], &payload[..]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_gso_send_is_split_for_a_socket_without_gro() {
        use std::os::fd::AsRawFd;
        let rx = plain_receiver();
        let tx = gso_sender();
        let to = socket2::SockAddr::from(rx.local_addr().unwrap());
        let payload = distinct_segments(10, 1000);
        assert_eq!(
            offload::send(tx.as_raw_fd(), &payload, 1000, Some(&to)).unwrap(),
            10_000
        );
        let mut buf = vec![0_u8; 65536];
        for i in 0..10_u8 {
            let (n, _) = rx.recv_from(&mut buf).unwrap();
            assert_eq!(n, 1000, "datagram {i}");
            assert!(buf[..n].iter().all(|b| *b == i), "datagram {i} content");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_tail_segment_is_kept() {
        use std::os::fd::AsRawFd;
        let Some(rx) = gro_socket() else { return };
        let tx = gso_sender();
        let to = socket2::SockAddr::from(rx.local_addr().unwrap());
        let mut payload = distinct_segments(3, 1000);
        payload.extend_from_slice(&[3_u8; 200]);
        assert_eq!(
            offload::send(tx.as_raw_fd(), &payload, 1000, Some(&to)).unwrap(),
            3200
        );
        let mut buf = vec![0_u8; offload::GRO_BUF_BYTES];
        let r = poll_recv(rx.as_raw_fd(), &mut buf, Duration::from_secs(2)).expect("a burst");
        assert_eq!(r.len, 3200);
        assert_eq!(r.segment, 1000);
        let lens: Vec<usize> = buf[..r.len].chunks(r.segment).map(<[u8]>::len).collect();
        assert_eq!(lens, vec![1000, 1000, 1000, 200]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_zero_length_datagram_round_trips() {
        use std::os::fd::AsRawFd;
        let Some(rx) = gro_socket() else { return };
        let tx = gso_sender();
        let to = socket2::SockAddr::from(rx.local_addr().unwrap());
        assert_eq!(offload::send(tx.as_raw_fd(), &[], 0, Some(&to)).unwrap(), 0);
        let mut buf = vec![0_u8; offload::GRO_BUF_BYTES];
        let r = poll_recv(rx.as_raw_fd(), &mut buf, Duration::from_secs(2)).expect("a datagram");
        assert_eq!(r.len, 0);
        assert_eq!(r.segment, 0);
        assert_eq!(offload::segments(0, 0), 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_segments_and_chunk_bytes_math() {
        assert_eq!(offload::segments(0, 0), 1);
        assert_eq!(offload::segments(5, 0), 1);
        assert_eq!(offload::segments(10, 10), 1);
        assert_eq!(offload::segments(11, 10), 2);
        assert_eq!(offload::segments(6400, 100), 64);
        assert_eq!(offload::chunk_bytes(1000), 64_000);
        assert_eq!(offload::chunk_bytes(1472), 64_768);
        assert_eq!(offload::chunk_bytes(100), 6400);
        assert_eq!(offload::chunk_bytes(30_000), 60_000);
        assert_eq!(offload::chunk_bytes(65_507), 65_507);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_send_chunks_stops_at_the_first_error_with_its_offset() {
        let buf = vec![7_u8; 6450];
        let mut calls = Vec::new();
        let r = offload::send_chunks(&buf, 100, |chunk, seg| {
            calls.push((chunk.len(), seg));
            if calls.len() == 2 {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            } else {
                Ok(chunk.len())
            }
        });
        assert_eq!(calls, vec![(6400, 100), (50, 0)]);
        match r {
            offload::GsoSend::Stopped { from, error } => {
                assert_eq!(from, 6400);
                assert!(offload::is_fallback_error(&error));
            }
            offload::GsoSend::Sent => panic!("expected Stopped"),
        }

        let r = offload::send_chunks(&buf, 100, |chunk, _| Ok(chunk.len()));
        assert!(matches!(r, offload::GsoSend::Sent));

        let r = offload::send_chunks(&buf, 100, |_, _| {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        });
        match r {
            offload::GsoSend::Stopped { from, error } => {
                assert_eq!(from, 0);
                assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            }
            offload::GsoSend::Sent => panic!("expected Stopped"),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offload_gso_rejection_is_a_fallback_error() {
        use std::os::fd::AsRawFd;
        let rx = plain_receiver();
        let tx = gso_sender();
        set_no_check(tx.as_raw_fd());
        let to = socket2::SockAddr::from(rx.local_addr().unwrap());
        let payload = distinct_segments(10, 100);
        let err = offload::send(tx.as_raw_fd(), &payload, 100, Some(&to)).unwrap_err();
        assert!(offload::is_fallback_error(&err), "got {err}");
        for piece in payload.chunks(100) {
            assert_eq!(
                offload::send(tx.as_raw_fd(), piece, 0, Some(&to)).unwrap(),
                100
            );
        }
        let mut buf = vec![0_u8; 65536];
        for i in 0..10_u8 {
            let (n, _) = rx.recv_from(&mut buf).unwrap();
            assert_eq!(n, 100);
            assert!(buf[..n].iter().all(|b| *b == i));
        }
    }

    /// Pins the kernel premise of D32a: a burst queued while UDP_GRO was on is
    /// read fused and without a cmsg once UDP_GRO is off, so rfw never clears it.
    #[cfg(target_os = "linux")]
    #[test]
    fn disabling_udp_gro_leaves_queued_bursts_merged() {
        use std::os::fd::AsRawFd;
        let Some(rx) = gro_socket() else { return };
        let tx = gso_sender();
        let to = socket2::SockAddr::from(rx.local_addr().unwrap());
        let payload = distinct_segments(10, 100);
        assert_eq!(
            offload::send(tx.as_raw_fd(), &payload, 100, Some(&to)).unwrap(),
            1000
        );
        // Let the burst reach the receive queue while GRO is still on.
        std::thread::sleep(Duration::from_millis(50));
        offload::set_gro(rx.as_raw_fd(), false).unwrap();
        let mut buf = vec![0_u8; offload::GRO_BUF_BYTES];
        let r = poll_recv(rx.as_raw_fd(), &mut buf, Duration::from_secs(2)).expect("data");
        assert_eq!(r.len, 1000);
        assert_eq!(r.segment, 0);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reply_coalesced_falls_back_once_and_delivers_everything() {
        use std::os::fd::AsRawFd;
        let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        set_no_check(listener.as_raw_fd());
        let client = plain_receiver();
        let client_addr = client.local_addr().unwrap();
        let to = socket2::SockAddr::from(client_addr);
        listener.writable().await.unwrap();
        let gso_ok = AtomicBool::new(true);
        let data = distinct_segments(8, 500);
        for round in 0..2 {
            let mut batch = Batch::default();
            let r = reply_coalesced(
                &listener,
                &to,
                client_addr,
                &data,
                500,
                &gso_ok,
                &mut batch,
                "t",
            );
            assert!(r.is_none(), "round {round}");
            assert!(!gso_ok.load(Ordering::Relaxed), "round {round}");
            assert_eq!(batch.datagrams, 8, "round {round}");
            assert_eq!(batch.bytes, 4000, "round {round}");
            assert_eq!(batch.dropped, 0, "round {round}");
            let mut buf = vec![0_u8; 65536];
            for i in 0..8_u8 {
                let (n, _) = client.recv_from(&mut buf).unwrap();
                assert_eq!(n, 500);
                assert!(buf[..n].iter().all(|b| *b == i), "round {round} piece {i}");
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn forward_upstream_coalesced_falls_back_and_counts() {
        use std::os::fd::AsRawFd;
        let target = plain_receiver();
        let remote = target.local_addr().unwrap();
        let upstream = connect_upstream(remote, None).unwrap();
        set_no_check(upstream.as_raw_fd());
        let stats = Arc::new(ForwarderStats::with_protocol("t".into(), Protocol::Udp));
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let shared = Arc::new(SessionShared {
            client: "127.0.0.1:9".parse().unwrap(),
            remote,
            upstream,
            last_activity_ms: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            rx_datagrams: AtomicU64::new(0),
            _guard: ConnGuard::new(stats.clone()),
            _permit: permit,
            gro: false,
            listener_gso: Arc::new(AtomicBool::new(true)),
        });
        let mut entry = SessionEntry {
            shared,
            cancel: CancellationToken::new(),
            opened: Instant::now(),
            tx_datagrams: 0,
            gso_ok: true,
        };
        let data = distinct_segments(8, 500);
        let mut batch = Batch::default();
        let mut gates = Gates::default();
        forward_upstream_coalesced(
            &mut entry, &data, 500, 1, &mut batch, &stats, &mut gates, "t",
        );
        assert!(!entry.gso_ok);
        assert_eq!(entry.tx_datagrams, 8);
        assert_eq!(batch.datagrams, 8);
        assert_eq!(batch.bytes, 4000);
        assert_eq!(batch.dropped, 0);
        let mut buf = vec![0_u8; 65536];
        for i in 0..8_u8 {
            let (n, _) = target.recv_from(&mut buf).unwrap();
            assert_eq!(n, 500);
            assert!(buf[..n].iter().all(|b| *b == i), "piece {i}");
        }
    }

    // ---- Forwarder-level GRO/GSO tests (3.2) ---------------------------------

    /// Backend with UDP_GRO that echoes every receive with the same segmentation
    /// (GSO when coalesced). A std thread that lives until the process exits.
    /// `None` on a kernel without UDP_GRO.
    #[cfg(target_os = "linux")]
    fn spawn_gso_echo_local() -> Option<u16> {
        use std::os::fd::AsRawFd;
        let sock = gro_socket()?;
        sock.set_nonblocking(false).unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let port = sock.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let fd = sock.as_raw_fd();
            let mut buf = vec![0_u8; offload::GRO_BUF_BYTES];
            loop {
                let Ok(r) = offload::recv(fd, &mut buf, true) else {
                    continue; // read timeout
                };
                let to = socket2::SockAddr::from(r.from.unwrap());
                if offload::is_coalesced(r.len, r.segment) {
                    let _ = offload::send_chunks(&buf[..r.len], r.segment, |c, seg| {
                        offload::send(fd, c, seg, Some(&to))
                    });
                } else {
                    let _ = offload::send(fd, &buf[..r.len], 0, Some(&to));
                }
            }
        });
        Some(port)
    }

    /// Plain echo backend (no GRO) recording every datagram it receives, in order.
    #[cfg(target_os = "linux")]
    fn spawn_recording_echo() -> (u16, Arc<Mutex<Vec<Vec<u8>>>>) {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let port = sock.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            let mut buf = vec![0_u8; 65536];
            loop {
                if let Ok((n, peer)) = sock.recv_from(&mut buf) {
                    log.lock().unwrap().push(buf[..n].to_vec());
                    let _ = sock.send_to(&buf[..n], peer);
                }
            }
        });
        (port, seen)
    }

    /// `n` datagrams of `seg` bytes; datagram `i` of burst `b` is `[b, i]` repeated.
    #[cfg(target_os = "linux")]
    fn unit_burst(b: u8, n: usize, seg: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| {
                (0..seg)
                    .map(|k| if k % 2 == 0 { b } else { i as u8 })
                    .collect()
            })
            .collect()
    }

    /// Send `datagrams` in one sendmsg from a plain client socket; GSO with
    /// segment = the first datagram's length (the last may be shorter), or a
    /// plain send for a single datagram.
    #[cfg(target_os = "linux")]
    fn send_burst(c: &UdpSocket, port: u16, datagrams: &[Vec<u8>]) {
        use std::os::fd::AsRawFd;
        let seg = if datagrams.len() > 1 {
            datagrams[0].len() as u16
        } else {
            0
        };
        let to = socket2::SockAddr::from(SocketAddr::from(([127, 0, 0, 1], port)));
        offload::send(c.as_raw_fd(), &datagrams.concat(), seg, Some(&to)).unwrap();
    }

    /// Expect every datagram back, identical and in order, within `within` in total.
    #[cfg(target_os = "linux")]
    async fn expect_echoes(c: &UdpSocket, datagrams: &[Vec<u8>], within: Duration, what: &str) {
        let deadline = tokio::time::Instant::now() + within;
        let mut buf = vec![0_u8; 65536];
        for (i, want) in datagrams.iter().enumerate() {
            let got = tokio::time::timeout_at(deadline, c.recv_from(&mut buf))
                .await
                .unwrap_or_else(|_| panic!("{what}: echo {i}/{} missing", datagrams.len()))
                .unwrap();
            assert_eq!(&buf[..got.0], want.as_slice(), "{what}: echo {i} differs");
        }
    }

    #[cfg(target_os = "linux")]
    fn gro_settings() -> Settings {
        Settings {
            udp_gro_gso: true,
            ..Settings::default()
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gro_gso_path_preserves_datagram_boundaries() {
        let Some(echo) = spawn_gso_echo_local() else {
            return;
        };
        let f = start_fwd("127.0.0.1", echo, gro_settings()).await;
        let c = client().await;
        let mut tail = unit_burst(2, 11, 1000);
        tail[10].truncate(300);
        let bursts = [
            ("64 x 1000", unit_burst(0, 64, 1000)),
            ("10 x 1000 + 300", tail),
            ("single 1000", unit_burst(3, 1, 1000)),
        ];
        for (what, burst) in &bursts {
            send_burst(&c, f.port, burst);
            expect_echoes(&c, burst, Duration::from_secs(3), what).await;
        }
        wait_until(
            || load(&f.stats.datagrams_received) == 76,
            Duration::from_secs(3),
            "received counter",
        )
        .await;
        assert_eq!(load(&f.stats.datagrams_sent), 76);
        assert_eq!(load(&f.stats.datagrams_received), 76);
        assert_eq!(load(&f.stats.datagrams_dropped), 0);
        assert_eq!(load(&f.stats.bytes_sent), 75_300);
        assert_eq!(load(&f.stats.bytes_received), 75_300);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gro_toggle_never_merges_datagrams() {
        let (backend, seen) = spawn_recording_echo();
        let f = start_fwd("127.0.0.1", backend, Settings::default()).await;
        for round in 0..6_u8 {
            f.knobs.update(&Settings {
                udp_gro_gso: round % 2 == 0,
                ..Settings::default()
            });
            tokio::time::sleep(Duration::from_millis(1300)).await; // one sweep tick applies it
            seen.lock().unwrap().clear();
            let burst = unit_burst(round, 32, 500);
            let c = client().await; // a new session under the current knob state
            send_burst(&c, f.port, &burst);
            expect_echoes(
                &c,
                &burst,
                Duration::from_secs(3),
                &format!("round {round}"),
            )
            .await;
            let recorded = seen.lock().unwrap().clone();
            assert_eq!(recorded, burst, "backend datagrams in round {round}");
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesced_burst_from_a_refused_client_counts_every_datagram() {
        let (echo, _) = spawn_udp_echo("127.0.0.1:0").await;
        let settings = Settings {
            udp_gro_gso: true,
            udp_max_sessions: 1,
            ..Settings::default()
        };
        let f = start_fwd("127.0.0.1", echo, settings).await;
        let a = client().await;
        assert!(roundtrip(&a, f.port, b"a", Duration::from_secs(2))
            .await
            .is_some());
        let b = client().await;
        send_burst(&b, f.port, &unit_burst(0, 10, 100));
        wait_until(
            || load(&f.stats.rejected_connections) == 10,
            Duration::from_secs(2),
            "rejected_connections == 10",
        )
        .await;
        assert!(load(&f.stats.datagrams_dropped) >= 10);
        let mut buf = vec![0_u8; 65536];
        assert!(
            tokio::time::timeout(Duration::from_millis(500), b.recv_from(&mut buf))
                .await
                .is_err(),
            "a refused client must receive nothing"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_burst_of_a_new_session_is_never_dropped() {
        let Some(echo) = spawn_gso_echo_local() else {
            return;
        };
        let f = start_fwd("127.0.0.1", echo, gro_settings()).await;
        for i in 0..20_u8 {
            let c = client().await;
            let burst = unit_burst(i, 16, 200);
            send_burst(&c, f.port, &burst);
            expect_echoes(&c, &burst, Duration::from_secs(2), &format!("client {i}")).await;
        }
        assert_eq!(load(&f.stats.datagrams_dropped), 0);
    }
}
