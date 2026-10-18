//! Outbound alerting to a Google Chat incoming webhook.
//!
//! Design constraints, in priority order:
//!
//! 1. **The data plane never waits on the webhook.** Producers hand events to
//!    a bounded queue with `try_send` and move on; a full queue drops (and
//!    counts) the event instead of blocking an accept loop or a connection.
//! 2. **One noisy source cannot flood the space.** Repetitive event kinds
//!    carry a throttle key and window; suppressed repeats are folded into the
//!    next delivered message as a count, never silently lost.
//! 3. **Delivery is best effort and bounded.** Transport errors, `429` and
//!    `5xx` are retried a few times with backoff; anything else is logged and
//!    counted (`rfw_webhook_*` metrics). A webhook outage can never escalate
//!    back into the forwarding process.
//! 4. **The URL is a credential.** Google Chat webhook URLs carry `key` and
//!    `token` query parameters; they are redacted in every log line and never
//!    echoed back in an error.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Protocol;

/// Google Chat rejects `text` messages beyond 4096 characters; stay under it.
pub const MAX_TEXT_CHARS: usize = 4000;
/// Events waiting for delivery. Producers drop (and count) beyond this.
const QUEUE_CAPACITY: usize = 256;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Delivery attempts per message (transport errors, `429` and `5xx` retry).
const MAX_ATTEMPTS: u32 = 3;
const RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
/// Upper bound honoured for a `Retry-After` header.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
/// Minimum spacing between two deliveries. Google Chat rate-limits webhook
/// posts per space; pacing here keeps a burst of distinct events (a reload
/// that flips several forwarders at once) from turning into `429`s.
const MIN_SEND_SPACING: Duration = Duration::from_millis(1000);
/// Longest an error string is allowed to get inside a message.
const MAX_DETAIL_CHARS: usize = 300;

/// Throttle windows per repetitive event kind (see [`Event::throttle`]).
pub const THROTTLE_DNS: Duration = Duration::from_secs(60);
pub const THROTTLE_BIND: Duration = Duration::from_secs(300);
pub const THROTTLE_LIMIT: Duration = Duration::from_secs(300);
pub const THROTTLE_RELOAD_FAILED: Duration = Duration::from_secs(60);
pub const THROTTLE_INTERNAL: Duration = Duration::from_secs(60);

/// How loud an event is. Drives the icon in the message and the log level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// Result of one reachability probe of a forwarder's target, as shown in the
/// startup message.
#[derive(Debug, Clone)]
pub struct ProbeReport {
    pub label: String,
    pub target: String,
    pub protocol: Protocol,
    /// `Ok((address that answered, connect latency))` or the error text.
    pub outcome: Result<(SocketAddr, Duration), String>,
}

/// Everything the process can tell the operator about.
#[derive(Debug, Clone)]
pub enum Event {
    /// All forwarders spawned; one reachability probe per target done.
    Started { forwarders: Vec<ProbeReport> },
    /// Shutdown signal received; in-flight connections are being drained.
    Stopping {
        signal: String,
        active_connections: u64,
    },
    /// The target hostname resolved to a different address set than before.
    DnsChanged {
        label: String,
        host: String,
        old: Vec<IpAddr>,
        new: Vec<IpAddr>,
    },
    /// The target has been failing probes for longer than the threshold.
    TargetUnreachable {
        label: String,
        target: String,
        down_for: Duration,
        last_error: String,
    },
    /// A target that was alerted as unreachable answers again.
    TargetRecovered {
        label: String,
        target: String,
        down_for: Duration,
        address: Option<SocketAddr>,
    },
    /// The local listener could not be bound (port taken, permission).
    BindFailed {
        label: String,
        local_addr: String,
        error: String,
        attempts: u32,
    },
    /// The listener bound after one or more failures.
    BindRecovered {
        label: String,
        local_addr: String,
        failed_attempts: u32,
    },
    /// Hot-reload applied.
    ConfigReloaded {
        active: usize,
        added: Vec<String>,
        removed: Vec<String>,
    },
    /// Hot-reload rejected; the running configuration was kept.
    ConfigReloadFailed { path: String, error: String },
    /// `max_connections` saturated; new connections are being refused.
    ConnectionLimitReached { label: String, rejected_total: u64 },
    /// `udp_max_sessions` saturated; datagrams from new UDP clients are being dropped.
    UdpSessionLimitReached { label: String, rejected_total: u64 },
    /// Anything else that went wrong inside the process.
    Internal {
        component: &'static str,
        message: String,
    },
}

impl Event {
    /// Short stable name for logs and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::Started { .. } => "started",
            Event::Stopping { .. } => "stopping",
            Event::DnsChanged { .. } => "dns_changed",
            Event::TargetUnreachable { .. } => "target_unreachable",
            Event::TargetRecovered { .. } => "target_recovered",
            Event::BindFailed { .. } => "bind_failed",
            Event::BindRecovered { .. } => "bind_recovered",
            Event::ConfigReloaded { .. } => "config_reloaded",
            Event::ConfigReloadFailed { .. } => "config_reload_failed",
            Event::ConnectionLimitReached { .. } => "connection_limit_reached",
            Event::UdpSessionLimitReached { .. } => "udp_session_limit_reached",
            Event::Internal { .. } => "internal",
        }
    }

    pub fn severity(&self) -> Severity {
        match self {
            Event::Started { forwarders } => {
                if forwarders.iter().any(|f| f.outcome.is_err()) {
                    Severity::Warning
                } else {
                    Severity::Info
                }
            }
            Event::Stopping { .. }
            | Event::DnsChanged { .. }
            | Event::TargetRecovered { .. }
            | Event::BindRecovered { .. }
            | Event::ConfigReloaded { .. } => Severity::Info,
            Event::ConfigReloadFailed { .. }
            | Event::ConnectionLimitReached { .. }
            | Event::UdpSessionLimitReached { .. }
            | Event::Internal { .. } => Severity::Warning,
            Event::TargetUnreachable { .. } | Event::BindFailed { .. } => Severity::Critical,
        }
    }

    /// Throttle key and window. `None` means the event is never throttled
    /// (one-shot lifecycle events and state transitions that the producer
    /// already de-duplicates).
    fn throttle(&self) -> Option<(String, Duration)> {
        match self {
            Event::DnsChanged { label, .. } => Some((format!("dns:{label}"), THROTTLE_DNS)),
            Event::BindFailed { label, .. } => Some((format!("bind:{label}"), THROTTLE_BIND)),
            Event::ConnectionLimitReached { label, .. }
            | Event::UdpSessionLimitReached { label, .. } => {
                Some((format!("limit:{label}"), THROTTLE_LIMIT))
            }
            Event::ConfigReloadFailed { .. } => {
                Some(("reload-failed".to_string(), THROTTLE_RELOAD_FAILED))
            }
            Event::Internal { component, .. } => {
                Some((format!("internal:{component}"), THROTTLE_INTERNAL))
            }
            _ => None,
        }
    }

    /// Render the Google Chat message text.
    ///
    /// Uses the basic text formatting Google Chat understands (`*bold*`,
    /// `_italic_`, `` `mono` ``). Every operator-supplied string (labels,
    /// hostnames, error text) goes through [`mono`] or [`plain`] so it can
    /// neither break the formatting nor run to arbitrary length.
    pub fn render(&self, instance: &InstanceInfo, suppressed: u64) -> String {
        let icon = match self.severity() {
            Severity::Info => "🟢",
            Severity::Warning => "🟠",
            Severity::Critical => "🔴",
        };
        let mut text = String::with_capacity(512);
        match self {
            Event::Started { forwarders } => {
                let bad = forwarders.iter().filter(|f| f.outcome.is_err()).count();
                text.push_str(&format!(
                    "{icon} *rfw started* on {} — {} forwarder(s)",
                    mono(&instance.name),
                    forwarders.len()
                ));
                if bad > 0 {
                    text.push_str(&format!(", {bad} target(s) unreachable"));
                }
                for f in forwarders {
                    text.push('\n');
                    match &f.outcome {
                        Ok((addr, latency)) if f.protocol == Protocol::Udp => {
                            text.push_str(&format!(
                                "• {} ✅ resolved to {} ({} ms, udp: passive check)",
                                mono(&f.label),
                                mono(&addr.to_string()),
                                latency.as_millis()
                            ))
                        }
                        Ok((addr, latency)) => text.push_str(&format!(
                            "• {} ✅ reachable at {} ({} ms)",
                            mono(&f.label),
                            mono(&addr.to_string()),
                            latency.as_millis()
                        )),
                        Err(e) => text.push_str(&format!(
                            "• {} ❌ unreachable: {}",
                            mono(&f.label),
                            plain(e)
                        )),
                    }
                }
            }
            Event::Stopping {
                signal,
                active_connections,
            } => {
                text.push_str(&format!(
                    "🟡 *rfw stopping* on {} ({}) — draining {} active connection(s)",
                    mono(&instance.name),
                    plain(signal),
                    active_connections
                ));
            }
            Event::DnsChanged {
                label,
                host,
                old,
                new,
            } => {
                text.push_str(&format!(
                    "🔵 *DNS change* for {}\nForwarder {}\n{} → {}\nNew connections now go to the new address; connections already open stay on the old one.",
                    mono(host),
                    mono(label),
                    mono(&join_ips(old)),
                    mono(&join_ips(new))
                ));
            }
            Event::TargetUnreachable {
                label,
                target,
                down_for,
                last_error,
            } => {
                text.push_str(&format!(
                    "{icon} *Target unreachable* {}\nForwarder {}\nFailing for {}; clients connecting to this port are being refused.\nLast error: {}",
                    mono(target),
                    mono(label),
                    fmt_duration(*down_for),
                    plain(last_error)
                ));
            }
            Event::TargetRecovered {
                label,
                target,
                down_for,
                address,
            } => {
                text.push_str(&format!(
                    "✅ *Target reachable again* {}\nForwarder {}\nWas unreachable for {}",
                    mono(target),
                    mono(label),
                    fmt_duration(*down_for)
                ));
                if let Some(addr) = address {
                    text.push_str(&format!("\nAnswering at {}", mono(&addr.to_string())));
                }
            }
            Event::BindFailed {
                label,
                local_addr,
                error,
                attempts,
            } => {
                text.push_str(&format!(
                    "{icon} *Cannot bind* {}\nForwarder {}\nError: {}\nAttempt {attempts}; retrying every 5 s. Clients cannot connect to this port until it binds.",
                    mono(local_addr),
                    mono(label),
                    plain(error)
                ));
            }
            Event::BindRecovered {
                label,
                local_addr,
                failed_attempts,
            } => {
                text.push_str(&format!(
                    "✅ *Listening again* on {}\nForwarder {}\nBound after {failed_attempts} failed attempt(s)",
                    mono(local_addr),
                    mono(label)
                ));
            }
            Event::ConfigReloaded {
                active,
                added,
                removed,
            } => {
                text.push_str(&format!(
                    "🔵 *Config reloaded* on {} — {active} forwarder(s) active",
                    mono(&instance.name)
                ));
                if added.is_empty() && removed.is_empty() {
                    text.push_str("\nForwarder list unchanged (settings only)");
                }
                if !added.is_empty() {
                    text.push_str(&format!("\nAdded: {}", join_mono(added)));
                }
                if !removed.is_empty() {
                    text.push_str(&format!("\nRemoved: {}", join_mono(removed)));
                }
            }
            Event::ConfigReloadFailed { path, error } => {
                text.push_str(&format!(
                    "{icon} *Config reload failed* on {} — running configuration kept\nFile: {}\nError: {}",
                    mono(&instance.name),
                    mono(path),
                    plain(error)
                ));
            }
            Event::ConnectionLimitReached {
                label,
                rejected_total,
            } => {
                text.push_str(&format!(
                    "{icon} *Connection limit reached*\nForwarder {}\nmax_connections is saturated; new connections are being refused ({rejected_total} refused so far).",
                    mono(label)
                ));
            }
            Event::UdpSessionLimitReached {
                label,
                rejected_total,
            } => {
                text.push_str(&format!(
                    "{icon} *UDP session limit reached*\nForwarder {}\nudp_max_sessions is saturated; datagrams from new clients are being dropped ({rejected_total} refused so far).",
                    mono(label)
                ));
            }
            Event::Internal { component, message } => {
                text.push_str(&format!(
                    "{icon} *Internal problem* in {} on {}\n{}",
                    mono(component),
                    mono(&instance.name),
                    plain(message)
                ));
            }
        }

        if suppressed > 0 {
            text.push_str(&format!(
                "\n_(+{suppressed} similar event(s) suppressed since the previous message)_"
            ));
        }
        text.push_str(&format!(
            "\n_{} · pid {} · rfw {} · {}_",
            plain(&instance.name),
            instance.pid,
            instance.version,
            utc_now()
        ));
        truncate_chars(text, MAX_TEXT_CHARS)
    }
}

/// Identifies this process in every message: several rfw instances typically
/// post into one space.
#[derive(Debug, Clone)]
pub struct InstanceInfo {
    pub name: String,
    pub version: &'static str,
    pub pid: u32,
}

impl InstanceInfo {
    /// `name` defaults to the machine hostname.
    pub fn new(name: Option<String>) -> Self {
        Self {
            name: name
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(default_instance_name),
            version: env!("CARGO_PKG_VERSION"),
            pid: std::process::id(),
        }
    }
}

/// Hostname from the environment, then from the OS, then a constant.
pub fn default_instance_name() -> String {
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                return v;
            }
        }
    }
    #[cfg(unix)]
    {
        let mut buf = [0_u8; 256];
        // SAFETY: gethostname writes at most `buf.len()` bytes into a valid,
        // writable buffer and returns 0 on success.
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc == 0 {
            let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
            if let Ok(s) = std::str::from_utf8(&buf[..end]) {
                if !s.is_empty() {
                    return s.to_string();
                }
            }
        }
    }
    "rfw".to_string()
}

// ---------------------------------------------------------------------------
// Text helpers
// ---------------------------------------------------------------------------

/// Wrap an operator-supplied value in monospace, neutralising backticks.
fn mono(s: &str) -> String {
    let inner: String = truncate_chars(s.replace('`', "'"), MAX_DETAIL_CHARS);
    format!("`{}`", strip_control(&inner))
}

/// Flatten an arbitrary string (error text) to a single bounded line.
fn plain(s: &str) -> String {
    truncate_chars(strip_control(s), MAX_DETAIL_CHARS)
}

fn strip_control(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_space = false;
    for c in s.chars() {
        let c = if c.is_control() { ' ' } else { c };
        if c == ' ' {
            if !last_space {
                out.push(' ');
            }
            last_space = true;
        } else {
            out.push(c);
            last_space = false;
        }
    }
    out.trim().to_string()
}

fn truncate_chars(s: String, max: usize) -> String {
    if s.chars().count() <= max {
        return s;
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn join_ips(ips: &[IpAddr]) -> String {
    if ips.is_empty() {
        return "(none)".to_string();
    }
    ips.iter()
        .map(|ip| ip.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_mono(items: &[String]) -> String {
    items.iter().map(|s| mono(s)).collect::<Vec<_>>().join(", ")
}

/// Human duration: `45s`, `5m 12s`, `2h 03m`, `3d 04h`.
pub fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else if secs < 86_400 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {:02}h", secs / 86_400, (secs % 86_400) / 3600)
    }
}

/// `YYYY-MM-DD HH:MM:SS UTC` for the message footer, without a date crate.
pub fn utc_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format_utc(now)
}

/// Civil date from Unix seconds (Howard Hinnant's `civil_from_days`).
pub fn format_utc(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    format!("{year:04}-{month:02}-{d:02} {h:02}:{m:02}:{s:02} UTC")
}

// ---------------------------------------------------------------------------
// URL handling
// ---------------------------------------------------------------------------

/// Parse and sanity-check a webhook URL at startup.
///
/// `http://` is accepted (local relays, tests) but the caller should warn:
/// the URL's `token` then travels in clear text.
pub fn validate_webhook_url(raw: &str) -> Result<url::Url, String> {
    let url = url::Url::parse(raw.trim()).map_err(|e| format!("not a valid URL: {e}"))?;
    match url.scheme() {
        "https" | "http" => {}
        other => return Err(format!("unsupported scheme '{other}' (expected https)")),
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err("missing host".to_string());
    }
    Ok(url)
}

/// The URL with every query value and any userinfo replaced by `***`.
///
/// Google Chat webhooks authenticate with `key=` and `token=` query
/// parameters, so the raw URL must never reach a log line.
pub fn redact_url(url: &url::Url) -> String {
    let mut r = url.clone();
    let _ = r.set_username("");
    let _ = r.set_password(None);
    if r.query().is_some() {
        let pairs: Vec<String> = url.query_pairs().map(|(k, _)| format!("{k}=***")).collect();
        r.set_query(Some(&pairs.join("&")));
    }
    r.to_string()
}

// ---------------------------------------------------------------------------
// Throttle
// ---------------------------------------------------------------------------

/// Per-key rate limiter with suppression counting.
///
/// The first event for a key is admitted. Further events inside the window
/// are suppressed and counted. The first event after the window is admitted
/// again and carries the count of what was suppressed in between.
#[derive(Debug, Default)]
pub struct Throttle {
    entries: HashMap<String, ThrottleEntry>,
}

#[derive(Debug)]
struct ThrottleEntry {
    last_sent: Instant,
    suppressed: u64,
}

/// Outcome of asking the throttle about an event.
#[derive(Debug, PartialEq, Eq)]
pub enum Admit {
    /// Deliver, mentioning this many suppressed predecessors.
    Send {
        suppressed: u64,
    },
    Suppress,
}

impl Throttle {
    pub fn admit(&mut self, key: &str, window: Duration, now: Instant) -> Admit {
        match self.entries.get_mut(key) {
            Some(entry) if now.duration_since(entry.last_sent) < window => {
                entry.suppressed += 1;
                Admit::Suppress
            }
            Some(entry) => {
                let suppressed = std::mem::take(&mut entry.suppressed);
                entry.last_sent = now;
                Admit::Send { suppressed }
            }
            None => {
                self.entries.insert(
                    key.to_string(),
                    ThrottleEntry {
                        last_sent: now,
                        suppressed: 0,
                    },
                );
                Admit::Send { suppressed: 0 }
            }
        }
    }

    /// Forget keys idle for longer than `max_idle` so a churning label set
    /// (hot-reloads with ever-new forwarders) cannot grow the map without
    /// bound.
    pub fn prune(&mut self, now: Instant, max_idle: Duration) {
        self.entries
            .retain(|_, e| now.duration_since(e.last_sent) < max_idle);
    }
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// Lifetime delivery counters, exposed on `/metrics` as `rfw_webhook_*`.
#[derive(Debug)]
pub struct NotifierCounters {
    enabled: bool,
    /// Messages accepted by the webhook (2xx).
    pub sent: AtomicU64,
    /// Messages given up on after the retry budget.
    pub failed: AtomicU64,
    /// Events dropped because the queue was full (or the worker was gone).
    pub dropped: AtomicU64,
    /// Events folded into a later message by the throttle.
    pub suppressed: AtomicU64,
}

impl NotifierCounters {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            sent: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> NotifierSnapshot {
        NotifierSnapshot {
            enabled: self.enabled,
            sent: self.sent.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            suppressed: self.suppressed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotifierSnapshot {
    pub enabled: bool,
    pub sent: u64,
    pub failed: u64,
    pub dropped: u64,
    pub suppressed: u64,
}

// ---------------------------------------------------------------------------
// Notifier handle (producer side)
// ---------------------------------------------------------------------------

/// Cheap, cloneable producer handle. A disabled notifier is a no-op, so call
/// sites never need to special-case "webhook not configured".
#[derive(Clone)]
pub struct Notifier {
    tx: Option<mpsc::Sender<Event>>,
    counters: Arc<NotifierCounters>,
}

impl std::fmt::Debug for Notifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifier")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl Notifier {
    /// No webhook configured: every `notify` is a no-op.
    pub fn disabled() -> Self {
        Self {
            tx: None,
            counters: Arc::new(NotifierCounters::new(false)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    pub fn counters(&self) -> Arc<NotifierCounters> {
        self.counters.clone()
    }

    /// Queue an event. Never blocks, never fails loudly: a full queue drops
    /// the event and bumps `dropped`.
    pub fn notify(&self, event: Event) {
        let Some(tx) = &self.tx else { return };
        match tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(ev)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                warn!(event = ev.kind(), "Webhook queue full; event dropped");
            }
            Err(mpsc::error::TrySendError::Closed(ev)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                debug!(event = ev.kind(), "Webhook worker gone; event dropped");
            }
        }
    }

    /// A notifier whose events land in the returned receiver instead of a
    /// webhook. Lets producers be tested without HTTP.
    #[cfg(test)]
    pub(crate) fn for_test() -> (Self, mpsc::Receiver<Event>) {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        (
            Self {
                tx: Some(tx),
                counters: Arc::new(NotifierCounters::new(true)),
            },
            rx,
        )
    }
}

// ---------------------------------------------------------------------------
// Worker (consumer side)
// ---------------------------------------------------------------------------

/// What [`start`] needs.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub url: url::Url,
    pub instance: InstanceInfo,
}

/// Owns the delivery task; `shutdown` flushes and stops it.
pub struct NotifierHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl NotifierHandle {
    /// Stop accepting new events, deliver what is already queued (bounded by
    /// `flush`), then return. Aborts the worker if the deadline passes.
    pub async fn shutdown(self, flush: Duration) {
        self.cancel.cancel();
        if tokio::time::timeout(flush, self.task).await.is_err() {
            warn!(
                "Webhook flush did not finish within {}s; pending alerts abandoned",
                flush.as_secs()
            );
        }
    }
}

/// Build the HTTP client and spawn the delivery worker.
///
/// Must be called from inside the Tokio runtime.
pub fn start(config: WebhookConfig) -> anyhow::Result<(Notifier, NotifierHandle)> {
    let client = build_client()?;
    let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
    let counters = Arc::new(NotifierCounters::new(true));
    let cancel = CancellationToken::new();

    if config.url.scheme() == "http" {
        warn!(
            webhook = %redact_url(&config.url),
            "Webhook URL uses plain http; its token travels unencrypted"
        );
    }
    info!(webhook = %redact_url(&config.url), instance = %config.instance.name, "Webhook alerting enabled");

    let worker = Worker {
        client,
        url: config.url,
        instance: config.instance,
        counters: counters.clone(),
        throttle: Throttle::default(),
        last_send: None,
    };
    let task = tokio::spawn(worker.run(rx, cancel.clone()));

    Ok((
        Notifier {
            tx: Some(tx),
            counters,
        },
        NotifierHandle { cancel, task },
    ))
}

/// HTTPS client: rustls + `ring`, bundled Mozilla roots, no redirects.
///
/// Redirects are refused on purpose: following one would replay the URL's
/// credentials against whatever host the redirect names.
fn build_client() -> anyhow::Result<reqwest::Client> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| anyhow::anyhow!("TLS configuration failed: {e}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();

    reqwest::Client::builder()
        .user_agent(format!("rfw/{}", env!("CARGO_PKG_VERSION")))
        .tls_backend_preconfigured(tls)
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| anyhow::anyhow!("HTTP client for the webhook failed to build: {e}"))
}

struct Worker {
    client: reqwest::Client,
    url: url::Url,
    instance: InstanceInfo,
    counters: Arc<NotifierCounters>,
    throttle: Throttle,
    last_send: Option<Instant>,
}

impl Worker {
    async fn run(mut self, mut rx: mpsc::Receiver<Event>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                ev = rx.recv() => match ev {
                    Some(ev) => self.process(ev).await,
                    None => return,
                },
            }
        }
        // Shutdown flush: deliver what producers already queued (typically
        // the "stopping" message). The caller bounds this with a timeout.
        rx.close();
        while let Ok(ev) = rx.try_recv() {
            self.process(ev).await;
        }
    }

    async fn process(&mut self, event: Event) {
        let now = Instant::now();
        let suppressed = match event.throttle() {
            Some((key, window)) => match self.throttle.admit(&key, window, now) {
                Admit::Send { suppressed } => suppressed,
                Admit::Suppress => {
                    self.counters.suppressed.fetch_add(1, Ordering::Relaxed);
                    debug!(event = event.kind(), key = %key, "Webhook event suppressed by throttle");
                    return;
                }
            },
            None => 0,
        };
        self.throttle.prune(now, Duration::from_secs(3600));

        if let Some(last) = self.last_send {
            let wait = MIN_SEND_SPACING.saturating_sub(last.elapsed());
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }

        let text = event.render(&self.instance, suppressed);
        self.last_send = Some(Instant::now());
        match self.deliver(&text).await {
            Ok(()) => {
                self.counters.sent.fetch_add(1, Ordering::Relaxed);
                debug!(event = event.kind(), "Webhook message delivered");
            }
            Err(e) => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                error!(
                    event = event.kind(),
                    webhook = %redact_url(&self.url),
                    error = %e,
                    "Webhook delivery failed"
                );
            }
        }
    }

    /// POST `{"text": ...}` with bounded retries. Errors never contain the URL.
    async fn deliver(&self, text: &str) -> Result<(), String> {
        let payload = serde_json::json!({ "text": text });
        let mut last_err = String::from("no attempt made");
        for attempt in 1..=MAX_ATTEMPTS {
            let sent = self
                .client
                .post(self.url.clone())
                .json(&payload)
                .send()
                .await;
            let delay = match sent {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(());
                    }
                    let retry_after = parse_retry_after(resp.headers());
                    let body = resp.text().await.unwrap_or_default();
                    last_err = format!("HTTP {}: {}", status.as_u16(), plain(&body));
                    let retryable = status.as_u16() == 429 || status.is_server_error();
                    if !retryable {
                        return Err(last_err);
                    }
                    retry_after.unwrap_or_else(|| backoff(attempt))
                }
                Err(e) => {
                    last_err = format!("transport error: {}", e.without_url());
                    backoff(attempt)
                }
            };
            if attempt < MAX_ATTEMPTS {
                warn!(attempt, error = %last_err, "Webhook delivery attempt failed; retrying in {delay:?}");
                tokio::time::sleep(delay).await;
            }
        }
        Err(format!("{last_err} (after {MAX_ATTEMPTS} attempts)"))
    }
}

fn backoff(attempt: u32) -> Duration {
    RETRY_BASE_DELAY * 2_u32.saturating_pow(attempt.saturating_sub(1))
}

/// `Retry-After` in seconds, capped. Date-form values are ignored.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|s| Duration::from_secs(s).min(MAX_RETRY_AFTER))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn instance() -> InstanceInfo {
        InstanceInfo {
            name: "test-host".to_string(),
            version: "9.9.9",
            pid: 42,
        }
    }

    // ---- URL --------------------------------------------------------------

    #[test]
    fn webhook_url_validation() {
        assert!(validate_webhook_url(
            "https://chat.googleapis.com/v1/spaces/AAA/messages?key=k&token=t"
        )
        .is_ok());
        assert!(validate_webhook_url(" http://127.0.0.1:8080/hook ").is_ok());
        assert!(validate_webhook_url("ftp://x/y")
            .unwrap_err()
            .contains("scheme"));
        assert!(validate_webhook_url("chat.googleapis.com/v1").is_err());
        assert!(validate_webhook_url("https://").is_err(), "empty host");
        assert!(validate_webhook_url("").is_err());
    }

    #[test]
    fn redaction_hides_every_query_value_and_userinfo() {
        let u = validate_webhook_url(
            "https://user:pw@chat.googleapis.com/v1/spaces/AAA/messages?key=SECRETK&token=SECRETT",
        )
        .unwrap();
        let r = redact_url(&u);
        assert_eq!(
            r,
            "https://chat.googleapis.com/v1/spaces/AAA/messages?key=***&token=***"
        );
        assert!(!r.contains("SECRET"));
        assert!(!r.contains("pw"));
        // No query: unchanged apart from userinfo.
        let plain_url = validate_webhook_url("http://127.0.0.1:1/hook").unwrap();
        assert_eq!(redact_url(&plain_url), "http://127.0.0.1:1/hook");
    }

    // ---- Throttle ---------------------------------------------------------

    #[test]
    fn throttle_admits_first_suppresses_inside_window_and_reports_count() {
        let mut t = Throttle::default();
        let w = Duration::from_secs(60);
        let t0 = Instant::now();
        assert_eq!(t.admit("a", w, t0), Admit::Send { suppressed: 0 });
        assert_eq!(
            t.admit("a", w, t0 + Duration::from_secs(1)),
            Admit::Suppress
        );
        assert_eq!(
            t.admit("a", w, t0 + Duration::from_secs(59)),
            Admit::Suppress
        );
        // Different key is independent.
        assert_eq!(
            t.admit("b", w, t0 + Duration::from_secs(2)),
            Admit::Send { suppressed: 0 }
        );
        // Window over: admitted, carrying the two suppressed.
        assert_eq!(
            t.admit("a", w, t0 + Duration::from_secs(60)),
            Admit::Send { suppressed: 2 }
        );
        // Counter reset after being reported.
        assert_eq!(
            t.admit("a", w, t0 + Duration::from_secs(121)),
            Admit::Send { suppressed: 0 }
        );
    }

    #[test]
    fn throttle_prune_forgets_idle_keys() {
        let mut t = Throttle::default();
        let t0 = Instant::now();
        t.admit("old", Duration::from_secs(1), t0);
        t.admit("new", Duration::from_secs(1), t0 + Duration::from_secs(100));
        t.prune(t0 + Duration::from_secs(101), Duration::from_secs(50));
        assert!(!t.entries.contains_key("old"));
        assert!(t.entries.contains_key("new"));
    }

    // ---- Formatting -------------------------------------------------------

    #[test]
    fn utc_formatting_matches_known_instants() {
        assert_eq!(format_utc(0), "1970-01-01 00:00:00 UTC");
        assert_eq!(format_utc(951_782_400), "2000-02-29 00:00:00 UTC"); // leap day
        assert_eq!(format_utc(1_700_000_000), "2023-11-14 22:13:20 UTC");
        assert_eq!(format_utc(4_102_444_799), "2099-12-31 23:59:59 UTC");
    }

    #[test]
    fn durations_are_human() {
        assert_eq!(fmt_duration(Duration::from_secs(5)), "5s");
        assert_eq!(fmt_duration(Duration::from_secs(312)), "5m 12s");
        assert_eq!(fmt_duration(Duration::from_secs(7_380)), "2h 03m");
        assert_eq!(fmt_duration(Duration::from_secs(273_600)), "3d 04h");
    }

    #[test]
    fn operator_strings_are_neutralised_and_bounded() {
        assert_eq!(mono("a`b"), "`a'b`");
        assert_eq!(
            plain("line1\nline2\t\ttab  spaced"),
            "line1 line2 tab spaced"
        );
        let long = "x".repeat(1000);
        assert_eq!(plain(&long).chars().count(), MAX_DETAIL_CHARS);
        assert!(plain(&long).ends_with('…'));
    }

    fn report(label: &str, ok: bool) -> ProbeReport {
        ProbeReport {
            label: label.to_string(),
            target: "db:5432".to_string(),
            protocol: Protocol::Tcp,
            outcome: if ok {
                Ok((
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 5432),
                    Duration::from_millis(12),
                ))
            } else {
                Err("connect timeout to 10.0.0.9:5432".to_string())
            },
        }
    }

    #[test]
    fn started_message_lists_every_forwarder_and_flags_failures() {
        let ok = Event::Started {
            forwarders: vec![report("a:1->db:5432", true)],
        };
        assert_eq!(ok.severity(), Severity::Info);
        let text = ok.render(&instance(), 0);
        assert!(text.contains("*rfw started*"), "{text}");
        assert!(text.contains("`test-host`"));
        assert!(text.contains("✅ reachable at `10.0.0.5:5432` (12 ms)"));
        assert!(text.contains("pid 42 · rfw 9.9.9 ·"));

        let mixed = Event::Started {
            forwarders: vec![report("a:1->db:5432", true), report("b:2->db:5432", false)],
        };
        assert_eq!(mixed.severity(), Severity::Warning);
        let text = mixed.render(&instance(), 0);
        assert!(text.contains("1 target(s) unreachable"), "{text}");
        assert!(text.contains("❌ unreachable: connect timeout to 10.0.0.9:5432"));
    }

    #[test]
    fn started_reports_udp_targets_as_resolved() {
        let r = ProbeReport {
            label: "a:1->b:53/udp".to_string(),
            target: "b:53".to_string(),
            protocol: Protocol::Udp,
            outcome: Ok((
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 53),
                Duration::from_millis(3),
            )),
        };
        let text = Event::Started {
            forwarders: vec![r],
        }
        .render(&instance(), 0);
        assert!(
            text.contains(
                "• `a:1->b:53/udp` ✅ resolved to `10.0.0.5:53` (3 ms, udp: passive check)"
            ),
            "{text}"
        );
        assert!(!text.contains("reachable at"), "{text}");
    }

    #[test]
    fn every_event_renders_with_its_key_facts() {
        let inst = instance();
        let cases: Vec<(Event, &[&str])> = vec![
            (
                Event::Stopping {
                    signal: "SIGTERM".into(),
                    active_connections: 7,
                },
                &["*rfw stopping*", "SIGTERM", "7 active"],
            ),
            (
                Event::DnsChanged {
                    label: "l".into(),
                    host: "db.example".into(),
                    old: vec!["10.0.0.1".parse().unwrap()],
                    new: vec!["10.0.0.2".parse().unwrap(), "10.0.0.3".parse().unwrap()],
                },
                &[
                    "*DNS change*",
                    "`db.example`",
                    "`10.0.0.1` → `10.0.0.2, 10.0.0.3`",
                ],
            ),
            (
                Event::TargetUnreachable {
                    label: "l".into(),
                    target: "db:5432".into(),
                    down_for: Duration::from_secs(312),
                    last_error: "connect timeout".into(),
                },
                &[
                    "🔴 *Target unreachable* `db:5432`",
                    "5m 12s",
                    "connect timeout",
                ],
            ),
            (
                Event::TargetRecovered {
                    label: "l".into(),
                    target: "db:5432".into(),
                    down_for: Duration::from_secs(400),
                    address: Some("10.0.0.2:5432".parse().unwrap()),
                },
                &["✅ *Target reachable again*", "6m 40s", "`10.0.0.2:5432`"],
            ),
            (
                Event::BindFailed {
                    label: "l".into(),
                    local_addr: "0.0.0.0:5432".into(),
                    error: "Address already in use".into(),
                    attempts: 3,
                },
                &[
                    "🔴 *Cannot bind* `0.0.0.0:5432`",
                    "Address already in use",
                    "Attempt 3",
                ],
            ),
            (
                Event::BindRecovered {
                    label: "l".into(),
                    local_addr: "0.0.0.0:5432".into(),
                    failed_attempts: 12,
                },
                &["✅ *Listening again*", "12 failed attempt"],
            ),
            (
                Event::ConfigReloaded {
                    active: 3,
                    added: vec!["x".into()],
                    removed: vec!["y".into(), "z".into()],
                },
                &[
                    "*Config reloaded*",
                    "3 forwarder(s)",
                    "Added: `x`",
                    "Removed: `y`, `z`",
                ],
            ),
            (
                Event::ConfigReloaded {
                    active: 1,
                    added: vec![],
                    removed: vec![],
                },
                &["settings only"],
            ),
            (
                Event::ConfigReloadFailed {
                    path: "/etc/rfw/forwarders.yml".into(),
                    error: "mapping values are not allowed".into(),
                },
                &[
                    "🟠 *Config reload failed*",
                    "`/etc/rfw/forwarders.yml`",
                    "mapping values",
                ],
            ),
            (
                Event::ConnectionLimitReached {
                    label: "l".into(),
                    rejected_total: 99,
                },
                &["*Connection limit reached*", "99 refused"],
            ),
            (
                Event::UdpSessionLimitReached {
                    label: "l".into(),
                    rejected_total: 7,
                },
                &["*UDP session limit reached*", "7 refused"],
            ),
            (
                Event::Internal {
                    component: "metrics",
                    message: "failed to bind 127.0.0.1:9090".into(),
                },
                &[
                    "*Internal problem* in `metrics`",
                    "failed to bind 127.0.0.1:9090",
                ],
            ),
        ];
        for (ev, needles) in cases {
            let text = ev.render(&inst, 0);
            for n in needles {
                assert!(text.contains(n), "{} missing {n:?} in:\n{text}", ev.kind());
            }
            assert!(
                text.contains("test-host · pid 42"),
                "footer missing: {text}"
            );
            assert!(text.chars().count() <= MAX_TEXT_CHARS);
        }
    }

    #[test]
    fn udp_session_limit_alert_is_rendered_and_throttled() {
        let ev = Event::UdpSessionLimitReached {
            label: "a:1->b:2/udp".into(),
            rejected_total: 7,
        };
        assert_eq!(ev.kind(), "udp_session_limit_reached");
        assert_eq!(ev.severity(), Severity::Warning);
        assert_eq!(ev.throttle().unwrap().0, "limit:a:1->b:2/udp");
        let text = ev.render(&instance(), 0);
        assert!(text.contains("*UDP session limit reached*"), "{text}");
        assert!(text.contains("(7 refused so far)"), "{text}");
    }

    #[test]
    fn suppressed_count_is_mentioned_and_text_is_capped() {
        let ev = Event::Internal {
            component: "x",
            message: "m".repeat(10_000),
        };
        let text = ev.render(&instance(), 5);
        assert!(text.contains("+5 similar event(s) suppressed"), "{text}");
        assert!(text.chars().count() <= MAX_TEXT_CHARS);
        assert!(!text.contains(&"m".repeat(MAX_DETAIL_CHARS + 1)));
    }

    #[test]
    fn throttle_policy_covers_the_repetitive_kinds_only() {
        let repetitive = [
            Event::DnsChanged {
                label: "l".into(),
                host: "h".into(),
                old: vec![],
                new: vec![],
            },
            Event::BindFailed {
                label: "l".into(),
                local_addr: "a".into(),
                error: "e".into(),
                attempts: 1,
            },
            Event::ConnectionLimitReached {
                label: "l".into(),
                rejected_total: 1,
            },
            Event::UdpSessionLimitReached {
                label: "l".into(),
                rejected_total: 1,
            },
            Event::ConfigReloadFailed {
                path: "p".into(),
                error: "e".into(),
            },
            Event::Internal {
                component: "c",
                message: "m".into(),
            },
        ];
        for ev in &repetitive {
            assert!(ev.throttle().is_some(), "{} must be throttled", ev.kind());
        }
        let one_shot = [
            Event::Started { forwarders: vec![] },
            Event::Stopping {
                signal: "s".into(),
                active_connections: 0,
            },
            Event::TargetUnreachable {
                label: "l".into(),
                target: "t".into(),
                down_for: Duration::ZERO,
                last_error: "e".into(),
            },
            Event::TargetRecovered {
                label: "l".into(),
                target: "t".into(),
                down_for: Duration::ZERO,
                address: None,
            },
            Event::BindRecovered {
                label: "l".into(),
                local_addr: "a".into(),
                failed_attempts: 1,
            },
            Event::ConfigReloaded {
                active: 0,
                added: vec![],
                removed: vec![],
            },
        ];
        for ev in &one_shot {
            assert!(
                ev.throttle().is_none(),
                "{} must not be throttled",
                ev.kind()
            );
        }
        // Throttle keys are per forwarder, so one flapping label cannot mute another.
        let a = Event::BindFailed {
            label: "a".into(),
            local_addr: "x".into(),
            error: "e".into(),
            attempts: 1,
        };
        let b = Event::BindFailed {
            label: "b".into(),
            local_addr: "x".into(),
            error: "e".into(),
            attempts: 1,
        };
        assert_ne!(a.throttle().unwrap().0, b.throttle().unwrap().0);
    }

    #[test]
    fn disabled_notifier_is_a_silent_no_op() {
        let n = Notifier::disabled();
        assert!(!n.is_enabled());
        n.notify(Event::Started { forwarders: vec![] });
        let snap = n.counters().snapshot();
        assert!(!snap.enabled);
        assert_eq!(
            snap,
            NotifierSnapshot {
                enabled: false,
                ..snap
            }
        );
        assert_eq!(snap.dropped, 0);
    }

    #[test]
    fn instance_name_falls_back_to_hostname() {
        let i = InstanceInfo::new(Some("  ".into()));
        assert!(!i.name.is_empty());
        assert_eq!(InstanceInfo::new(Some("edge-1".into())).name, "edge-1");
        assert_eq!(i.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn retry_after_is_parsed_and_capped() {
        let mut h = reqwest::header::HeaderMap::new();
        assert_eq!(parse_retry_after(&h), None);
        h.insert(reqwest::header::RETRY_AFTER, "7".parse().unwrap());
        assert_eq!(parse_retry_after(&h), Some(Duration::from_secs(7)));
        h.insert(reqwest::header::RETRY_AFTER, "9999".parse().unwrap());
        assert_eq!(parse_retry_after(&h), Some(MAX_RETRY_AFTER));
        h.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(parse_retry_after(&h), None);
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(3), Duration::from_secs(4));
    }

    // ---- Delivery against a local HTTP sink --------------------------------

    /// Minimal HTTP/1.1 sink: answers each request with the next status in
    /// `statuses` (the last one repeats) and hands the request body to the
    /// returned channel.
    async fn spawn_sink(statuses: Vec<u16>) -> (String, mpsc::UnboundedReceiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut i = 0_usize;
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let status = statuses[i.min(statuses.len() - 1)];
                i += 1;
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    let (head_end, content_len) = loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                            let len = head
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    k.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().ok())
                                        .flatten()
                                })
                                .unwrap_or(0);
                            assert!(head.starts_with("POST "), "must POST: {head}");
                            assert!(
                                head.to_ascii_lowercase()
                                    .contains("content-type: application/json"),
                                "{head}"
                            );
                            break (pos + 4, len);
                        }
                    };
                    while buf.len() < head_end + content_len {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let body = String::from_utf8_lossy(&buf[head_end..head_end + content_len]);
                    let _ = tx.send(body.to_string());
                    let reason = match status {
                        200 => "OK",
                        400 => "Bad Request",
                        429 => "Too Many Requests",
                        500 => "Internal Server Error",
                        _ => "Whatever",
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (format!("http://{addr}/hook"), rx)
    }

    fn started_with(url: &str) -> (Notifier, NotifierHandle) {
        start(WebhookConfig {
            url: validate_webhook_url(url).unwrap(),
            instance: instance(),
        })
        .expect("start notifier")
    }

    async fn wait_counter(n: &Notifier, pick: impl Fn(NotifierSnapshot) -> u64, want: u64) {
        for _ in 0..400 {
            if pick(n.counters().snapshot()) >= want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "counter never reached {want}: {:?}",
            n.counters().snapshot()
        );
    }

    #[tokio::test]
    async fn delivers_google_chat_text_payload() {
        let (url, mut bodies) = spawn_sink(vec![200]).await;
        let (n, handle) = started_with(&url);
        assert!(n.is_enabled());
        n.notify(Event::Stopping {
            signal: "SIGTERM".into(),
            active_connections: 3,
        });

        let body = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .expect("webhook must be called")
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&body).expect("JSON body");
        let text = json["text"].as_str().expect("text field");
        assert!(text.contains("*rfw stopping*"), "{text}");
        assert!(text.contains("SIGTERM"));

        wait_counter(&n, |s| s.sent, 1).await;
        assert_eq!(n.counters().snapshot().failed, 0);
        handle.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn retries_server_errors_then_succeeds() {
        let (url, mut bodies) = spawn_sink(vec![500, 200]).await;
        let (n, handle) = started_with(&url);
        n.notify(Event::Internal {
            component: "t",
            message: "boom".into(),
        });
        // Two requests: the failed first attempt and the retry.
        let first = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first, second, "retry must resend the same message");
        wait_counter(&n, |s| s.sent, 1).await;
        assert_eq!(n.counters().snapshot().failed, 0);
        handle.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn client_errors_are_not_retried_and_are_counted() {
        let (url, mut bodies) = spawn_sink(vec![400]).await;
        let (n, handle) = started_with(&url);
        n.notify(Event::Internal {
            component: "t",
            message: "bad".into(),
        });
        let _ = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .unwrap()
            .unwrap();
        wait_counter(&n, |s| s.failed, 1).await;
        // No second request must arrive.
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), bodies.recv())
                .await
                .is_err(),
            "a 400 must not be retried"
        );
        assert_eq!(n.counters().snapshot().sent, 0);
        handle.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn throttled_repeats_are_folded_into_one_message() {
        let (url, mut bodies) = spawn_sink(vec![200]).await;
        let (n, handle) = started_with(&url);
        for i in 0..5 {
            n.notify(Event::BindFailed {
                label: "l".into(),
                local_addr: "a".into(),
                error: format!("e{i}"),
                attempts: i,
            });
        }
        let first = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(first.contains("Cannot bind"));
        wait_counter(&n, |s| s.suppressed, 4).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), bodies.recv())
                .await
                .is_err(),
            "repeats inside the window must not be delivered"
        );
        assert_eq!(n.counters().snapshot().sent, 1);
        handle.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn unreachable_webhook_fails_closed_without_blocking_producers() {
        // Nothing listens here; every attempt is a transport error.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", dead.local_addr().unwrap());
        drop(dead);
        let (n, handle) = started_with(&url);
        let t0 = Instant::now();
        n.notify(Event::Internal {
            component: "t",
            message: "x".into(),
        });
        assert!(
            t0.elapsed() < Duration::from_millis(50),
            "notify must return immediately"
        );
        wait_counter(&n, |s| s.failed, 1).await;
        handle.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn shutdown_flushes_queued_events() {
        let (url, mut bodies) = spawn_sink(vec![200]).await;
        let (n, handle) = started_with(&url);
        n.notify(Event::Stopping {
            signal: "SIGINT".into(),
            active_connections: 0,
        });
        // Cancel right away: the event is still in the queue.
        handle.shutdown(Duration::from_secs(5)).await;
        let body = tokio::time::timeout(Duration::from_secs(1), bodies.recv())
            .await
            .expect("queued event must be flushed on shutdown")
            .unwrap();
        assert!(body.contains("rfw stopping"));
        assert_eq!(n.counters().snapshot().sent, 1);
    }

    #[tokio::test]
    async fn full_queue_drops_and_counts() {
        let (tx, _rx) = mpsc::channel(1);
        let n = Notifier {
            tx: Some(tx),
            counters: Arc::new(NotifierCounters::new(true)),
        };
        n.notify(Event::Started { forwarders: vec![] });
        n.notify(Event::Started { forwarders: vec![] });
        assert_eq!(n.counters().snapshot().dropped, 1);
    }
}
