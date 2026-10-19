use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Transport of a forwarder. TCP is the default everywhere (CLI, env, YAML).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    #[serde(alias = "TCP")]
    Tcp,
    #[serde(alias = "UDP")]
    Udp,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        }
    }
}

/// One forwarder: a single local port to a single remote port. Port ranges are
/// expanded into one of these per port when the configuration is loaded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwarderConfig {
    pub local_host: String,
    pub local_port: u16,
    pub remote_host: String,
    pub remote_port: u16,
    /// `tcp` (default) or `udp`
    #[serde(default)]
    pub protocol: Protocol,
}

impl ForwarderConfig {
    pub fn label(&self) -> String {
        let base = format!(
            "{}:{}->{}:{}",
            self.local_host, self.local_port, self.remote_host, self.remote_port
        );
        match self.protocol {
            Protocol::Tcp => base,
            Protocol::Udp => format!("{base}/{}", self.protocol.as_str()),
        }
    }

    pub fn local_addr(&self) -> String {
        format!("{}:{}", self.local_host, self.local_port)
    }
}

/// Most ports one range entry may expand to. Every port becomes a listener
/// with its own monitor and target probe, so an unbounded range (`1-65535`,
/// usually a typo) would exhaust descriptors before it forwarded anything.
pub const MAX_RANGE_PORTS: usize = 4096;

/// An inclusive span of ports; a single port is `start == end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PortSpan {
    start: u16,
    end: u16,
}

impl PortSpan {
    fn single(port: u16) -> Self {
        Self {
            start: port,
            end: port,
        }
    }

    /// A validated range. Port 0 means "any free port" for a single listener,
    /// which has no sensible reading for a span, so it is rejected here.
    fn range(start: u16, end: u16) -> Result<Self, String> {
        if start == 0 {
            return Err("port 0 is not allowed in a range".to_string());
        }
        if start > end {
            return Err(format!("start {start} is greater than end {end}"));
        }
        let span = Self { start, end };
        if span.len() > MAX_RANGE_PORTS {
            return Err(format!(
                "{} ports exceed the limit of {MAX_RANGE_PORTS} per range",
                span.len()
            ));
        }
        Ok(span)
    }

    fn len(self) -> usize {
        usize::from(self.end - self.start) + 1
    }

    /// Parse `80` or `30000-30050`. `what` is "local" or "remote", for errors.
    fn parse(raw: &str, what: &str) -> Result<Self, String> {
        let Some((first, last)) = raw.split_once('-') else {
            return raw
                .parse()
                .map(Self::single)
                .map_err(|e| format!("Invalid {what} port '{raw}': {e}"));
        };
        let bound = |s: &str| -> Result<u16, String> {
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return Err(format!(
                    "Invalid {what} port range '{raw}': '{s}' is not a port number"
                ));
            }
            s.parse()
                .map_err(|e| format!("Invalid {what} port range '{raw}': {e}"))
        };
        Self::range(bound(first)?, bound(last)?)
            .map_err(|e| format!("Invalid {what} port range '{raw}': {e}"))
    }
}

impl std::fmt::Display for PortSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

/// One configured entry before range expansion. Both sides always have the
/// same number of ports (checked in [`ForwarderSpec::new`]), so local port
/// `local.start + k` maps to remote port `remote.start + k`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ForwarderSpec {
    local_host: String,
    local: PortSpan,
    remote_host: String,
    remote: PortSpan,
    protocol: Protocol,
}

impl ForwarderSpec {
    fn new(
        local_host: String,
        local: PortSpan,
        remote_host: String,
        remote: PortSpan,
        protocol: Protocol,
    ) -> Result<Self, String> {
        let spec = Self {
            local_host,
            local,
            remote_host,
            remote,
            protocol,
        };
        if local.len() != remote.len() {
            return Err(format!(
                "Port range length mismatch in '{}': local {local} has {} port(s) but remote {remote} has {}. A range maps port for port, so both sides need the same number of ports.",
                spec.describe(),
                local.len(),
                remote.len()
            ));
        }
        Ok(spec)
    }

    /// Same shape as [`ForwarderConfig::label`], with the ports as written.
    fn describe(&self) -> String {
        let base = format!(
            "{}:{}->{}:{}",
            self.local_host, self.local, self.remote_host, self.remote
        );
        match self.protocol {
            Protocol::Tcp => base,
            Protocol::Udp => format!("{base}/{}", self.protocol.as_str()),
        }
    }

    fn expand(&self) -> Vec<ForwarderConfig> {
        (0..self.local.len() as u16)
            .map(|offset| ForwarderConfig {
                local_host: self.local_host.clone(),
                local_port: self.local.start + offset,
                remote_host: self.remote_host.clone(),
                remote_port: self.remote.start + offset,
                protocol: self.protocol,
            })
            .collect()
    }
}

/// Expand every entry into single-port forwarders, rejecting any local
/// endpoint claimed twice. Done on the entries (not the flat list) so the
/// error can name both entries that collide, e.g. a range and a single port
/// inside it.
fn expand_specs(specs: &[ForwarderSpec]) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
    let total: usize = specs.iter().map(|s| s.local.len()).sum();
    let mut owner: std::collections::HashMap<(Protocol, &str, u16), usize> =
        std::collections::HashMap::with_capacity(total);
    let mut out = Vec::with_capacity(total);
    for (index, spec) in specs.iter().enumerate() {
        for port in spec.local.start..=spec.local.end {
            let key = (spec.protocol, spec.local_host.as_str(), port);
            if let Some(&first) = owner.get(&key) {
                let addr = match spec.protocol {
                    Protocol::Tcp => format!("{}:{port}", spec.local_host),
                    Protocol::Udp => format!("{}:{port}/udp", spec.local_host),
                };
                let (a, b) = (specs[first].describe(), spec.describe());
                let who = if a == b {
                    format!("'{a}' is listed twice")
                } else {
                    format!("defined by both '{a}' and '{b}'")
                };
                anyhow::bail!(
                    "Duplicate local address '{addr}': {who}. Each forwarder needs a unique local endpoint."
                );
            }
            owner.insert(key, index);
        }
        out.extend(spec.expand());
    }
    Ok(out)
}

/// A YAML forwarder entry. `local_port_end` turns it into a range: ports
/// `local_port..=local_port_end` map to `remote_port..` at the same offsets.
/// `remote_port_end` is optional; when present it must span as many ports.
#[derive(Debug, Clone, Deserialize)]
struct RawForwarder {
    local_host: String,
    local_port: u16,
    #[serde(default)]
    local_port_end: Option<u16>,
    remote_host: String,
    remote_port: u16,
    #[serde(default)]
    remote_port_end: Option<u16>,
    #[serde(default)]
    protocol: Protocol,
}

impl RawForwarder {
    fn into_spec(self) -> Result<ForwarderSpec, String> {
        let shown = format!("{}:{}", self.local_host, self.local_port);
        let local = match self.local_port_end {
            None => PortSpan::single(self.local_port),
            Some(end) => PortSpan::range(self.local_port, end).map_err(|e| {
                format!(
                    "Invalid local port range {}-{end} for '{shown}': {e}",
                    self.local_port
                )
            })?,
        };
        let remote = match (self.local_port_end, self.remote_port_end) {
            (None, None) => PortSpan::single(self.remote_port),
            (None, Some(end)) => {
                return Err(format!(
                    "Invalid forwarder '{shown}': remote_port_end {end} needs local_port_end (a range has to span the same ports on both sides)"
                ));
            }
            (Some(_), explicit) => {
                let end = match explicit {
                    Some(end) => end,
                    None => {
                        let last = u32::from(self.remote_port) + local.len() as u32 - 1;
                        u16::try_from(last).map_err(|_| {
                            format!(
                                "Invalid forwarder '{shown}': the {} local ports starting at remote_port {} would run past 65535",
                                local.len(),
                                self.remote_port
                            )
                        })?
                    }
                };
                PortSpan::range(self.remote_port, end).map_err(|e| {
                    format!(
                        "Invalid remote port range {}-{end} for '{shown}': {e}",
                        self.remote_port
                    )
                })?
            }
        };
        ForwarderSpec::new(
            self.local_host,
            local,
            self.remote_host,
            remote,
            self.protocol,
        )
    }
}

/// Deserialize the `forwarders:` list, expanding ranges, so everything past
/// the config layer (manager, hot-reload diff, stats) only ever sees one
/// forwarder per port.
fn deserialize_forwarders<'de, D>(deserializer: D) -> Result<Vec<ForwarderConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let raw = Vec::<RawForwarder>::deserialize(deserializer)?;
    let specs = raw
        .into_iter()
        .map(RawForwarder::into_spec)
        .collect::<Result<Vec<_>, _>>()
        .map_err(D::Error::custom)?;
    expand_specs(&specs).map_err(D::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigFile {
    #[serde(deserialize_with = "deserialize_forwarders")]
    forwarders: Vec<ForwarderConfig>,
    #[serde(default)]
    settings: Settings,
}

fn default_report_interval() -> u64 {
    60
}
fn default_sample_interval() -> u64 {
    1
}
fn default_buffer_bytes() -> usize {
    65536
}
fn default_use_splice() -> bool {
    false
}
fn default_connect_timeout() -> u64 {
    10
}
fn default_dns_timeout() -> u64 {
    5
}
fn default_keepalive_idle() -> u64 {
    60
}
fn default_keepalive_interval() -> u64 {
    10
}
fn default_keepalive_retries() -> u32 {
    3
}
fn default_probe_interval() -> u64 {
    30
}
fn default_unreachable_after() -> u64 {
    300
}
fn default_udp_session_timeout() -> u64 {
    60
}
fn default_udp_max_sessions() -> usize {
    16384
}

/// Smallest accepted copy buffer. A zero-sized buffer makes
/// `copy_bidirectional` spin at 100% CPU while moving no bytes, so it is
/// rejected rather than clamped silently.
pub const MIN_BUFFER_BYTES: usize = 1024;
/// Largest accepted copy buffer. Each connection allocates two of these on the
/// portable path, so an unbounded value is an easy way to OOM the process.
pub const MAX_BUFFER_BYTES: usize = 16 * 1024 * 1024;
/// Largest accepted `udp_max_sessions`. Each UDP session holds one socket (one
/// file descriptor), so an unbounded value is an easy way to exhaust them.
pub const MAX_UDP_SESSIONS: usize = 1_048_576;

/// Process-global runtime settings (one HTTP endpoint, one sampler, one reporter).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    /// `host:port` for the HTTP stats endpoint. `None` disables it (default).
    #[serde(default)]
    pub metrics_addr: Option<String>,
    /// Periodic log report cadence, seconds.
    #[serde(default = "default_report_interval")]
    pub report_interval_secs: u64,
    /// Throughput-rate sampling cadence, seconds.
    #[serde(default = "default_sample_interval")]
    pub sample_interval_secs: u64,
    /// Per-direction copy buffer size, bytes.
    #[serde(default = "default_buffer_bytes")]
    pub buffer_bytes: usize,
    /// Kernel socket buffer size (`SO_SNDBUF`/`SO_RCVBUF`) per socket, bytes.
    /// `None` leaves the OS default. Raising it helps on high bandwidth-delay
    /// (WAN / high-latency) links.
    #[serde(default)]
    pub socket_buffer_bytes: Option<usize>,
    /// Use the Linux `splice(2)` zero-copy data path (opt-in, Linux only).
    /// Ignored with a warning on non-Linux targets.
    #[serde(default = "default_use_splice")]
    pub use_splice: bool,
    /// Upper bound on concurrently proxied connections across all forwarders.
    /// `None` means unbounded. Each in-flight connection costs roughly
    /// `2 * buffer_bytes` of heap on the portable path, so a bound keeps the
    /// process inside a container memory limit instead of being OOM-killed.
    #[serde(default)]
    pub max_connections: Option<usize>,
    /// Per-address TCP connect timeout, seconds. With several resolved
    /// addresses this bounds how long a dead record can stall a connection.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    /// DNS resolution timeout, seconds.
    #[serde(default = "default_dns_timeout")]
    pub dns_timeout_secs: u64,
    /// TCP keepalive idle time before the first probe, seconds, applied to
    /// both sockets of every proxied connection. `0` disables keepalive.
    ///
    /// Without keepalive a peer that vanishes without closing (client crash,
    /// VPN/NAT drop, remote failover) leaves a half-open connection that
    /// never ends: it holds a `max_connections` slot, a backend session and
    /// two copy buffers until the other side happens to write.
    #[serde(default = "default_keepalive_idle")]
    pub keepalive_idle_secs: u64,
    /// Seconds between keepalive probes once the idle time has elapsed.
    #[serde(default = "default_keepalive_interval")]
    pub keepalive_interval_secs: u64,
    /// Unanswered probes before the connection is declared dead.
    #[serde(default = "default_keepalive_retries")]
    pub keepalive_retries: u32,
    /// Google Chat incoming-webhook URL for operational alerts. `None`
    /// disables alerting (default). Prefer the `RFW_WEBHOOK_URL` environment
    /// variable in production: the URL carries the space's credentials.
    #[serde(default)]
    pub webhook_url: Option<String>,
    /// Name identifying this instance in alerts. Defaults to the hostname.
    #[serde(default)]
    pub instance_name: Option<String>,
    /// Seconds between active reachability probes of each target (one TCP
    /// connect, closed immediately). Skipped while real connections keep
    /// succeeding.
    #[serde(default = "default_probe_interval")]
    pub probe_interval_secs: u64,
    /// Seconds a target must fail probes continuously before the
    /// "unreachable" alert is raised (default 300 = 5 minutes).
    #[serde(default = "default_unreachable_after")]
    pub unreachable_after_secs: u64,
    /// Seconds a UDP session (one client address) may stay idle, no datagram
    /// in either direction, before its upstream socket is closed. Live on reload.
    #[serde(default = "default_udp_session_timeout")]
    pub udp_session_timeout_secs: u64,
    /// Upper bound on concurrent UDP sessions across all UDP forwarders.
    /// Each session holds one socket (one file descriptor). Startup-only.
    #[serde(default = "default_udp_max_sessions")]
    pub udp_max_sessions: usize,
    /// Linux only: receive coalesced UDP bursts (UDP_GRO) and forward them with
    /// segmented sends (UDP_SEGMENT). Hot-reloadable.
    #[serde(default)]
    pub udp_gro_gso: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            metrics_addr: None,
            report_interval_secs: default_report_interval(),
            sample_interval_secs: default_sample_interval(),
            buffer_bytes: default_buffer_bytes(),
            socket_buffer_bytes: None,
            use_splice: default_use_splice(),
            max_connections: None,
            connect_timeout_secs: default_connect_timeout(),
            dns_timeout_secs: default_dns_timeout(),
            keepalive_idle_secs: default_keepalive_idle(),
            keepalive_interval_secs: default_keepalive_interval(),
            keepalive_retries: default_keepalive_retries(),
            webhook_url: None,
            instance_name: None,
            probe_interval_secs: default_probe_interval(),
            unreachable_after_secs: default_unreachable_after(),
            udp_session_timeout_secs: default_udp_session_timeout(),
            udp_max_sessions: default_udp_max_sessions(),
            udp_gro_gso: false,
        }
    }
}

impl Settings {
    /// Reject values that would panic a task or wedge the data plane.
    ///
    /// Used at startup, where a bad value should stop the process with a clear
    /// message instead of silently killing the stats tasks (a zero interval
    /// panics `tokio::time::interval`) or spinning the CPU (a zero copy buffer
    /// makes `copy_bidirectional` treat every read as EOF).
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if self.report_interval_secs == 0 {
            anyhow::bail!("report_interval_secs must be >= 1 (0 would panic the stats reporter)");
        }
        if self.sample_interval_secs == 0 {
            anyhow::bail!("sample_interval_secs must be >= 1 (0 would panic the rate sampler)");
        }
        if self.connect_timeout_secs == 0 {
            anyhow::bail!("connect_timeout_secs must be >= 1");
        }
        if self.dns_timeout_secs == 0 {
            anyhow::bail!("dns_timeout_secs must be >= 1");
        }
        if !(MIN_BUFFER_BYTES..=MAX_BUFFER_BYTES).contains(&self.buffer_bytes) {
            anyhow::bail!(
                "buffer_bytes must be between {MIN_BUFFER_BYTES} and {MAX_BUFFER_BYTES} (got {})",
                self.buffer_bytes
            );
        }
        if let Some(sz) = self.socket_buffer_bytes {
            if sz == 0 {
                anyhow::bail!("socket_buffer_bytes must be >= 1, or omitted for the OS default");
            }
        }
        if let Some(max) = self.max_connections {
            if max == 0 {
                anyhow::bail!("max_connections must be >= 1, or omitted for unbounded");
            }
        }
        if let Some(addr) = &self.metrics_addr {
            if addr.parse::<std::net::SocketAddr>().is_err() {
                anyhow::bail!("metrics_addr '{addr}' is not a valid host:port socket address");
            }
        }
        if self.keepalive_idle_secs > 0 {
            if self.keepalive_interval_secs == 0 {
                anyhow::bail!(
                    "keepalive_interval_secs must be >= 1 when keepalive is enabled (keepalive_idle_secs > 0)"
                );
            }
            if self.keepalive_retries == 0 {
                anyhow::bail!(
                    "keepalive_retries must be >= 1 when keepalive is enabled (keepalive_idle_secs > 0)"
                );
            }
        }
        if self.probe_interval_secs == 0 {
            anyhow::bail!("probe_interval_secs must be >= 1");
        }
        if self.unreachable_after_secs == 0 {
            anyhow::bail!("unreachable_after_secs must be >= 1");
        }
        if self.udp_session_timeout_secs == 0 {
            anyhow::bail!("udp_session_timeout_secs must be >= 1");
        }
        if !(1..=MAX_UDP_SESSIONS).contains(&self.udp_max_sessions) {
            anyhow::bail!(
                "udp_max_sessions must be between 1 and {MAX_UDP_SESSIONS} (got {})",
                self.udp_max_sessions
            );
        }
        if let Some(url) = &self.webhook_url {
            if let Err(e) = crate::notify::validate_webhook_url(url) {
                // Never echo the URL back: it carries the webhook credentials.
                anyhow::bail!("webhook_url is invalid: {e}");
            }
        }
        Ok(())
    }

    /// Clamp out-of-range values in place, logging each correction.
    ///
    /// Used on hot-reload, where the process is already serving traffic and
    /// aborting is not an option: a bad edit degrades to the nearest safe value
    /// instead of panicking a task or wedging the copy loop.
    pub fn clamp_in_place(&mut self) {
        fn floor_u64(field: &str, value: &mut u64, min: u64) {
            if *value < min {
                warn!(
                    field,
                    got = *value,
                    clamped_to = min,
                    "Invalid setting clamped"
                );
                *value = min;
            }
        }

        floor_u64("report_interval_secs", &mut self.report_interval_secs, 1);
        floor_u64("sample_interval_secs", &mut self.sample_interval_secs, 1);
        floor_u64("connect_timeout_secs", &mut self.connect_timeout_secs, 1);
        floor_u64("dns_timeout_secs", &mut self.dns_timeout_secs, 1);
        floor_u64("probe_interval_secs", &mut self.probe_interval_secs, 1);
        floor_u64(
            "unreachable_after_secs",
            &mut self.unreachable_after_secs,
            1,
        );
        floor_u64(
            "udp_session_timeout_secs",
            &mut self.udp_session_timeout_secs,
            1,
        );

        let clamped_buffer = self.buffer_bytes.clamp(MIN_BUFFER_BYTES, MAX_BUFFER_BYTES);
        if clamped_buffer != self.buffer_bytes {
            warn!(
                field = "buffer_bytes",
                got = self.buffer_bytes,
                clamped_to = clamped_buffer,
                "Invalid setting clamped"
            );
            self.buffer_bytes = clamped_buffer;
        }

        let clamped_sessions = self.udp_max_sessions.clamp(1, MAX_UDP_SESSIONS);
        if clamped_sessions != self.udp_max_sessions {
            warn!(
                field = "udp_max_sessions",
                got = self.udp_max_sessions,
                clamped_to = clamped_sessions,
                "Invalid setting clamped"
            );
            self.udp_max_sessions = clamped_sessions;
        }

        if self.socket_buffer_bytes == Some(0) {
            warn!(
                field = "socket_buffer_bytes",
                "Invalid setting (0) ignored; using the OS default"
            );
            self.socket_buffer_bytes = None;
        }
        if self.max_connections == Some(0) {
            warn!(
                field = "max_connections",
                "Invalid setting (0) ignored; treating as unbounded"
            );
            self.max_connections = None;
        }
        if self.keepalive_idle_secs > 0 {
            floor_u64(
                "keepalive_interval_secs",
                &mut self.keepalive_interval_secs,
                1,
            );
            if self.keepalive_retries == 0 {
                warn!(
                    field = "keepalive_retries",
                    got = 0,
                    clamped_to = 1,
                    "Invalid setting clamped"
                );
                self.keepalive_retries = 1;
            }
        }
    }
}

/// TCP keepalive parameters applied to both sockets of a proxied connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeepaliveConfig {
    /// Idle time before the first probe.
    pub idle: std::time::Duration,
    /// Interval between probes.
    pub interval: std::time::Duration,
    /// Unanswered probes before the connection is declared dead.
    pub retries: u32,
}

/// Live, lock-free copy of the data-plane knobs shared by every connection task.
///
/// Held behind an `Arc`; each new connection reads the current values with a
/// `Relaxed` load. Hot-reload updates them via [`RuntimeKnobs::update`], so new
/// connections pick up changes to `buffer_bytes` / `socket_buffer_bytes` /
/// `use_splice` without a restart (in-flight connections keep their values).
#[derive(Debug)]
pub struct RuntimeKnobs {
    buffer_bytes: AtomicUsize,
    /// `0` means "leave the OS default".
    socket_buffer_bytes: AtomicUsize,
    use_splice: AtomicBool,
    connect_timeout_secs: AtomicU64,
    dns_timeout_secs: AtomicU64,
    /// `0` means keepalive disabled.
    keepalive_idle_secs: AtomicU64,
    keepalive_interval_secs: AtomicU64,
    keepalive_retries: AtomicU32,
    /// Target monitor cadence and alert threshold (read live by the monitor).
    probe_interval_secs: AtomicU64,
    unreachable_after_secs: AtomicU64,
    /// UDP session idle timeout (read live by the session sweep).
    udp_session_timeout_secs: AtomicU64,
    /// Linux UDP GRO/GSO fast path (read by listeners at each sweep, by sessions at creation).
    udp_gro_gso: AtomicBool,
}

impl RuntimeKnobs {
    pub fn from_settings(s: &Settings) -> Self {
        let knobs = Self {
            buffer_bytes: AtomicUsize::new(default_buffer_bytes()),
            socket_buffer_bytes: AtomicUsize::new(0),
            use_splice: AtomicBool::new(false),
            connect_timeout_secs: AtomicU64::new(default_connect_timeout()),
            dns_timeout_secs: AtomicU64::new(default_dns_timeout()),
            keepalive_idle_secs: AtomicU64::new(default_keepalive_idle()),
            keepalive_interval_secs: AtomicU64::new(default_keepalive_interval()),
            keepalive_retries: AtomicU32::new(default_keepalive_retries()),
            probe_interval_secs: AtomicU64::new(default_probe_interval()),
            unreachable_after_secs: AtomicU64::new(default_unreachable_after()),
            udp_session_timeout_secs: AtomicU64::new(default_udp_session_timeout()),
            udp_gro_gso: AtomicBool::new(false),
        };
        knobs.update(s);
        knobs
    }

    /// Apply reloaded settings to the shared knobs (called on hot-reload).
    ///
    /// Values are clamped first, so a bad hot-reload edit cannot install a
    /// zero-sized copy buffer (which would spin the copy loop) into the live
    /// data plane.
    pub fn update(&self, s: &Settings) {
        let mut s = s.clone();
        s.clamp_in_place();
        self.buffer_bytes.store(s.buffer_bytes, Ordering::Relaxed);
        self.socket_buffer_bytes
            .store(s.socket_buffer_bytes.unwrap_or(0), Ordering::Relaxed);
        self.use_splice.store(s.use_splice, Ordering::Relaxed);
        self.connect_timeout_secs
            .store(s.connect_timeout_secs, Ordering::Relaxed);
        self.dns_timeout_secs
            .store(s.dns_timeout_secs, Ordering::Relaxed);
        self.keepalive_idle_secs
            .store(s.keepalive_idle_secs, Ordering::Relaxed);
        self.keepalive_interval_secs
            .store(s.keepalive_interval_secs, Ordering::Relaxed);
        self.keepalive_retries
            .store(s.keepalive_retries, Ordering::Relaxed);
        self.probe_interval_secs
            .store(s.probe_interval_secs, Ordering::Relaxed);
        self.unreachable_after_secs
            .store(s.unreachable_after_secs, Ordering::Relaxed);
        self.udp_session_timeout_secs
            .store(s.udp_session_timeout_secs, Ordering::Relaxed);
        self.udp_gro_gso.store(s.udp_gro_gso, Ordering::Relaxed);
    }

    /// Idle time after which a UDP session is closed (read live by every sweep).
    pub fn udp_session_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.udp_session_timeout_secs.load(Ordering::Relaxed).max(1))
    }

    /// Cadence of the target monitor's active probes.
    pub fn probe_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.probe_interval_secs.load(Ordering::Relaxed).max(1))
    }

    /// Continuous failure time before the "target unreachable" alert.
    pub fn unreachable_after(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.unreachable_after_secs.load(Ordering::Relaxed).max(1))
    }

    /// Returns `None` when keepalive is disabled (`keepalive_idle_secs == 0`).
    pub fn keepalive(&self) -> Option<KeepaliveConfig> {
        match self.keepalive_idle_secs.load(Ordering::Relaxed) {
            0 => None,
            idle => Some(KeepaliveConfig {
                idle: std::time::Duration::from_secs(idle),
                interval: std::time::Duration::from_secs(
                    self.keepalive_interval_secs.load(Ordering::Relaxed).max(1),
                ),
                retries: self.keepalive_retries.load(Ordering::Relaxed).max(1),
            }),
        }
    }

    pub fn buffer_bytes(&self) -> usize {
        self.buffer_bytes.load(Ordering::Relaxed)
    }

    /// Returns `None` when the OS default should be left in place.
    pub fn socket_buffer_bytes(&self) -> Option<usize> {
        match self.socket_buffer_bytes.load(Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    pub fn use_splice(&self) -> bool {
        self.use_splice.load(Ordering::Relaxed)
    }

    /// Linux UDP GRO/GSO fast path (applied by listeners at their next sweep, by sessions at creation).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn udp_gro_gso(&self) -> bool {
        self.udp_gro_gso.load(Ordering::Relaxed)
    }

    pub fn connect_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.connect_timeout_secs.load(Ordering::Relaxed).max(1))
    }

    pub fn dns_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.dns_timeout_secs.load(Ordering::Relaxed).max(1))
    }

    /// Snapshot every data-plane knob in one go, so a connection cannot be
    /// built from a mix of pre- and post-reload values.
    pub fn snapshot(&self) -> KnobSnapshot {
        KnobSnapshot {
            buffer_bytes: self.buffer_bytes(),
            socket_buffer_bytes: self.socket_buffer_bytes(),
            use_splice: self.use_splice(),
            connect_timeout: self.connect_timeout(),
            dns_timeout: self.dns_timeout(),
            keepalive: self.keepalive(),
        }
    }
}

/// Immutable set of data-plane knobs captured at accept time. In-flight
/// connections keep these values across a hot-reload.
#[derive(Debug, Clone, Copy)]
pub struct KnobSnapshot {
    pub buffer_bytes: usize,
    pub socket_buffer_bytes: Option<usize>,
    pub use_splice: bool,
    pub connect_timeout: std::time::Duration,
    pub dns_timeout: std::time::Duration,
    /// `None` = keepalive disabled.
    pub keepalive: Option<KeepaliveConfig>,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rfw",
    version,
    about = "Rust Forwarder — TCP/UDP port forwarder with hot-reload, auto-reconnect, and dynamic DNS"
)]
pub struct CliArgs {
    /// Path to YAML configuration file
    #[arg(short = 'f', long = "file")]
    pub config_file: Option<PathBuf>,

    /// Bind address (host:port) for the HTTP stats endpoint (/metrics, /stats)
    #[arg(long = "metrics-addr")]
    pub metrics_addr: Option<String>,

    /// Periodic traffic-report log cadence, seconds (default 60)
    #[arg(long = "report-interval")]
    pub report_interval_secs: Option<u64>,

    /// Throughput-rate sampling cadence, seconds (default 1)
    #[arg(long = "sample-interval")]
    pub sample_interval_secs: Option<u64>,

    /// Per-direction copy buffer size, bytes (default 65536)
    #[arg(long = "buffer-bytes")]
    pub buffer_bytes: Option<usize>,

    /// Kernel socket buffer size (SO_SNDBUF/SO_RCVBUF) per socket, bytes
    /// (default: OS default)
    #[arg(long = "socket-buffer-bytes")]
    pub socket_buffer_bytes: Option<usize>,

    /// Use the Linux splice(2) zero-copy data path (Linux only)
    #[arg(long = "splice")]
    pub use_splice: bool,

    /// Max concurrently proxied connections across all forwarders
    /// (default: unbounded)
    #[arg(long = "max-connections")]
    pub max_connections: Option<usize>,

    /// Per-address TCP connect timeout, seconds (default 10)
    #[arg(long = "connect-timeout")]
    pub connect_timeout_secs: Option<u64>,

    /// DNS resolution timeout, seconds (default 5)
    #[arg(long = "dns-timeout")]
    pub dns_timeout_secs: Option<u64>,

    /// TCP keepalive idle time before the first probe, seconds, on both sides
    /// of every proxied connection (default 60; 0 disables keepalive)
    #[arg(long = "keepalive-idle", value_name = "SECS")]
    pub keepalive_idle_secs: Option<u64>,

    /// Seconds between TCP keepalive probes (default 10)
    #[arg(long = "keepalive-interval", value_name = "SECS")]
    pub keepalive_interval_secs: Option<u64>,

    /// Unanswered keepalive probes before a connection is declared dead
    /// (default 3)
    #[arg(long = "keepalive-retries", value_name = "N")]
    pub keepalive_retries: Option<u32>,

    /// Google Chat incoming-webhook URL for operational alerts (startup,
    /// target unreachable, DNS change, bind failures, reload errors).
    /// The URL carries credentials: prefer the RFW_WEBHOOK_URL environment
    /// variable over a command-line argument visible in `ps`.
    #[arg(
        long = "webhook-url",
        value_name = "URL",
        env = "RFW_WEBHOOK_URL",
        hide_env_values = true
    )]
    pub webhook_url: Option<String>,

    /// Name identifying this instance in alerts (default: hostname)
    #[arg(long = "instance-name", value_name = "NAME", env = "RFW_INSTANCE_NAME")]
    pub instance_name: Option<String>,

    /// Seconds between active reachability probes of each target (default 30)
    #[arg(long = "probe-interval", value_name = "SECS")]
    pub probe_interval_secs: Option<u64>,

    /// Seconds a target must stay unreachable before the alert is sent
    /// (default 300 = 5 minutes)
    #[arg(long = "unreachable-after", value_name = "SECS")]
    pub unreachable_after_secs: Option<u64>,

    /// Seconds a UDP session may stay idle before it is closed (default 60)
    #[arg(long = "udp-session-timeout", value_name = "SECS")]
    pub udp_session_timeout_secs: Option<u64>,

    /// Max concurrent UDP sessions across all UDP forwarders (default 16384)
    #[arg(long = "udp-max-sessions", value_name = "N")]
    pub udp_max_sessions: Option<usize>,

    /// Enable the Linux UDP GRO/GSO fast path (same as `udp_gro_gso: true`).
    #[arg(long = "udp-gro-gso")]
    pub udp_gro_gso: bool,

    /// Directory for the rolling JSON log file. Use `--log-dir ""` to disable
    /// file logging entirely and log to the console only.
    #[arg(long = "log-dir", default_value = "logs")]
    pub log_dir: String,

    /// Probe a running rfw's stats endpoint at HOST:PORT and exit 0 if healthy,
    /// 1 otherwise. Intended as a container healthcheck in images with no shell
    /// (the binary is its own probe). Starts no forwarders.
    #[arg(long = "health-check", value_name = "HOST:PORT")]
    pub health_check: Option<String>,

    /// Forwarders in format local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp]
    /// (a port range maps port for port)
    #[arg(trailing_var_arg = true)]
    pub forwarders: Vec<String>,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            config_file: None,
            metrics_addr: None,
            report_interval_secs: None,
            sample_interval_secs: None,
            buffer_bytes: None,
            socket_buffer_bytes: None,
            use_splice: false,
            max_connections: None,
            connect_timeout_secs: None,
            dns_timeout_secs: None,
            keepalive_idle_secs: None,
            keepalive_interval_secs: None,
            keepalive_retries: None,
            webhook_url: None,
            instance_name: None,
            probe_interval_secs: None,
            unreachable_after_secs: None,
            udp_session_timeout_secs: None,
            udp_max_sessions: None,
            udp_gro_gso: false,
            log_dir: "logs".to_string(),
            health_check: None,
            forwarders: Vec::new(),
        }
    }
}

/// Parse one forwarder entry
/// `local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp]`.
///
/// A range on one side needs a range of the same length on the other; the
/// check lives in [`ForwarderSpec::new`] so CLI, env and YAML agree.
fn parse_forwarder_spec(s: &str) -> Result<ForwarderSpec, String> {
    let (body, protocol) = match s.rsplit_once('/') {
        None => (s, Protocol::Tcp),
        Some((body, suffix)) => {
            let protocol = if suffix.eq_ignore_ascii_case("tcp") {
                Protocol::Tcp
            } else if suffix.eq_ignore_ascii_case("udp") {
                Protocol::Udp
            } else {
                return Err(format!(
                    "Unknown protocol suffix '/{suffix}' in '{s}': expected /tcp or /udp"
                ));
            };
            (body, protocol)
        }
    };
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() != 4 {
        return Err(format!(
            "Invalid forwarder format '{s}': expected local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp]"
        ));
    }
    ForwarderSpec::new(
        parts[0].to_string(),
        PortSpan::parse(parts[1], "local")?,
        parts[2].to_string(),
        PortSpan::parse(parts[3], "remote")?,
        protocol,
    )
}

/// Load forwarders from RFW_FORWARDER_N env vars. An entry that does not parse
/// is skipped with a warning; entries that parse but claim the same local
/// endpoint are an error.
fn load_env_forwarders() -> Result<Vec<ForwarderConfig>, anyhow::Error> {
    let mut entries: Vec<_> = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("RFW_FORWARDER_")
                .and_then(|suffix| suffix.parse::<usize>().ok())
                .map(|index| (index, key, value))
        })
        .collect();

    entries.sort_by_key(|(index, _, _)| *index);

    let mut specs = Vec::with_capacity(entries.len());
    for (_, key, value) in entries {
        match parse_forwarder_spec(&value) {
            Ok(spec) => specs.push(spec),
            Err(error) => {
                warn!(env_var = %key, error = %error, "Ignoring invalid env forwarder");
            }
        }
    }

    expand_specs(&specs)
}

/// Parsed YAML config: the forwarder list plus the `settings:` block.
#[derive(Debug)]
pub(crate) struct LoadedConfig {
    pub forwarders: Vec<ForwarderConfig>,
    pub settings: Settings,
}

/// Read and parse a YAML config file once, returning both halves.
///
/// Hot-reload uses this so the `settings:` block and the `forwarders:` list are
/// applied atomically: a file that fails to parse leaves *both* untouched,
/// instead of resetting the data-plane knobs to their defaults while the
/// forwarder list keeps running (which is what two separate loads did).
pub(crate) fn load_yaml_config(path: &std::path::Path) -> Result<LoadedConfig, anyhow::Error> {
    let content = std::fs::read_to_string(path)?;
    let cf: ConfigFile = serde_saphyr::from_str(&content)?;
    validate_no_duplicates(&cf.forwarders)?;
    Ok(LoadedConfig {
        forwarders: cf.forwarders,
        settings: cf.settings,
    })
}

/// Load forwarders from a YAML config file.
pub(crate) fn load_yaml_forwarders(
    path: &std::path::Path,
) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
    Ok(load_yaml_config(path)?.forwarders)
}

/// Where the running forwarder list came from at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwarderSource {
    Env,
    Cli,
    File,
    None,
}

impl std::fmt::Display for ForwarderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ForwarderSource::Env => "RFW_FORWARDER_N environment variables",
            ForwarderSource::Cli => "command-line arguments",
            ForwarderSource::File => "config file",
            ForwarderSource::None => "nowhere",
        })
    }
}

/// Load forwarders merging YAML file, CLI args, and env vars, and report which
/// source the returned list came from.
///
/// Precedence: env vars > CLI args > YAML file.
/// - If env vars are set, they completely replace all other sources.
/// - If CLI args are given, they replace the YAML config.
/// - Otherwise the YAML file is used.
pub fn load_forwarders_with_source(
    cli: &CliArgs,
) -> Result<(Vec<ForwarderConfig>, ForwarderSource), anyhow::Error> {
    let (configured_forwarders, configured_source) = if !cli.forwarders.is_empty() {
        // CLI args: parse and use instead of YAML
        let specs = cli
            .forwarders
            .iter()
            .map(|s| parse_forwarder_spec(s).map_err(|e| anyhow::anyhow!("{}", e)))
            .collect::<Result<Vec<_>, _>>()?;
        (expand_specs(&specs)?, ForwarderSource::Cli)
    } else if let Some(path) = &cli.config_file {
        // YAML file
        (load_yaml_forwarders(path)?, ForwarderSource::File)
    } else {
        (Vec::new(), ForwarderSource::None)
    };

    // Env vars override everything
    let env_forwarders = load_env_forwarders()?;
    let (forwarders, source) = if env_forwarders.is_empty() {
        (configured_forwarders, configured_source)
    } else {
        (env_forwarders, ForwarderSource::Env)
    };

    validate_no_duplicates(&forwarders)?;
    Ok((forwarders, source))
}

/// Test-only convenience: the forwarder list without its source.
#[cfg(test)]
pub fn load_forwarders(cli: &CliArgs) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
    Ok(load_forwarders_with_source(cli)?.0)
}

/// Apply every CLI flag the operator actually passed over `settings`.
/// Shared by startup (`load_settings`) and hot-reload, so CLI flags win in both.
pub fn apply_cli_overrides(settings: &mut Settings, cli: &CliArgs) {
    if let Some(addr) = &cli.metrics_addr {
        settings.metrics_addr = Some(addr.clone());
    }
    if let Some(v) = cli.report_interval_secs {
        settings.report_interval_secs = v;
    }
    if let Some(v) = cli.sample_interval_secs {
        settings.sample_interval_secs = v;
    }
    if let Some(v) = cli.buffer_bytes {
        settings.buffer_bytes = v;
    }
    if let Some(v) = cli.socket_buffer_bytes {
        settings.socket_buffer_bytes = Some(v);
    }
    if cli.use_splice {
        settings.use_splice = true;
    }
    if let Some(v) = cli.max_connections {
        settings.max_connections = Some(v);
    }
    if let Some(v) = cli.connect_timeout_secs {
        settings.connect_timeout_secs = v;
    }
    if let Some(v) = cli.dns_timeout_secs {
        settings.dns_timeout_secs = v;
    }
    if let Some(v) = cli.keepalive_idle_secs {
        settings.keepalive_idle_secs = v;
    }
    if let Some(v) = cli.keepalive_interval_secs {
        settings.keepalive_interval_secs = v;
    }
    if let Some(v) = cli.keepalive_retries {
        settings.keepalive_retries = v;
    }
    if let Some(url) = &cli.webhook_url {
        // An empty value (e.g. `RFW_WEBHOOK_URL=` in a compose file) means off.
        settings.webhook_url = Some(url.clone()).filter(|u| !u.trim().is_empty());
    }
    if let Some(name) = &cli.instance_name {
        settings.instance_name = Some(name.clone());
    }
    if let Some(v) = cli.probe_interval_secs {
        settings.probe_interval_secs = v;
    }
    if let Some(v) = cli.unreachable_after_secs {
        settings.unreachable_after_secs = v;
    }
    if let Some(v) = cli.udp_session_timeout_secs {
        settings.udp_session_timeout_secs = v;
    }
    if let Some(v) = cli.udp_max_sessions {
        settings.udp_max_sessions = v;
    }
    if cli.udp_gro_gso {
        settings.udp_gro_gso = true;
    }
}

/// Load runtime settings: YAML `settings:` block (if a config file is given),
/// with CLI flags overriding individual fields. Missing/unreadable file falls
/// back to defaults (the forwarder load path already surfaces file errors).
pub fn load_settings(cli: &CliArgs) -> Result<Settings, anyhow::Error> {
    let mut settings = match &cli.config_file {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(content) => serde_saphyr::from_str::<ConfigFile>(&content)?.settings,
            Err(_) => Settings::default(),
        },
        None => Settings::default(),
    };

    apply_cli_overrides(&mut settings, cli);

    settings.validate()?;
    Ok(settings)
}

/// Check for duplicate local endpoints among forwarders. The key is the
/// protocol plus the local address, so a TCP and a UDP forwarder may share a port.
fn validate_no_duplicates(fwd: &[ForwarderConfig]) -> Result<(), anyhow::Error> {
    let mut seen = std::collections::HashSet::new();
    for f in fwd {
        let addr = f.local_addr();
        if !seen.insert((f.protocol, addr.clone())) {
            let shown = match f.protocol {
                Protocol::Tcp => addr,
                Protocol::Udp => format!("{addr}/udp"),
            };
            anyhow::bail!(
                "Duplicate local address '{}'. Each forwarder needs a unique local endpoint.",
                shown
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct ForwarderEnvGuard {
        saved: Vec<(String, String)>,
    }

    impl ForwarderEnvGuard {
        fn set(vars: &[(&str, &str)]) -> Self {
            let saved = std::env::vars()
                .filter(|(key, _)| key.starts_with("RFW_FORWARDER_"))
                .collect::<Vec<_>>();

            clear_forwarder_env();
            for (key, value) in vars {
                unsafe {
                    std::env::set_var(key, value);
                }
            }

            Self { saved }
        }
    }

    impl Drop for ForwarderEnvGuard {
        fn drop(&mut self) {
            clear_forwarder_env();
            for (key, value) in self.saved.drain(..) {
                unsafe {
                    std::env::set_var(key, value);
                }
            }
        }
    }

    fn clear_forwarder_env() {
        let keys = std::env::vars()
            .filter(|(key, _)| key.starts_with("RFW_FORWARDER_"))
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        for key in keys {
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn test_settings_defaults() {
        let yaml =
            "forwarders:\n  - {local_host: a, local_port: 1, remote_host: b, remote_port: 2}\n";
        let cf: ConfigFile = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(cf.settings.report_interval_secs, 60);
        assert_eq!(cf.settings.sample_interval_secs, 1);
        assert_eq!(cf.settings.buffer_bytes, 65536);
        assert!(cf.settings.metrics_addr.is_none());
        // Keepalive is on by default: a DB forwarder must reap half-open peers.
        assert_eq!(cf.settings.keepalive_idle_secs, 60);
        assert_eq!(cf.settings.keepalive_interval_secs, 10);
        assert_eq!(cf.settings.keepalive_retries, 3);
    }

    #[test]
    fn yaml_null_and_missing_optionals_parse_as_none() {
        // The example config documents `null` / omitted as "disabled". The
        // parser swap (serde_yaml -> serde-saphyr) must keep that contract.
        let yaml = "forwarders:\n  - {local_host: a, local_port: 1, remote_host: b, remote_port: 2}\nsettings:\n  metrics_addr: null\n  socket_buffer_bytes: ~\n";
        let cf: ConfigFile = serde_saphyr::from_str(yaml).unwrap();
        assert!(cf.settings.metrics_addr.is_none());
        assert!(cf.settings.socket_buffer_bytes.is_none());
        assert!(cf.settings.max_connections.is_none());
    }

    #[test]
    fn yaml_comments_and_quoted_scalars_parse() {
        // Shape of the shipped forwarders.yml: comments, quoted hosts, ints.
        let yaml = "# header\nforwarders:\n  # a forwarder\n  - local_host: \"0.0.0.0\"\n    local_port: 5432\n    remote_host: \"db.example.com\"  # trailing\n    remote_port: 5432\nsettings:\n  metrics_addr: \"127.0.0.1:9090\"\n  use_splice: false\n  keepalive_idle_secs: 0\n";
        let cf: ConfigFile = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(cf.forwarders.len(), 1);
        assert_eq!(cf.forwarders[0].local_host, "0.0.0.0");
        assert_eq!(cf.forwarders[0].remote_host, "db.example.com");
        assert_eq!(cf.settings.metrics_addr.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(cf.settings.keepalive_idle_secs, 0);
    }

    #[test]
    fn shipped_example_config_parses_and_validates() {
        // The file in the repo root is what users copy; it must always load.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("forwarders.yml");
        let loaded = load_yaml_config(&path).expect("forwarders.yml must parse");
        assert_eq!(loaded.forwarders.len(), 6);
        assert_eq!(
            loaded
                .forwarders
                .iter()
                .filter(|f| f.protocol == Protocol::Udp)
                .count(),
            2
        );
        loaded
            .settings
            .validate()
            .expect("forwarders.yml settings must validate");
    }

    #[test]
    fn test_cli_overrides_settings() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs::parse_from([
            "rfw",
            "--report-interval",
            "5",
            "--buffer-bytes",
            "1234",
            "127.0.0.1:1:b:2",
        ]);
        let s = load_settings(&cli).unwrap();
        assert_eq!(s.report_interval_secs, 5);
        assert_eq!(s.buffer_bytes, 1234);
        assert_eq!(s.sample_interval_secs, 1); // untouched -> default
        assert!(s.metrics_addr.is_none());
    }

    #[test]
    fn test_parse_forwarder_ok() {
        let f = parse_forwarder("localhost:8080:172.16.0.5:80").unwrap();
        assert_eq!(f.local_host, "localhost");
        assert_eq!(f.local_port, 8080);
        assert_eq!(f.remote_host, "172.16.0.5");
        assert_eq!(f.remote_port, 80);
    }

    #[test]
    fn test_parse_forwarder_ipv6() {
        // IPv6 addresses contain colons - this won't parse correctly with simple split
        // For now this is expected to fail; IPv6 support requires smarter parsing
        let result = parse_forwarder(":::8080:::80");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_forwarder_invalid_parts() {
        assert!(parse_forwarder("a:b:c").is_err());
        assert!(parse_forwarder("a:b:c:d:e").is_err());
    }

    #[test]
    fn test_parse_forwarder_invalid_port() {
        assert!(parse_forwarder("localhost:bad:remote:80").is_err());
    }

    #[test]
    fn test_validate_duplicates() {
        let f = vec![
            ForwarderConfig {
                local_host: "0.0.0.0".into(),
                local_port: 8080,
                remote_host: "a.com".into(),
                remote_port: 80,
                protocol: Protocol::Tcp,
            },
            ForwarderConfig {
                local_host: "0.0.0.0".into(),
                local_port: 8080,
                remote_host: "b.com".into(),
                remote_port: 80,
                protocol: Protocol::Tcp,
            },
        ];
        assert!(validate_no_duplicates(&f).is_err());
    }

    #[test]
    fn test_label() {
        let f = ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port: 9090,
            remote_host: "example.com".into(),
            remote_port: 443,
            protocol: Protocol::Tcp,
        };
        assert_eq!(f.label(), "127.0.0.1:9090->example.com:443");
    }

    #[test]
    fn test_load_forwarders_env_overrides_cli_with_sparse_indices() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[
            ("RFW_FORWARDER_3", "127.0.0.1:9003:three.example:80"),
            ("RFW_FORWARDER_1", "127.0.0.1:9001:one.example:80"),
        ]);

        let cli = CliArgs {
            forwarders: vec!["127.0.0.1:8000:cli.example:80".into()],
            ..CliArgs::default()
        };

        let forwarders = load_forwarders(&cli).expect("env forwarders should load");

        assert_eq!(forwarders.len(), 2);
        assert_eq!(forwarders[0].local_port, 9001);
        assert_eq!(forwarders[1].local_port, 9003);
    }

    #[test]
    fn test_load_forwarders_validates_duplicates_from_env() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[
            ("RFW_FORWARDER_1", "0.0.0.0:8080:first.example:80"),
            ("RFW_FORWARDER_2", "0.0.0.0:8080:second.example:443"),
        ]);

        let cli = CliArgs::default();

        let error = load_forwarders(&cli).expect_err("duplicate env forwarders must fail");
        assert!(error
            .to_string()
            .contains("Duplicate local address '0.0.0.0:8080'"));
    }

    // ---- Settings validation ------------------------------------------------
    //
    // These values reach `tokio::time::interval` and `copy_bidirectional`
    // directly. Before validation existed, `--sample-interval 0` panicked the
    // sampler task (silently killing every throughput gauge for the life of the
    // process) and `--buffer-bytes 0` made the copy loop spin at ~67% CPU while
    // forwarding nothing.

    #[test]
    fn defaults_are_valid() {
        Settings::default()
            .validate()
            .expect("defaults must validate");
    }

    #[test]
    fn validate_rejects_zero_report_interval() {
        let s = Settings {
            report_interval_secs: 0,
            ..Settings::default()
        };
        let err = s
            .validate()
            .expect_err("zero report interval must be rejected");
        assert!(err.to_string().contains("report_interval_secs"), "{err}");
    }

    #[test]
    fn validate_rejects_zero_sample_interval() {
        let s = Settings {
            sample_interval_secs: 0,
            ..Settings::default()
        };
        let err = s
            .validate()
            .expect_err("zero sample interval must be rejected");
        assert!(err.to_string().contains("sample_interval_secs"), "{err}");
    }

    #[test]
    fn validate_rejects_out_of_range_buffer() {
        for bad in [0, MIN_BUFFER_BYTES - 1, MAX_BUFFER_BYTES + 1] {
            let s = Settings {
                buffer_bytes: bad,
                ..Settings::default()
            };
            let err = s
                .validate()
                .expect_err(&format!("buffer_bytes {bad} must be rejected"));
            assert!(err.to_string().contains("buffer_bytes"), "{err}");
        }
        for good in [MIN_BUFFER_BYTES, 65536, MAX_BUFFER_BYTES] {
            Settings {
                buffer_bytes: good,
                ..Settings::default()
            }
            .validate()
            .unwrap_or_else(|e| panic!("buffer_bytes {good} must be accepted: {e}"));
        }
    }

    #[test]
    fn validate_rejects_zero_timeouts_and_limits() {
        let cases: Vec<(Settings, &str)> = vec![
            (
                Settings {
                    connect_timeout_secs: 0,
                    ..Settings::default()
                },
                "connect_timeout_secs",
            ),
            (
                Settings {
                    dns_timeout_secs: 0,
                    ..Settings::default()
                },
                "dns_timeout_secs",
            ),
            (
                Settings {
                    max_connections: Some(0),
                    ..Settings::default()
                },
                "max_connections",
            ),
            (
                Settings {
                    socket_buffer_bytes: Some(0),
                    ..Settings::default()
                },
                "socket_buffer_bytes",
            ),
        ];
        for (s, field) in cases {
            let err = s.validate().expect_err("must be rejected");
            assert!(err.to_string().contains(field), "expected {field}: {err}");
        }
    }

    #[test]
    fn validate_keepalive_rules() {
        // Disabled keepalive does not care about the other two fields.
        Settings {
            keepalive_idle_secs: 0,
            keepalive_interval_secs: 0,
            keepalive_retries: 0,
            ..Settings::default()
        }
        .validate()
        .expect("keepalive off must validate regardless of interval/retries");

        // Enabled keepalive needs a usable cadence.
        for (s, field) in [
            (
                Settings {
                    keepalive_interval_secs: 0,
                    ..Settings::default()
                },
                "keepalive_interval_secs",
            ),
            (
                Settings {
                    keepalive_retries: 0,
                    ..Settings::default()
                },
                "keepalive_retries",
            ),
        ] {
            let err = s.validate().expect_err("must be rejected");
            assert!(err.to_string().contains(field), "expected {field}: {err}");
        }
    }

    #[test]
    fn clamp_repairs_keepalive_cadence_only_when_enabled() {
        let mut on = Settings {
            keepalive_interval_secs: 0,
            keepalive_retries: 0,
            ..Settings::default()
        };
        on.clamp_in_place();
        assert_eq!(on.keepalive_interval_secs, 1);
        assert_eq!(on.keepalive_retries, 1);
        on.validate().expect("clamped keepalive must validate");

        let mut off = Settings {
            keepalive_idle_secs: 0,
            keepalive_interval_secs: 0,
            keepalive_retries: 0,
            ..Settings::default()
        };
        off.clamp_in_place();
        assert_eq!(
            off.keepalive_idle_secs, 0,
            "clamping must not turn keepalive on"
        );
        assert_eq!(off.keepalive_interval_secs, 0);
    }

    #[test]
    fn knobs_expose_keepalive_and_zero_idle_disables_it() {
        let knobs = RuntimeKnobs::from_settings(&Settings::default());
        let ka = knobs.keepalive().expect("default keepalive is on");
        assert_eq!(ka.idle, std::time::Duration::from_secs(60));
        assert_eq!(ka.interval, std::time::Duration::from_secs(10));
        assert_eq!(ka.retries, 3);
        assert_eq!(knobs.snapshot().keepalive, Some(ka));

        // Hot-reload can switch it off and on again.
        knobs.update(&Settings {
            keepalive_idle_secs: 0,
            ..Settings::default()
        });
        assert!(knobs.keepalive().is_none());
        assert!(knobs.snapshot().keepalive.is_none());

        knobs.update(&Settings {
            keepalive_idle_secs: 30,
            keepalive_interval_secs: 5,
            keepalive_retries: 2,
            ..Settings::default()
        });
        let ka = knobs.keepalive().unwrap();
        assert_eq!(ka.idle, std::time::Duration::from_secs(30));
        assert_eq!(ka.interval, std::time::Duration::from_secs(5));
        assert_eq!(ka.retries, 2);
    }

    #[test]
    fn cli_keepalive_flags_override_settings() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs::parse_from([
            "rfw",
            "--keepalive-idle",
            "120",
            "--keepalive-interval",
            "15",
            "--keepalive-retries",
            "5",
            "127.0.0.1:1:b:2",
        ]);
        let s = load_settings(&cli).unwrap();
        assert_eq!(s.keepalive_idle_secs, 120);
        assert_eq!(s.keepalive_interval_secs, 15);
        assert_eq!(s.keepalive_retries, 5);

        // `--keepalive-idle 0` is the documented off switch.
        let cli = CliArgs::parse_from(["rfw", "--keepalive-idle", "0", "127.0.0.1:1:b:2"]);
        let s = load_settings(&cli).unwrap();
        assert_eq!(s.keepalive_idle_secs, 0);
        assert!(RuntimeKnobs::from_settings(&s).keepalive().is_none());
    }

    #[test]
    fn knobs_expose_the_udp_session_timeout() {
        let knobs = RuntimeKnobs::from_settings(&Settings {
            udp_session_timeout_secs: 7,
            ..Settings::default()
        });
        assert_eq!(
            knobs.udp_session_timeout(),
            std::time::Duration::from_secs(7)
        );
        knobs.update(&Settings {
            udp_session_timeout_secs: 0,
            ..Settings::default()
        });
        assert_eq!(
            knobs.udp_session_timeout(),
            std::time::Duration::from_secs(1)
        );
    }

    #[test]
    fn validate_rejects_unparseable_metrics_addr() {
        let s = Settings {
            metrics_addr: Some("nonsense".into()),
            ..Settings::default()
        };
        assert!(s.validate().is_err());

        let ok = Settings {
            metrics_addr: Some("127.0.0.1:9090".into()),
            ..Settings::default()
        };
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn clamp_repairs_instead_of_failing() {
        // Hot-reload path: a bad edit must degrade, not abort a live process.
        let mut s = Settings {
            report_interval_secs: 0,
            sample_interval_secs: 0,
            connect_timeout_secs: 0,
            dns_timeout_secs: 0,
            buffer_bytes: 0,
            socket_buffer_bytes: Some(0),
            max_connections: Some(0),
            ..Settings::default()
        };
        s.clamp_in_place();
        assert_eq!(s.report_interval_secs, 1);
        assert_eq!(s.sample_interval_secs, 1);
        assert_eq!(s.connect_timeout_secs, 1);
        assert_eq!(s.dns_timeout_secs, 1);
        assert_eq!(s.buffer_bytes, MIN_BUFFER_BYTES);
        assert_eq!(s.socket_buffer_bytes, None);
        assert_eq!(s.max_connections, None);
        s.validate().expect("clamped settings must be valid");
    }

    #[test]
    fn clamp_leaves_valid_settings_untouched() {
        let original = Settings {
            buffer_bytes: 131_072,
            socket_buffer_bytes: Some(1 << 20),
            max_connections: Some(1024),
            report_interval_secs: 30,
            ..Settings::default()
        };
        let mut s = original.clone();
        s.clamp_in_place();
        assert_eq!(s.buffer_bytes, original.buffer_bytes);
        assert_eq!(s.socket_buffer_bytes, original.socket_buffer_bytes);
        assert_eq!(s.max_connections, original.max_connections);
        assert_eq!(s.report_interval_secs, original.report_interval_secs);
    }

    // ---- RuntimeKnobs -------------------------------------------------------

    #[test]
    fn knobs_never_install_a_zero_buffer() {
        // A hot-reload must not be able to push a spinning buffer size into the
        // live data plane, even though the file was accepted by serde.
        let knobs = RuntimeKnobs::from_settings(&Settings {
            buffer_bytes: 0,
            ..Settings::default()
        });
        assert_eq!(knobs.buffer_bytes(), MIN_BUFFER_BYTES);

        knobs.update(&Settings {
            buffer_bytes: 0,
            ..Settings::default()
        });
        assert_eq!(knobs.buffer_bytes(), MIN_BUFFER_BYTES);
    }

    #[test]
    fn knobs_round_trip_and_update() {
        let knobs = RuntimeKnobs::from_settings(&Settings::default());
        assert_eq!(knobs.buffer_bytes(), default_buffer_bytes());
        assert_eq!(knobs.socket_buffer_bytes(), None);
        assert!(!knobs.use_splice());

        knobs.update(&Settings {
            buffer_bytes: 4096,
            socket_buffer_bytes: Some(8192),
            use_splice: true,
            connect_timeout_secs: 3,
            dns_timeout_secs: 2,
            ..Settings::default()
        });

        let snap = knobs.snapshot();
        assert_eq!(snap.buffer_bytes, 4096);
        assert_eq!(snap.socket_buffer_bytes, Some(8192));
        assert!(snap.use_splice);
        assert_eq!(snap.connect_timeout, std::time::Duration::from_secs(3));
        assert_eq!(snap.dns_timeout, std::time::Duration::from_secs(2));
    }

    #[test]
    fn knobs_zero_socket_buffer_means_os_default() {
        let knobs = RuntimeKnobs::from_settings(&Settings {
            socket_buffer_bytes: Some(0),
            ..Settings::default()
        });
        assert_eq!(knobs.socket_buffer_bytes(), None);
    }

    // ---- YAML loading -------------------------------------------------------

    fn write_temp(name: &str, body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("rfw-test-{name}-{}.yml", std::process::id()));
        std::fs::write(&path, body).expect("write temp config");
        path
    }

    #[test]
    fn load_yaml_config_returns_both_halves() {
        let path = write_temp(
            "both",
            "forwarders:\n  - {local_host: 127.0.0.1, local_port: 1, remote_host: b, remote_port: 2}\nsettings:\n  buffer_bytes: 4096\n  use_splice: true\n",
        );
        let loaded = load_yaml_config(&path).expect("must parse");
        assert_eq!(loaded.forwarders.len(), 1);
        assert_eq!(loaded.settings.buffer_bytes, 4096);
        assert!(loaded.settings.use_splice);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_yaml_config_rejects_duplicate_local_addresses() {
        // The reload path used to skip this check, so a duplicate in an edited
        // file started two forwarders on one port and left one retrying its
        // bind every 5s forever.
        let path = write_temp(
            "dupes",
            "forwarders:\n  - {local_host: 0.0.0.0, local_port: 8080, remote_host: a, remote_port: 80}\n  - {local_host: 0.0.0.0, local_port: 8080, remote_host: b, remote_port: 80}\n",
        );
        let err = load_yaml_config(&path).expect_err("duplicates must be rejected");
        assert!(err.to_string().contains("Duplicate local address"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_yaml_config_reports_parse_errors() {
        // A half-written file (editor mid-save) must surface as an error so the
        // caller keeps the running configuration rather than applying defaults.
        let path = write_temp(
            "truncated",
            "forwarders:\n  - local_host: \"127.0.0.1\"\n    local_p",
        );
        assert!(load_yaml_config(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_yaml_config_reports_missing_file() {
        let missing = std::env::temp_dir().join("rfw-test-definitely-not-here.yml");
        let _ = std::fs::remove_file(&missing);
        assert!(load_yaml_config(&missing).is_err());
    }

    // ---- CLI ----------------------------------------------------------------

    #[test]
    fn cli_overrides_new_settings() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs::parse_from([
            "rfw",
            "--max-connections",
            "512",
            "--connect-timeout",
            "3",
            "--dns-timeout",
            "2",
            "127.0.0.1:1:b:2",
        ]);
        let s = load_settings(&cli).unwrap();
        assert_eq!(s.max_connections, Some(512));
        assert_eq!(s.connect_timeout_secs, 3);
        assert_eq!(s.dns_timeout_secs, 2);
    }

    #[test]
    fn load_settings_rejects_invalid_cli_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs::parse_from(["rfw", "--buffer-bytes", "0", "127.0.0.1:1:b:2"]);
        assert!(load_settings(&cli).is_err());
    }

    #[test]
    fn log_dir_defaults_to_logs_and_accepts_empty() {
        let cli = CliArgs::parse_from(["rfw", "127.0.0.1:1:b:2"]);
        assert_eq!(cli.log_dir, "logs");
        let cli = CliArgs::parse_from(["rfw", "--log-dir", "", "127.0.0.1:1:b:2"]);
        assert_eq!(cli.log_dir, "");
    }

    #[test]
    fn apply_cli_overrides_overrides_only_given_flags() {
        let mut s = Settings {
            buffer_bytes: 4096,
            probe_interval_secs: 30,
            ..Settings::default()
        };
        let cli = CliArgs {
            buffer_bytes: Some(2048),
            use_splice: true,
            ..CliArgs::default()
        };
        apply_cli_overrides(&mut s, &cli);
        assert_eq!(s.buffer_bytes, 2048);
        assert!(s.use_splice);
        assert_eq!(s.probe_interval_secs, 30);
        assert_eq!(s.keepalive_idle_secs, 60);

        let before = s.clone();
        apply_cli_overrides(&mut s, &CliArgs::default());
        assert_eq!(s.buffer_bytes, before.buffer_bytes);
        assert_eq!(s.probe_interval_secs, before.probe_interval_secs);
        assert_eq!(s.connect_timeout_secs, before.connect_timeout_secs);
        assert_eq!(s.use_splice, before.use_splice);
    }

    #[test]
    fn load_forwarders_reports_its_source() {
        let _lock = ENV_LOCK.lock().unwrap();

        let env = ForwarderEnvGuard::set(&[("RFW_FORWARDER_1", "127.0.0.1:1:127.0.0.1:2")]);
        let (fwd, src) = load_forwarders_with_source(&CliArgs::default()).unwrap();
        assert_eq!((fwd.len(), src), (1, ForwarderSource::Env));
        drop(env);

        let _env = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs {
            forwarders: vec!["127.0.0.1:3:127.0.0.1:4".into()],
            ..CliArgs::default()
        };
        let (fwd, src) = load_forwarders_with_source(&cli).unwrap();
        assert_eq!((fwd.len(), src), (1, ForwarderSource::Cli));

        let path = write_temp(
            "src-file",
            "forwarders:\n  - {local_host: 127.0.0.1, local_port: 5, remote_host: b, remote_port: 6}\n",
        );
        let cli = CliArgs {
            config_file: Some(path.clone()),
            ..CliArgs::default()
        };
        let (fwd, src) = load_forwarders_with_source(&cli).unwrap();
        assert_eq!((fwd.len(), src), (1, ForwarderSource::File));
        let _ = std::fs::remove_file(&path);

        let (fwd, src) = load_forwarders_with_source(&CliArgs::default()).unwrap();
        assert!(fwd.is_empty());
        assert_eq!(src, ForwarderSource::None);

        assert_eq!(
            ForwarderSource::Env.to_string(),
            "RFW_FORWARDER_N environment variables"
        );
        assert_eq!(ForwarderSource::Cli.to_string(), "command-line arguments");
        assert_eq!(ForwarderSource::File.to_string(), "config file");
        assert_eq!(ForwarderSource::None.to_string(), "nowhere");
    }

    #[test]
    fn load_settings_equals_file_plus_overrides() {
        let path = write_temp(
            "settings-eq",
            "forwarders:\n  - {local_host: 127.0.0.1, local_port: 1, remote_host: b, remote_port: 2}\nsettings:\n  buffer_bytes: 4096\n  probe_interval_secs: 30\n",
        );
        let cli = CliArgs {
            config_file: Some(path.clone()),
            buffer_bytes: Some(2048),
            ..CliArgs::default()
        };
        let s = load_settings(&cli).unwrap();
        assert_eq!(s.buffer_bytes, 2048);
        assert_eq!(s.probe_interval_secs, 30);

        let mut m = load_yaml_config(&path).unwrap().settings;
        apply_cli_overrides(&mut m, &cli);
        assert_eq!(m.buffer_bytes, 2048);
        assert_eq!(m.probe_interval_secs, 30);
        let _ = std::fs::remove_file(&path);
    }

    // ---- Protocol (UDP forwarding, sub-phase 1.1) ---------------------------

    fn fwd(protocol: Protocol, port: u16) -> ForwarderConfig {
        ForwarderConfig {
            local_host: "0.0.0.0".into(),
            local_port: port,
            remote_host: "db.example.com".into(),
            remote_port: port,
            protocol,
        }
    }

    #[test]
    fn parse_forwarder_accepts_protocol_suffixes() {
        assert_eq!(parse_forwarder("a:1:b:2").unwrap().protocol, Protocol::Tcp);
        assert_eq!(
            parse_forwarder("a:1:b:2/udp").unwrap().protocol,
            Protocol::Udp
        );
        assert_eq!(
            parse_forwarder("a:1:b:2/UDP").unwrap().protocol,
            Protocol::Udp
        );
        assert_eq!(
            parse_forwarder("a:1:b:2/Tcp").unwrap().protocol,
            Protocol::Tcp
        );
        let f = parse_forwarder("a:1:b:2/udp").unwrap();
        assert_eq!(f.local_host, "a");
        assert_eq!(f.local_port, 1);
        assert_eq!(f.remote_host, "b");
        assert_eq!(f.remote_port, 2);
    }

    #[test]
    fn parse_forwarder_rejects_unknown_or_empty_suffix() {
        assert_eq!(
            parse_forwarder("a:1:b:2/sctp").unwrap_err(),
            "Unknown protocol suffix '/sctp' in 'a:1:b:2/sctp': expected /tcp or /udp"
        );
        assert_eq!(
            parse_forwarder("a:1:b:53/").unwrap_err(),
            "Unknown protocol suffix '/' in 'a:1:b:53/': expected /tcp or /udp"
        );
        assert_eq!(
            parse_forwarder("/udp").unwrap_err(),
            "Invalid forwarder format '/udp': expected local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp]"
        );
        assert_eq!(
            parse_forwarder("a:b:2/udp").unwrap_err(),
            "Invalid forwarder format 'a:b:2/udp': expected local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp]"
        );
    }

    #[test]
    fn tcp_label_is_byte_identical_to_v1_0_6() {
        let f = ForwarderConfig {
            local_host: "0.0.0.0".into(),
            local_port: 5432,
            remote_host: "db.example.com".into(),
            remote_port: 5432,
            protocol: Protocol::Tcp,
        };
        assert_eq!(f.label(), "0.0.0.0:5432->db.example.com:5432");
    }

    #[test]
    fn udp_label_has_the_udp_suffix() {
        let f = fwd(Protocol::Udp, 5432);
        assert_eq!(f.label(), "0.0.0.0:5432->db.example.com:5432/udp");
    }

    #[test]
    fn yaml_protocol_defaults_aliases_and_rejects_unknown() {
        let entry = |extra: &str| {
            format!(
                "forwarders:\n  - {{local_host: 127.0.0.1, local_port: 1, remote_host: b, remote_port: 2{extra}}}\n"
            )
        };
        let proto = |extra: &str| {
            serde_saphyr::from_str::<ConfigFile>(&entry(extra))
                .expect("parse")
                .forwarders[0]
                .protocol
        };
        assert_eq!(proto(""), Protocol::Tcp);
        assert_eq!(proto(", protocol: udp"), Protocol::Udp);
        assert_eq!(proto(", protocol: UDP"), Protocol::Udp);

        let path = write_temp("protocol-sctp", &entry(", protocol: sctp"));
        let res = load_yaml_config(&path);
        let _ = std::fs::remove_file(&path);
        assert!(res.is_err(), "unknown protocol must be a parse error");
    }

    #[test]
    fn duplicates_are_keyed_by_protocol_and_address() {
        validate_no_duplicates(&[fwd(Protocol::Tcp, 53), fwd(Protocol::Udp, 53)])
            .expect("tcp and udp may share a port");

        let err = validate_no_duplicates(&[fwd(Protocol::Udp, 53), fwd(Protocol::Udp, 53)])
            .expect_err("udp+udp must clash");
        assert!(
            err.to_string()
                .contains("Duplicate local address '0.0.0.0:53/udp'"),
            "{err}"
        );

        let err = validate_no_duplicates(&[fwd(Protocol::Tcp, 8080), fwd(Protocol::Tcp, 8080)])
            .expect_err("tcp+tcp must clash");
        assert!(
            err.to_string()
                .contains("Duplicate local address '0.0.0.0:8080'"),
            "{err}"
        );
    }

    #[test]
    fn udp_settings_defaults_validate_and_clamp() {
        let d = Settings::default();
        assert_eq!(d.udp_session_timeout_secs, 60);
        assert_eq!(d.udp_max_sessions, 16384);
        d.validate().expect("defaults are valid");

        let bad = |s: Settings| s.validate().is_err();
        assert!(bad(Settings {
            udp_session_timeout_secs: 0,
            ..Settings::default()
        }));
        assert!(bad(Settings {
            udp_max_sessions: 0,
            ..Settings::default()
        }));
        assert!(bad(Settings {
            udp_max_sessions: MAX_UDP_SESSIONS + 1,
            ..Settings::default()
        }));
        Settings {
            udp_max_sessions: MAX_UDP_SESSIONS,
            ..Settings::default()
        }
        .validate()
        .expect("MAX_UDP_SESSIONS is accepted");

        let mut s = Settings {
            udp_session_timeout_secs: 0,
            udp_max_sessions: 0,
            ..Settings::default()
        };
        s.clamp_in_place();
        assert_eq!(s.udp_session_timeout_secs, 1);
        assert_eq!(s.udp_max_sessions, 1);
        let mut s = Settings {
            udp_max_sessions: MAX_UDP_SESSIONS + 5,
            ..Settings::default()
        };
        s.clamp_in_place();
        assert_eq!(s.udp_max_sessions, MAX_UDP_SESSIONS);
    }

    #[test]
    fn cli_udp_flags_override_settings() {
        let cli = CliArgs::try_parse_from([
            "rfw",
            "--udp-session-timeout",
            "5",
            "--udp-max-sessions",
            "10",
            "127.0.0.1:1:127.0.0.1:2/udp",
        ])
        .expect("parse cli");
        let mut s = Settings::default();
        apply_cli_overrides(&mut s, &cli);
        assert_eq!(s.udp_session_timeout_secs, 5);
        assert_eq!(s.udp_max_sessions, 10);
        assert_eq!(
            cli.forwarders,
            vec!["127.0.0.1:1:127.0.0.1:2/udp".to_string()]
        );
    }

    #[test]
    fn udp_gro_gso_setting_cli_and_knob() {
        let path = write_temp(
            "udp-gro-gso",
            "forwarders:\n  - {local_host: 127.0.0.1, local_port: 1, remote_host: b, remote_port: 2}\nsettings:\n  udp_gro_gso: true\n",
        );
        let loaded = load_yaml_config(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(loaded.settings.udp_gro_gso);
        assert!(!Settings::default().udp_gro_gso);

        let cli = CliArgs::try_parse_from(["rfw", "--udp-gro-gso", "127.0.0.1:1:127.0.0.1:2/udp"])
            .expect("parse cli");
        let mut s = Settings::default();
        apply_cli_overrides(&mut s, &cli);
        assert!(s.udp_gro_gso);
        let plain = CliArgs::try_parse_from(["rfw", "127.0.0.1:1:127.0.0.1:2/udp"]).unwrap();
        let mut t = Settings::default();
        apply_cli_overrides(&mut t, &plain);
        assert!(!t.udp_gro_gso);

        let knobs = RuntimeKnobs::from_settings(&s);
        assert!(knobs.udp_gro_gso());
        knobs.update(&Settings::default());
        assert!(!knobs.udp_gro_gso());
    }

    // ---- Port ranges --------------------------------------------------------

    /// Parse one entry and expand it, the way every load path does.
    fn parse_forwarders(s: &str) -> Result<Vec<ForwarderConfig>, String> {
        parse_forwarder_spec(s).map(|spec| spec.expand())
    }

    /// Single-port convenience: the entry must expand to exactly one forwarder.
    fn parse_forwarder(s: &str) -> Result<ForwarderConfig, String> {
        let mut all = parse_forwarders(s)?;
        assert_eq!(all.len(), 1, "'{s}' is not a single-port entry");
        Ok(all.remove(0))
    }

    fn ports(list: &[ForwarderConfig]) -> Vec<(u16, u16)> {
        list.iter().map(|f| (f.local_port, f.remote_port)).collect()
    }

    fn expand_all(entries: &[&str]) -> Result<Vec<ForwarderConfig>, anyhow::Error> {
        let specs = entries
            .iter()
            .map(|e| parse_forwarder_spec(e).expect("entry must parse"))
            .collect::<Vec<_>>();
        expand_specs(&specs)
    }

    #[test]
    fn range_expands_one_forwarder_per_port_at_the_same_offset() {
        let list = parse_forwarders("0.0.0.0:30000-30003:tunnel.example.com:40000-40003").unwrap();
        assert_eq!(
            ports(&list),
            vec![
                (30000, 40000),
                (30001, 40001),
                (30002, 40002),
                (30003, 40003)
            ]
        );
        for f in &list {
            assert_eq!(f.local_host, "0.0.0.0");
            assert_eq!(f.remote_host, "tunnel.example.com");
            assert_eq!(f.protocol, Protocol::Tcp);
        }
    }

    #[test]
    fn range_with_the_same_ports_on_both_sides() {
        let list = parse_forwarders("0.0.0.0:30000-30050:h:30000-30050").unwrap();
        assert_eq!(list.len(), 51);
        assert!(list.iter().all(|f| f.local_port == f.remote_port));
        assert_eq!(list[0].local_port, 30000);
        assert_eq!(list[50].local_port, 30050);
    }

    #[test]
    fn udp_range_keeps_the_protocol_on_every_port() {
        for suffix in ["/udp", "/UDP"] {
            let list = parse_forwarders(&format!("0.0.0.0:5000-5002:h:6000-6002{suffix}")).unwrap();
            assert_eq!(ports(&list), vec![(5000, 6000), (5001, 6001), (5002, 6002)]);
            assert!(list.iter().all(|f| f.protocol == Protocol::Udp));
            assert_eq!(list[1].label(), "0.0.0.0:5001->h:6001/udp");
        }
        let tcp = parse_forwarders("0.0.0.0:5000-5001:h:6000-6001/tcp").unwrap();
        assert!(tcp.iter().all(|f| f.protocol == Protocol::Tcp));
    }

    #[test]
    fn single_ports_and_one_port_ranges_are_equivalent() {
        let single = parse_forwarders("a:7:b:9").unwrap();
        let ranged = parse_forwarders("a:7-7:b:9-9").unwrap();
        assert_eq!(ports(&single), ports(&ranged));
        assert_eq!(ranged.len(), 1);
    }

    #[test]
    fn range_may_end_at_the_top_of_the_port_space() {
        let list = parse_forwarders("a:65530-65535:b:65530-65535").unwrap();
        assert_eq!(list.len(), 6);
        assert_eq!(list[5].local_port, 65535);
        assert_eq!(list[5].remote_port, 65535);
        // and the overlap scan must not wrap past 65535
        assert_eq!(expand_all(&["a:65530-65535:b:1-6"]).unwrap().len(), 6);
    }

    #[test]
    fn hostnames_with_hyphens_are_not_mistaken_for_ranges() {
        let list = parse_forwarders("my-host:1-3:db-prod.example.com:10-12").unwrap();
        assert_eq!(list[0].local_host, "my-host");
        assert_eq!(list[0].remote_host, "db-prod.example.com");
        assert_eq!(ports(&list), vec![(1, 10), (2, 11), (3, 12)]);
        let single = parse_forwarder("my-host:80:db-prod.example.com:80").unwrap();
        assert_eq!(single.remote_host, "db-prod.example.com");
    }

    #[test]
    fn range_size_limit_is_enforced_and_inclusive() {
        let ok = format!("a:1-{MAX_RANGE_PORTS}:b:1-{MAX_RANGE_PORTS}");
        assert_eq!(parse_forwarders(&ok).unwrap().len(), MAX_RANGE_PORTS);
        let too_big = format!("a:1-{}:b:1-{}", MAX_RANGE_PORTS + 1, MAX_RANGE_PORTS + 1);
        let err = parse_forwarders(&too_big).unwrap_err();
        assert!(err.contains("exceed the limit"), "{err}");
    }

    #[test]
    fn range_with_different_lengths_is_rejected_naming_both_sides() {
        let err = parse_forwarders("a:30000-30050:b:30000-30010").unwrap_err();
        assert!(err.contains("length mismatch"), "{err}");
        assert!(err.contains("local 30000-30050 has 51 port(s)"), "{err}");
        assert!(err.contains("remote 30000-30010 has 11"), "{err}");
    }

    #[test]
    fn range_on_one_side_only_is_rejected() {
        // many local ports onto one remote port, and the reverse
        let err = parse_forwarders("a:30000-30050:b:80").unwrap_err();
        assert!(err.contains("length mismatch"), "{err}");
        let err = parse_forwarders("a:80:b:30000-30050").unwrap_err();
        assert!(err.contains("length mismatch"), "{err}");
    }

    #[test]
    fn malformed_ranges_are_rejected() {
        for bad in [
            "a:5-1:b:5-1", // reversed
            "a:0-5:b:1-6", // port 0 in a local range
            "a:1-6:b:0-5", // port 0 in a remote range
            "a:30000-:b:30000-30001",
            "a:-30050:b:1-2",
            "a:1-2-3:b:1-3",
            "a:1 -3:b:1-3",
            "a:1- 3:b:1-3",
            "a:+1-3:b:1-3",
            "a:1-3:b:+1-3",
            "a:1-70000:b:1-70000", // past 65535
            "a:x-y:b:1-2",
            "a:-:b:1-2",
            "a:1-2:b:1-",
        ] {
            assert!(parse_forwarders(bad).is_err(), "'{bad}' must be rejected");
        }
        let err = parse_forwarders("a:5-1:b:5-1").unwrap_err();
        assert!(err.contains("start 5 is greater than end 1"), "{err}");
        let err = parse_forwarders("a:0-5:b:1-6").unwrap_err();
        assert!(err.contains("port 0"), "{err}");
        // a single port 0 keeps its historical meaning (kernel-chosen port)
        assert_eq!(parse_forwarder("a:0:b:1").unwrap().local_port, 0);
    }

    // ---- Range overlaps and duplicates ----------------------------------------

    #[test]
    fn overlapping_ranges_are_rejected_naming_both_entries_and_the_port() {
        let err = expand_all(&[
            "0.0.0.0:30000-30050:h:30000-30050",
            "0.0.0.0:30040-30060:h:1-21",
        ])
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("Duplicate local address '0.0.0.0:30040'"),
            "{err}"
        );
        assert!(err.contains("0.0.0.0:30000-30050->h:30000-30050"), "{err}");
        assert!(err.contains("0.0.0.0:30040-30060->h:1-21"), "{err}");
    }

    #[test]
    fn a_single_port_inside_a_range_is_rejected_in_either_order() {
        for entries in [
            ["0.0.0.0:30000-30050:h:1-51", "0.0.0.0:30025:x:9"],
            ["0.0.0.0:30025:x:9", "0.0.0.0:30000-30050:h:1-51"],
        ] {
            let err = expand_all(&entries).unwrap_err().to_string();
            assert!(
                err.contains("Duplicate local address '0.0.0.0:30025'"),
                "{err}"
            );
            assert!(err.contains("defined by both"), "{err}");
        }
    }

    #[test]
    fn adjacent_and_disjoint_ranges_are_accepted() {
        let list =
            expand_all(&["a:100-109:h:100-109", "a:110-119:h:110-119", "a:200:h:200"]).unwrap();
        assert_eq!(list.len(), 21);
    }

    #[test]
    fn tcp_and_udp_ranges_may_share_the_same_ports() {
        let list = expand_all(&["a:100-109:h:100-109", "a:100-109:h:100-109/udp"]).unwrap();
        assert_eq!(list.len(), 20);
        validate_no_duplicates(&list).expect("flat list must also be clean");
    }

    #[test]
    fn two_udp_ranges_overlapping_are_rejected_with_the_udp_suffix() {
        let err = expand_all(&["a:100-109:h:1-10/udp", "a:109-111:h:1-3/udp"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Duplicate local address 'a:109/udp'"), "{err}");
    }

    #[test]
    fn different_local_hosts_may_reuse_ports() {
        // The duplicate check is by literal local address, as before ranges.
        let list = expand_all(&["127.0.0.1:100-102:h:1-3", "10.0.0.1:100-102:h:1-3"]).unwrap();
        assert_eq!(list.len(), 6);
    }

    #[test]
    fn the_same_entry_listed_twice_says_so() {
        let err = expand_all(&["a:100-102:h:1-3", "a:100-102:h:1-3"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("is listed twice"), "{err}");
        let err = expand_all(&["a:8080:h:1", "a:8080:h:1"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Duplicate local address 'a:8080'"), "{err}");
        assert!(err.contains("is listed twice"), "{err}");
    }

    #[test]
    fn expansion_preserves_entry_order() {
        let list = expand_all(&["a:300-301:h:1-2", "a:100:h:9", "a:200-201:h:5-6"]).unwrap();
        let locals: Vec<u16> = list.iter().map(|f| f.local_port).collect();
        assert_eq!(locals, vec![300, 301, 100, 200, 201]);
    }

    // ---- Ranges in YAML -----------------------------------------------------

    fn yaml_forwarders(entries: &str) -> Result<Vec<ForwarderConfig>, String> {
        let yaml = format!("forwarders:\n{entries}");
        serde_saphyr::from_str::<ConfigFile>(&yaml)
            .map(|cf| cf.forwarders)
            .map_err(|e| e.to_string())
    }

    #[test]
    fn yaml_local_port_end_expands_and_remote_follows_the_offset() {
        let list = yaml_forwarders(
            "  - {local_host: 0.0.0.0, local_port: 30000, local_port_end: 30002, remote_host: h, remote_port: 40000}\n",
        )
        .unwrap();
        assert_eq!(
            ports(&list),
            vec![(30000, 40000), (30001, 40001), (30002, 40002)]
        );
    }

    #[test]
    fn yaml_block_style_range_with_udp_and_explicit_remote_end() {
        let list = yaml_forwarders(
            "  - local_host: \"0.0.0.0\"\n    local_port: 5000\n    local_port_end: 5002\n    remote_host: \"h\"\n    remote_port: 7000\n    remote_port_end: 7002\n    protocol: udp\n  - {local_host: 0.0.0.0, local_port: 80, remote_host: h, remote_port: 8080}\n",
        )
        .unwrap();
        assert_eq!(
            ports(&list),
            vec![(5000, 7000), (5001, 7001), (5002, 7002), (80, 8080)]
        );
        assert!(list[..3].iter().all(|f| f.protocol == Protocol::Udp));
        assert_eq!(list[3].protocol, Protocol::Tcp);
    }

    #[test]
    fn yaml_one_port_range_is_allowed() {
        let list = yaml_forwarders(
            "  - {local_host: a, local_port: 7, local_port_end: 7, remote_host: b, remote_port: 9}\n",
        )
        .unwrap();
        assert_eq!(ports(&list), vec![(7, 9)]);
    }

    #[test]
    fn yaml_invalid_ranges_are_rejected() {
        let entry = |extra: &str| {
            format!("  - {{local_host: a, local_port: 100, remote_host: b, remote_port: 200, {extra}}}\n")
        };
        let cases = [
            ("local_port_end: 99", "greater than end"),
            (
                "local_port_end: 150, remote_port_end: 210",
                "length mismatch",
            ),
            ("remote_port_end: 210", "needs local_port_end"),
        ];
        for (extra, expect) in cases {
            let err = yaml_forwarders(&entry(extra)).expect_err(extra);
            assert!(err.contains(expect), "{extra}: {err}");
        }
        // the remote side would run past 65535
        let err = yaml_forwarders(
            "  - {local_host: a, local_port: 100, local_port_end: 150, remote_host: b, remote_port: 65500}\n",
        )
        .unwrap_err();
        assert!(err.contains("run past 65535"), "{err}");
        // port 0 in a range, local or remote
        for e in [
            "  - {local_host: a, local_port: 0, local_port_end: 5, remote_host: b, remote_port: 1}\n",
            "  - {local_host: a, local_port: 1, local_port_end: 5, remote_host: b, remote_port: 0}\n",
        ] {
            let err = yaml_forwarders(e).unwrap_err();
            assert!(err.contains("port 0"), "{err}");
        }
        // too many ports
        let err = yaml_forwarders(
            "  - {local_host: a, local_port: 1, local_port_end: 5000, remote_host: b, remote_port: 1}\n",
        )
        .unwrap_err();
        assert!(err.contains("exceed the limit"), "{err}");
    }

    #[test]
    fn yaml_overlapping_ranges_are_rejected_when_loaded() {
        let path = write_temp(
            "range-overlap",
            "forwarders:\n  - {local_host: a, local_port: 100, local_port_end: 110, remote_host: b, remote_port: 1}\n  - {local_host: a, local_port: 105, remote_host: c, remote_port: 2}\n",
        );
        let res = load_yaml_config(&path);
        let _ = std::fs::remove_file(&path);
        let err = res.expect_err("overlap must fail the load").to_string();
        assert!(err.contains("Duplicate local address 'a:105'"), "{err}");
    }

    #[test]
    fn yaml_range_keeps_settings_and_loads_through_load_yaml_config() {
        let path = write_temp(
            "range-ok",
            "forwarders:\n  - {local_host: a, local_port: 100, local_port_end: 102, remote_host: b, remote_port: 1}\nsettings:\n  buffer_bytes: 4096\n",
        );
        let loaded = load_yaml_config(&path).expect("must parse");
        let _ = std::fs::remove_file(&path);
        assert_eq!(loaded.forwarders.len(), 3);
        assert_eq!(loaded.settings.buffer_bytes, 4096);
    }

    #[test]
    fn a_plain_yaml_entry_without_range_fields_is_unchanged() {
        let list = yaml_forwarders(
            "  - {local_host: a, local_port: 1, remote_host: b, remote_port: 2, protocol: udp}\n",
        )
        .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].label(), "a:1->b:2/udp");
    }

    // ---- Ranges from env and CLI ----------------------------------------------

    #[test]
    fn env_ranges_expand_and_invalid_entries_are_skipped() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[
            ("RFW_FORWARDER_1", "0.0.0.0:9000-9002:h:9100-9102"),
            ("RFW_FORWARDER_2", "0.0.0.0:9500-9505:h:9500-9501"), // length mismatch
            ("RFW_FORWARDER_3", "0.0.0.0:9300:h:80/udp"),
        ]);
        let list = load_forwarders(&CliArgs::default()).expect("valid entries still load");
        assert_eq!(
            ports(&list),
            vec![(9000, 9100), (9001, 9101), (9002, 9102), (9300, 80)]
        );
        assert_eq!(list[3].protocol, Protocol::Udp);
    }

    #[test]
    fn env_overlapping_ranges_fail_the_load() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[
            ("RFW_FORWARDER_1", "0.0.0.0:9000-9010:h:1-11"),
            ("RFW_FORWARDER_2", "0.0.0.0:9005:h:80"),
        ]);
        let err = load_forwarders(&CliArgs::default()).expect_err("overlap must fail");
        assert!(
            err.to_string()
                .contains("Duplicate local address '0.0.0.0:9005'"),
            "{err}"
        );
    }

    #[test]
    fn cli_ranges_expand_and_errors_surface() {
        let _env_lock = ENV_LOCK.lock().expect("env lock poisoned");
        let _env_guard = ForwarderEnvGuard::set(&[]);
        let cli = CliArgs::try_parse_from([
            "rfw",
            "0.0.0.0:100-102:h:200-202",
            "0.0.0.0:100-101:h:200-201/udp",
            "0.0.0.0:500:h:600",
        ])
        .unwrap();
        let list = load_forwarders(&cli).expect("must load");
        assert_eq!(list.len(), 3 + 2 + 1);

        let cli = CliArgs::try_parse_from(["rfw", "0.0.0.0:100-110:h:200-205"]).unwrap();
        let err = load_forwarders(&cli).expect_err("mismatch must fail");
        assert!(err.to_string().contains("length mismatch"), "{err}");

        let cli =
            CliArgs::try_parse_from(["rfw", "0.0.0.0:100-110:h:1-11", "0.0.0.0:110:h:5"]).unwrap();
        let err = load_forwarders(&cli).expect_err("overlap must fail");
        assert!(
            err.to_string()
                .contains("Duplicate local address '0.0.0.0:110'"),
            "{err}"
        );
    }
}
