//! Open-file limit handling.
//!
//! Every proxied TCP connection holds two sockets (four more descriptors with
//! `--splice`: two pipes). Docker's default soft `RLIMIT_NOFILE` is 1024, so a
//! TCP-only forwarder started without `--ulimit` used to hit `EMFILE` at ~500
//! connections and then spin on failing accepts. The hard limit is almost
//! always far higher, so the fix is to raise the soft limit to it at startup.

use tracing::{info, warn};

/// Never ask for more than this even when the hard limit is "unlimited": some
/// kernels refuse `RLIM_INFINITY` for `RLIMIT_NOFILE`, and nothing here needs
/// more than a million descriptors.
const SOFT_LIMIT_CAP: u64 = 1_048_576;
/// Descriptors the process needs outside proxied connections (listeners, the
/// stats endpoint, log files, DNS, signal and watcher fds).
const BASE_FDS: u64 = 128;

/// What to do about the soft limit.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    /// Already as high as it can go.
    Keep,
    /// Raise the soft limit to this value.
    Raise(u64),
}

fn plan(soft: u64, hard: u64) -> Plan {
    let target = hard.min(SOFT_LIMIT_CAP);
    if target > soft {
        Plan::Raise(target)
    } else {
        Plan::Keep
    }
}

/// What the configuration can ask of the descriptor table, all at once.
#[derive(Debug, Clone, Copy, Default)]
pub struct Budget {
    /// `max_connections`, when capped.
    pub max_connections: Option<usize>,
    pub splice: bool,
    /// `udp_max_sessions` when a UDP forwarder is configured: one upstream
    /// socket per session.
    pub udp_sessions: Option<usize>,
    /// Listening sockets (one per forwarder; a port range has thousands).
    pub listeners: usize,
}

impl Budget {
    /// Descriptors needed when every cap is reached together. TCP
    /// connections, UDP sessions and listeners share one table, so the
    /// parts add up: sizing each against the limit on its own let a
    /// configuration through that could not fit as a whole.
    fn fds(&self) -> u64 {
        let tcp = (self.max_connections.unwrap_or(0) as u64).saturating_mul(if self.splice {
            6
        } else {
            2
        });
        tcp.saturating_add(self.udp_sessions.unwrap_or(0) as u64)
            .saturating_add(self.listeners as u64)
            .saturating_add(BASE_FDS)
    }
}

/// Raise the soft open-file limit to the hard limit and warn when the
/// configured caps still cannot fit. Failures are logged, never fatal.
#[cfg(unix)]
pub fn raise_soft_limit(budget: Budget) {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `rl` is a valid, writable `rlimit` for the duration of the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } != 0 {
        warn!(error = %std::io::Error::last_os_error(), "Could not read the open-file limit; not adjusting it");
        return;
    }
    // `rlim_t` is 64-bit on every platform this raises the limit on.
    let (soft, hard): (u64, u64) = (rl.rlim_cur, rl.rlim_max);
    let mut current = soft;
    if let Plan::Raise(to) = plan(soft, hard) {
        let raised = libc::rlimit {
            rlim_cur: to as libc::rlim_t,
            rlim_max: rl.rlim_max,
        };
        // SAFETY: `raised` is a valid `rlimit`; the call only reads it.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
            info!("Raised the open-file soft limit from {soft} to {to}");
            current = to;
        } else {
            warn!(error = %std::io::Error::last_os_error(), "Could not raise the open-file soft limit from {soft}");
        }
    }
    let needed = budget.fds();
    if current < needed {
        let tcp = match budget.max_connections {
            Some(max) => format!("max_connections = {max}"),
            None => "unlimited max_connections".to_string(),
        };
        let udp = budget
            .udp_sessions
            .map(|n| format!(", udp_max_sessions = {n}"))
            .unwrap_or_default();
        warn!(
            "Open-file limit {current} is below what {tcp}{udp} and {} listener(s) can need together ({needed}); connections and UDP sessions will fail with EMFILE before the caps are reached (raise ulimit -n / --ulimit nofile / LimitNOFILE, or lower the caps)",
            budget.listeners
        );
    }
}

#[cfg(not(unix))]
pub fn raise_soft_limit(_budget: Budget) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raises_to_the_hard_limit() {
        // Docker's default: soft 1024, hard 524288.
        assert_eq!(plan(1024, 524_288), Plan::Raise(524_288));
    }

    #[test]
    fn caps_an_unlimited_hard_limit() {
        assert_eq!(plan(1024, u64::MAX), Plan::Raise(SOFT_LIMIT_CAP));
    }

    #[test]
    fn keeps_a_limit_that_is_already_high() {
        assert_eq!(plan(65_536, 65_536), Plan::Keep);
        assert_eq!(plan(1_048_576, u64::MAX), Plan::Keep);
        assert_eq!(plan(2_000_000, u64::MAX), Plan::Keep);
    }

    #[test]
    fn splice_needs_more_descriptors_per_connection() {
        let tcp = |max, splice| Budget {
            max_connections: Some(max),
            splice,
            ..Budget::default()
        };
        assert_eq!(tcp(1000, false).fds(), 2128);
        assert_eq!(tcp(1000, true).fds(), 6128);
        assert_eq!(tcp(usize::MAX, true).fds(), u64::MAX);
    }

    #[test]
    fn tcp_udp_and_listeners_add_up() {
        let b = Budget {
            max_connections: Some(1000),
            splice: false,
            udp_sessions: Some(16384),
            listeners: 4096,
        };
        assert_eq!(b.fds(), 2000 + 16384 + 4096 + BASE_FDS);
        assert_eq!(Budget::default().fds(), BASE_FDS);
    }
}
