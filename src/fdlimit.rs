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

/// Descriptors `max_connections` simultaneous connections can need.
fn fds_for(max_connections: u64, splice: bool) -> u64 {
    max_connections
        .saturating_mul(if splice { 6 } else { 2 })
        .saturating_add(BASE_FDS)
}

/// Raise the soft open-file limit to the hard limit and warn when a configured
/// `max_connections` still cannot fit. Failures are logged, never fatal.
#[cfg(unix)]
pub fn raise_soft_limit(max_connections: Option<usize>, splice: bool) {
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
    if let Some(max) = max_connections {
        let needed = fds_for(max as u64, splice);
        if current < needed {
            warn!(
                "Open-file limit {current} is below what max_connections = {max} can need ({needed}); connections will be refused with EMFILE before the cap is reached (raise ulimit -n / --ulimit nofile / LimitNOFILE, or lower max_connections)"
            );
        }
    }
}

#[cfg(not(unix))]
pub fn raise_soft_limit(_max_connections: Option<usize>, _splice: bool) {}

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
        assert_eq!(fds_for(1000, false), 2128);
        assert_eq!(fds_for(1000, true), 6128);
        assert_eq!(fds_for(u64::MAX, true), u64::MAX);
    }
}
