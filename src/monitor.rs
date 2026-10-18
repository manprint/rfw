//! Active health monitoring of every forwarder's target.
//!
//! The data path only learns about a dead target when a client connects, and
//! it learns about a DNS change only when the next connection resolves. This
//! monitor closes both gaps for the operator: it periodically resolves the
//! target and opens (then immediately closes) one TCP connection to it, feeds
//! the `target_reachable` gauge, records the observed address set, and raises
//! the alerts:
//!
//! - [`Event::DnsChanged`] as soon as a probe (or a real connection, which
//!   shares [`ForwarderStats::observe_addrs`]) sees a different address set;
//! - [`Event::TargetUnreachable`] once probes have failed continuously for
//!   `unreachable_after`, then exactly one [`Event::TargetRecovered`] when a
//!   probe succeeds again.
//!
//! Probing is traffic-aware: a real connection that just succeeded proves
//! reachability, so the scheduled probe is skipped (a busy forwarder costs the
//! backend no extra connections); a real connection that just failed kicks a
//! probe early, so the downtime clock starts within a second of the first
//! client-visible failure instead of at the next scheduled tick.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::TokioResolver;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{ForwarderConfig, Protocol, RuntimeKnobs};
use crate::forwarder::{connect_racing, resolve_targets};
use crate::notify::{fmt_duration, Event, Notifier, ProbeReport};
use crate::stats::{ForwarderStats, IcmpKind, UdpSample};

/// While the target is down, probe at most this far apart (still bounded by
/// `probe_interval`), so the alert lands close to the configured threshold
/// and recovery is noticed within seconds.
const DOWN_PROBE_INTERVAL: Duration = Duration::from_secs(5);
/// Minimum spacing between a probe and one kicked by a data-path failure, so a
/// burst of failing clients cannot turn into a burst of probes.
const KICK_MIN_SPACING: Duration = Duration::from_secs(1);

/// One resolve-and-connect attempt.
#[derive(Debug)]
pub struct ProbeOutcome {
    /// Addresses the hostname resolved to (empty when resolution failed or
    /// the remote is an IP literal).
    pub addrs: Vec<IpAddr>,
    /// The address that accepted and the connect latency, or the error.
    pub result: Result<(SocketAddr, Duration), String>,
}

/// Resolve the target and open one TCP connection to it, closing it at once.
///
/// Shares the data path's resolver and connect logic on purpose: a probe that
/// succeeds means a client connection would have, with the same DNS policy
/// and the same Happy-Eyeballs racing across every resolved address.
pub async fn probe_target(
    cfg: &ForwarderConfig,
    resolver: &TokioResolver,
    dns_timeout: Duration,
    connect_timeout: Duration,
) -> ProbeOutcome {
    let literal: Option<IpAddr> = cfg.remote_host.parse().ok();
    let targets = match resolve_targets(
        &cfg.remote_host,
        cfg.remote_port,
        literal,
        resolver,
        dns_timeout,
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            return ProbeOutcome {
                addrs: Vec::new(),
                result: Err(e.to_string()),
            }
        }
    };
    let addrs = if literal.is_some() {
        Vec::new()
    } else {
        targets.iter().map(|a| a.ip()).collect()
    };
    let started = Instant::now();
    match connect_racing(&targets, connect_timeout).await {
        Ok((stream, addr)) => {
            // Close cleanly; the backend sees an empty connection. For a
            // database this is at most one "incomplete startup packet" log
            // line per probe.
            drop(stream);
            ProbeOutcome {
                addrs,
                result: Ok((addr, started.elapsed())),
            }
        }
        Err(e) => ProbeOutcome {
            addrs,
            result: Err(e.to_string()),
        },
    }
}

/// Probe every configured target once, concurrently, for the startup report.
pub async fn probe_all(
    configs: &[ForwarderConfig],
    resolver: Arc<TokioResolver>,
    knobs: &RuntimeKnobs,
) -> Vec<ProbeReport> {
    let dns_timeout = knobs.dns_timeout();
    let connect_timeout = knobs.connect_timeout();
    let mut set = tokio::task::JoinSet::new();
    for (i, cfg) in configs.iter().cloned().enumerate() {
        let resolver = resolver.clone();
        set.spawn(async move {
            let outcome = match cfg.protocol {
                Protocol::Tcp => probe_target(&cfg, &resolver, dns_timeout, connect_timeout).await,
                Protocol::Udp => probe_udp_target(&cfg, &resolver, dns_timeout).await,
            };
            (
                i,
                ProbeReport {
                    label: cfg.label(),
                    target: format!("{}:{}", cfg.remote_host, cfg.remote_port),
                    protocol: cfg.protocol,
                    outcome: outcome.result,
                },
            )
        });
    }
    let mut reports = Vec::with_capacity(configs.len());
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(r) => reports.push(r),
            Err(e) => warn!(error = %e, "Startup probe task failed"),
        }
    }
    // Present them in configuration order, not completion order.
    reports.sort_by_key(|(i, _)| *i);
    reports.into_iter().map(|(_, r)| r).collect()
}

/// Continuous downtime bookkeeping.
struct Downtime {
    since: Instant,
    /// Whether `TargetUnreachable` has been raised for this stretch.
    alerted: bool,
}

/// Sleep unless cancelled. Returns `true` if cancelled.
async fn cancelled(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

/// Wait for the next probe tick: the interval (shortened while down), or an
/// early kick from the data path. Returns `true` if cancelled.
async fn wait_for_tick(
    cancel: &CancellationToken,
    stats: &ForwarderStats,
    interval: Duration,
    is_down: bool,
    previous: Instant,
) -> bool {
    let wait = if is_down {
        interval.min(DOWN_PROBE_INTERVAL)
    } else {
        interval
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => return true,
        _ = tokio::time::sleep(wait) => {}
        _ = stats.probe_kick().notified() => {
            // A real connection just failed: probe soon, not in a stampede.
            let spacing = KICK_MIN_SPACING.saturating_sub(previous.elapsed());
            if !spacing.is_zero() && cancelled(cancel, spacing).await {
                return true;
            }
        }
    }
    false
}

/// Run the monitor for one forwarder until `cancel` fires.
///
/// The first probe runs immediately so the `target_reachable` gauge is
/// populated at startup; later probes follow `knobs.probe_interval()`, read
/// live so a hot-reload takes effect on the next tick.
pub async fn run_monitor(
    cfg: Arc<ForwarderConfig>,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    notifier: Notifier,
) {
    if cfg.protocol == Protocol::Udp {
        return run_udp_monitor(cfg, cancel, stats, resolver, knobs, notifier).await;
    }
    let label = cfg.label();
    let target = format!("{}:{}", cfg.remote_host, cfg.remote_port);
    let mut down: Option<Downtime> = None;
    let mut last_probe: Option<Instant> = None;

    loop {
        let interval = knobs.probe_interval();
        if let Some(previous) = last_probe {
            if wait_for_tick(&cancel, &stats, interval, down.is_some(), previous).await {
                return;
            }
        }
        if cancel.is_cancelled() {
            return;
        }
        last_probe = Some(Instant::now());

        // A connection that succeeded within the last interval proves the
        // target reachable; skip the active probe while healthy.
        let recent_ok = stats
            .last_connect_ok_age()
            .is_some_and(|age| age < interval);
        let outcome = if recent_ok && down.is_none() {
            None
        } else {
            Some(
                probe_target(
                    &cfg,
                    &resolver,
                    knobs.dns_timeout(),
                    knobs.connect_timeout(),
                )
                .await,
            )
        };

        if let Some(o) = &outcome {
            if !o.addrs.is_empty() {
                report_dns_change(&label, &cfg.remote_host, &o.addrs, &stats, &notifier);
            }
        }

        let result: Result<Option<SocketAddr>, String> = match outcome {
            None => Ok(None),
            Some(o) => o.result.map(|(addr, _)| Some(addr)),
        };

        match result {
            Ok(address) => record_up(&label, &target, &mut down, address, &stats, &notifier),
            Err(e) => record_down(&label, &target, &mut down, e, &stats, &knobs, &notifier),
        }
    }
}

/// Report a changed DNS answer, if `addrs` differs from the last observed set.
fn report_dns_change(
    label: &str,
    host: &str,
    addrs: &[IpAddr],
    stats: &ForwarderStats,
    notifier: &Notifier,
) {
    if let Some(old) = stats.observe_addrs(addrs) {
        let mut new = addrs.to_vec();
        new.sort_unstable();
        new.dedup();
        info!(
            forwarder = %label,
            host = %host,
            old = ?old,
            new = ?new,
            "Target DNS answer changed"
        );
        notifier.notify(Event::DnsChanged {
            label: label.to_string(),
            host: host.to_string(),
            old,
            new,
        });
    }
}

/// The target looks reachable: set the gauge and close any open outage.
fn record_up(
    label: &str,
    target: &str,
    down: &mut Option<Downtime>,
    address: Option<SocketAddr>,
    stats: &ForwarderStats,
    notifier: &Notifier,
) {
    stats.set_target_reachable(true);
    if let Some(d) = down.take() {
        let down_for = d.since.elapsed();
        if d.alerted {
            info!(
                forwarder = %label,
                down_for = %fmt_duration(down_for),
                "Target reachable again"
            );
            notifier.notify(Event::TargetRecovered {
                label: label.to_string(),
                target: target.to_string(),
                down_for,
                address,
            });
        } else {
            info!(
                forwarder = %label,
                down_for = %fmt_duration(down_for),
                "Target reachable again (outage stayed below the alert threshold)"
            );
        }
    }
}

/// The target looks unreachable: set the gauge, start or extend the outage,
/// and alert once it has lasted `unreachable_after`.
fn record_down(
    label: &str,
    target: &str,
    down: &mut Option<Downtime>,
    error: String,
    stats: &ForwarderStats,
    knobs: &RuntimeKnobs,
    notifier: &Notifier,
) {
    stats.set_target_reachable(false);
    let threshold = knobs.unreachable_after();
    let d = down.get_or_insert_with(|| {
        warn!(forwarder = %label, error = %error, "Target probe failed; watching");
        Downtime {
            since: Instant::now(),
            alerted: false,
        }
    });
    let down_for = d.since.elapsed();
    if !d.alerted && down_for >= threshold {
        d.alerted = true;
        error!(
            forwarder = %label,
            down_for = %fmt_duration(down_for),
            error = %error,
            "Target unreachable beyond the alert threshold"
        );
        notifier.notify(Event::TargetUnreachable {
            label: label.to_string(),
            target: target.to_string(),
            down_for,
            last_error: error,
        });
    }
}

/// An ICMP error older than this, with traffic still flowing and no newer
/// error, no longer counts: the kernel would have reported a fresh one (ICMP is
/// rate-limited to about 1/s per destination).
const ICMP_QUIET_MS: u64 = 3_000;

/// Passive verdict on a UDP target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpVerdict {
    Up,
    Down {
        kind: IcmpKind,
        age_ms: u64,
    },
    /// No new evidence either way: keep the current state.
    Hold,
}

/// Decide from the data-path evidence alone; nothing is ever sent to the target.
/// Rows are evaluated in order. Recovery needs positive evidence (a reply, or
/// traffic that no longer draws ICMP errors); silence freezes the state.
fn udp_verdict(cur: &UdpSample, prev: Option<&UdpSample>) -> UdpVerdict {
    let Some((icmp_ms, kind)) = cur.icmp else {
        return UdpVerdict::Up;
    };
    if cur.reply_ms.is_some_and(|r| r >= icmp_ms) {
        return UdpVerdict::Up;
    }
    let down = UdpVerdict::Down {
        kind,
        age_ms: cur.now_ms.saturating_sub(icmp_ms),
    };
    let Some(prev) = prev else {
        return down;
    };
    if prev.icmp.map(|(ms, _)| ms) != Some(icmp_ms) {
        return down;
    }
    if cur.now_ms.saturating_sub(icmp_ms) < ICMP_QUIET_MS {
        return down;
    }
    if cur.sent > prev.sent {
        return UdpVerdict::Up;
    }
    UdpVerdict::Hold
}

/// Resolve the UDP target (DNS only; UDP has no handshake to probe).
pub async fn probe_udp_target(
    cfg: &ForwarderConfig,
    resolver: &TokioResolver,
    dns_timeout: Duration,
) -> ProbeOutcome {
    let literal: Option<IpAddr> = cfg.remote_host.parse().ok();
    let started = Instant::now();
    let targets = match resolve_targets(
        &cfg.remote_host,
        cfg.remote_port,
        literal,
        resolver,
        dns_timeout,
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            return ProbeOutcome {
                addrs: Vec::new(),
                result: Err(e.to_string()),
            }
        }
    };
    let elapsed = started.elapsed();
    let addrs = if literal.is_some() {
        Vec::new()
    } else {
        targets.iter().map(|a| a.ip()).collect()
    };
    match targets.first() {
        Some(first) => ProbeOutcome {
            addrs,
            result: Ok((*first, elapsed)),
        },
        None => ProbeOutcome {
            addrs,
            result: Err("no address resolved".to_string()),
        },
    }
}

/// Passive monitor for a UDP forwarder: DNS observation plus [`udp_verdict`].
async fn run_udp_monitor(
    cfg: Arc<ForwarderConfig>,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    notifier: Notifier,
) {
    let label = cfg.label();
    let target = format!("{}:{}", cfg.remote_host, cfg.remote_port);
    let mut down: Option<Downtime> = None;
    let mut last_probe: Option<Instant> = None;
    let mut prev: Option<UdpSample> = None;

    loop {
        let interval = knobs.probe_interval();
        if let Some(previous) = last_probe {
            if wait_for_tick(&cancel, &stats, interval, down.is_some(), previous).await {
                return;
            }
        }
        if cancel.is_cancelled() {
            return;
        }
        last_probe = Some(Instant::now());

        let o = probe_udp_target(&cfg, &resolver, knobs.dns_timeout()).await;
        if !o.addrs.is_empty() {
            report_dns_change(&label, &cfg.remote_host, &o.addrs, &stats, &notifier);
        }
        let cur = stats.udp_sample();
        match o.result {
            Err(e) => record_down(&label, &target, &mut down, e, &stats, &knobs, &notifier),
            Ok((addr, _)) => match udp_verdict(&cur, prev.as_ref()) {
                UdpVerdict::Up => {
                    record_up(&label, &target, &mut down, Some(addr), &stats, &notifier)
                }
                UdpVerdict::Down { kind, age_ms } => {
                    let error = format!(
                        "{} from the target {} ago; no reply since",
                        kind.as_str(),
                        fmt_duration(Duration::from_millis(age_ms))
                    );
                    record_down(&label, &target, &mut down, error, &stats, &knobs, &notifier)
                }
                UdpVerdict::Hold => {}
            },
        }
        prev = Some(cur);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use crate::config::Settings;
    use crate::testutil::{resolver_for, spawn_fake_dns};

    fn cfg(remote_host: &str, remote_port: u16) -> Arc<ForwarderConfig> {
        Arc::new(ForwarderConfig {
            local_host: "127.0.0.1".into(),
            local_port: 1,
            remote_host: remote_host.into(),
            remote_port,
            protocol: Protocol::Tcp,
        })
    }

    fn knobs(probe_interval_secs: u64, unreachable_after_secs: u64) -> Arc<RuntimeKnobs> {
        Arc::new(RuntimeKnobs::from_settings(&Settings {
            probe_interval_secs,
            unreachable_after_secs,
            connect_timeout_secs: 2,
            dns_timeout_secs: 2,
            ..Settings::default()
        }))
    }

    fn system_resolver() -> Arc<TokioResolver> {
        Arc::new(TokioResolver::builder_tokio().unwrap().build().unwrap())
    }

    /// Backend that accepts and counts connections, then closes them.
    async fn counting_backend(listener: TcpListener) -> Arc<AtomicUsize> {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                c.fetch_add(1, Ordering::Relaxed);
                drop(sock);
            }
        });
        count
    }

    async fn next_event(rx: &mut mpsc::Receiver<Event>, within: Duration) -> Event {
        tokio::time::timeout(within, rx.recv())
            .await
            .expect("event must arrive in time")
            .expect("channel open")
    }

    async fn wait_reachable(stats: &ForwarderStats, want: Option<bool>) {
        for _ in 0..200 {
            if stats.target_reachable() == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("target_reachable never became {want:?}");
    }

    #[tokio::test]
    async fn probe_reports_the_answering_address_and_latency() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let _count = counting_backend(l).await;
        let out = probe_target(
            &cfg("127.0.0.1", port),
            &system_resolver(),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        let (addr, latency) = out.result.expect("live backend");
        assert_eq!(addr.port(), port);
        assert!(latency < Duration::from_secs(1));
        assert!(out.addrs.is_empty(), "IP literal: no DNS observation");
    }

    #[tokio::test]
    async fn probe_fails_fast_against_a_closed_port() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let t0 = Instant::now();
        let out = probe_target(
            &cfg("127.0.0.1", port),
            &system_resolver(),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        let err = out.result.expect_err("nothing listens");
        assert!(err.contains("connect"), "{err}");
        assert!(t0.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn probe_reports_dns_failures_as_errors() {
        let answer = Arc::new(AtomicU32::new(0));
        let dns = spawn_fake_dns(answer, Arc::new(AtomicUsize::new(0)), 1).await;
        // A fake resolver that never gets asked: point at a closed UDP port so
        // the lookup times out.
        let dead = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        let _ = dns;
        let resolver = resolver_for(dead_addr);
        let out = probe_target(
            &cfg("db.test", 5432),
            &resolver,
            Duration::from_millis(500),
            Duration::from_secs(1),
        )
        .await;
        let err = out.result.expect_err("DNS must fail");
        assert!(
            err.to_lowercase().contains("dns") || err.to_lowercase().contains("resol"),
            "{err}"
        );
        assert!(out.addrs.is_empty());
    }

    #[tokio::test]
    async fn probe_all_keeps_configuration_order() {
        let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pa = a.local_addr().unwrap().port();
        let _ca = counting_backend(a).await;
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pd = dead.local_addr().unwrap().port();
        drop(dead);

        let configs = vec![
            ForwarderConfig {
                local_host: "127.0.0.1".into(),
                local_port: 1,
                remote_host: "127.0.0.1".into(),
                remote_port: pd,
                protocol: Protocol::Tcp,
            },
            ForwarderConfig {
                local_host: "127.0.0.1".into(),
                local_port: 2,
                remote_host: "127.0.0.1".into(),
                remote_port: pa,
                protocol: Protocol::Tcp,
            },
        ];
        let reports = probe_all(&configs, system_resolver(), &knobs(30, 300)).await;
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].label, configs[0].label());
        assert!(reports[0].outcome.is_err());
        assert_eq!(reports[1].label, configs[1].label());
        assert!(reports[1].outcome.is_ok());
        assert_eq!(reports[1].target, format!("127.0.0.1:{pa}"));
    }

    #[tokio::test]
    async fn outage_beyond_threshold_alerts_once_and_recovery_follows() {
        // A listener the test controls: accept the first probe by hand, then
        // drop it to take the target down, then rebind to bring it back.
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let cfg = cfg("127.0.0.1", port);
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (notifier, mut rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs(1, 1),
            notifier,
        ));
        let (sock, _) = l.accept().await.unwrap();
        drop(sock);
        wait_reachable(&stats, Some(true)).await;
        drop(l);

        // Threshold is 1s and the down-probe cadence is 1s: the alert must
        // land within a few seconds, exactly once.
        let ev = next_event(&mut rx, Duration::from_secs(8)).await;
        match &ev {
            Event::TargetUnreachable {
                label,
                target,
                down_for,
                last_error,
            } => {
                assert_eq!(label, &cfg.label());
                assert_eq!(target, &format!("127.0.0.1:{port}"));
                assert!(*down_for >= Duration::from_secs(1));
                assert!(last_error.contains("connect"), "{last_error}");
            }
            other => panic!("expected TargetUnreachable, got {other:?}"),
        }
        assert_eq!(stats.target_reachable(), Some(false));
        assert!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .is_err(),
            "must not re-alert while still down"
        );

        // Bring the backend back on the same port.
        let l = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let _count = counting_backend(l).await;
        let ev = next_event(&mut rx, Duration::from_secs(8)).await;
        match &ev {
            Event::TargetRecovered {
                label,
                down_for,
                address,
                ..
            } => {
                assert_eq!(label, &cfg.label());
                assert!(*down_for >= Duration::from_secs(1));
                assert_eq!(address.map(|a| a.port()), Some(port));
            }
            other => panic!("expected TargetRecovered, got {other:?}"),
        }
        assert_eq!(stats.target_reachable(), Some(true));
        cancel.cancel();
    }

    #[tokio::test]
    async fn short_blip_below_threshold_does_not_alert() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let cfg = cfg("127.0.0.1", port);
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (notifier, mut rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs(1, 30),
            notifier,
        ));
        let (sock, _) = l.accept().await.unwrap();
        drop(sock);
        wait_reachable(&stats, Some(true)).await;

        drop(l); // ~2s blip
        wait_reachable(&stats, Some(false)).await;
        let l = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let _count = counting_backend(l).await;
        wait_reachable(&stats, Some(true)).await;

        assert!(
            tokio::time::timeout(Duration::from_millis(500), rx.recv())
                .await
                .is_err(),
            "a blip shorter than unreachable_after must stay silent"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn recent_data_path_success_skips_the_active_probe() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let count = counting_backend(l).await;
        let cfg = cfg("127.0.0.1", port);
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (notifier, _rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs(1, 30),
            notifier,
        ));
        wait_reachable(&stats, Some(true)).await;
        assert_eq!(count.load(Ordering::Relaxed), 1, "initial probe");

        // Pretend clients keep connecting successfully.
        for _ in 0..12 {
            stats.mark_connect_ok();
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert_eq!(
            count.load(Ordering::Relaxed),
            1,
            "a busy, healthy forwarder must not cost the backend probe connections"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn data_path_failure_kicks_an_early_probe() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let count = counting_backend(l).await;
        let cfg = cfg("127.0.0.1", port);
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (notifier, _rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs(600, 30), // next scheduled probe is 10 minutes away
            notifier,
        ));
        wait_reachable(&stats, Some(true)).await;
        assert_eq!(count.load(Ordering::Relaxed), 1);

        tokio::time::sleep(Duration::from_millis(1100)).await; // past KICK_MIN_SPACING
        stats.mark_connect_failed();
        for _ in 0..80 {
            if count.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            count.load(Ordering::Relaxed),
            2,
            "a client-visible failure must trigger a probe without waiting for the interval"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn dns_change_is_reported_with_old_and_new_sets() {
        let answer = Arc::new(AtomicU32::new(u32::from(Ipv4Addr::new(127, 0, 0, 1))));
        let a_queries = Arc::new(AtomicUsize::new(0));
        let dns = spawn_fake_dns(answer.clone(), a_queries, 1).await;
        let resolver = Arc::new(resolver_for(dns));

        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let _count = counting_backend(l).await;

        let cfg = cfg("db.test", port);
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (notifier, mut rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            resolver,
            knobs(1, 300),
            notifier,
        ));
        wait_reachable(&stats, Some(true)).await;
        assert_eq!(
            stats.last_addrs(),
            Some(vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))])
        );

        // Flip the record. 10.255.0.1 is not routable here, so the probe will
        // fail, but the DNS change must be reported regardless of reachability.
        answer.store(u32::from(Ipv4Addr::new(10, 255, 0, 1)), Ordering::Relaxed);
        let ev = next_event(&mut rx, Duration::from_secs(8)).await;
        match ev {
            Event::DnsChanged {
                label,
                host,
                old,
                new,
            } => {
                assert_eq!(label, cfg.label());
                assert_eq!(host, "db.test");
                assert_eq!(old, vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]);
                assert_eq!(new, vec![IpAddr::V4(Ipv4Addr::new(10, 255, 0, 1))]);
            }
            other => panic!("expected DnsChanged, got {other:?}"),
        }
        cancel.cancel();
    }

    #[tokio::test]
    async fn monitor_stops_on_cancel() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let _count = counting_backend(l).await;
        let cfg = cfg("127.0.0.1", port);
        let stats = Arc::new(ForwarderStats::new(cfg.label()));
        let (notifier, _rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_monitor(
            cfg,
            cancel.clone(),
            stats,
            system_resolver(),
            knobs(600, 300),
            notifier,
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("monitor must exit promptly on cancel")
            .unwrap();
    }

    fn udp_cfg(remote_port: u16) -> Arc<ForwarderConfig> {
        Arc::new(ForwarderConfig {
            protocol: Protocol::Udp,
            ..(*cfg("127.0.0.1", remote_port)).clone()
        })
    }

    fn sample(now_ms: u64, icmp_ms: Option<u64>, reply_ms: Option<u64>, sent: u64) -> UdpSample {
        UdpSample {
            now_ms,
            icmp: icmp_ms.map(|ms| (ms, IcmpKind::PortUnreachable)),
            reply_ms,
            sent,
        }
    }

    #[test]
    fn udp_verdict_truth_table() {
        let down = |age_ms| UdpVerdict::Down {
            kind: IcmpKind::PortUnreachable,
            age_ms,
        };
        // 1: no ICMP at all.
        assert_eq!(
            udp_verdict(&sample(10_000, None, None, 5), None),
            UdpVerdict::Up
        );
        // 2: a reply at or after the ICMP error.
        assert_eq!(
            udp_verdict(&sample(10_000, Some(1_000), Some(1_000), 5), None),
            UdpVerdict::Up
        );
        assert_eq!(
            udp_verdict(&sample(10_000, Some(1_000), Some(2_000), 5), None),
            UdpVerdict::Up
        );
        // 3: no previous tick.
        assert_eq!(
            udp_verdict(&sample(10_000, Some(1_000), Some(500), 5), None),
            down(9_000)
        );
        // 4: an ICMP error newer than the previous tick saw.
        let prev = sample(9_000, Some(1_000), None, 5);
        assert_eq!(
            udp_verdict(&sample(10_000, Some(9_500), None, 5), Some(&prev)),
            down(500)
        );
        let prev_none = sample(9_000, None, None, 5);
        assert_eq!(
            udp_verdict(&sample(10_000, Some(1_000), None, 9), Some(&prev_none)),
            down(9_000)
        );
        // 5: same ICMP error, still younger than the quiet period.
        let prev = sample(3_000, Some(1_000), None, 5);
        assert_eq!(
            udp_verdict(&sample(3_999, Some(1_000), None, 9), Some(&prev)),
            down(2_999)
        );
        // 6: old error, more traffic since the last tick and no new error: Up
        // (the boundary age == ICMP_QUIET_MS is included).
        let prev = sample(3_000, Some(1_000), None, 5);
        assert_eq!(
            udp_verdict(&sample(4_000, Some(1_000), None, 6), Some(&prev)),
            UdpVerdict::Up
        );
        assert_eq!(
            udp_verdict(&sample(10_000, Some(1_000), None, 6), Some(&prev)),
            UdpVerdict::Up
        );
        // 7: old error, no traffic: freeze.
        assert_eq!(
            udp_verdict(&sample(10_000, Some(1_000), None, 5), Some(&prev)),
            UdpVerdict::Hold
        );
        // Order: row 2 beats row 4 (new ICMP, but a newer reply).
        let prev = sample(9_000, Some(1_000), None, 5);
        assert_eq!(
            udp_verdict(&sample(10_000, Some(9_500), Some(9_600), 5), Some(&prev)),
            UdpVerdict::Up
        );
        // Order: row 4 beats row 6 (new ICMP even though traffic grew).
        assert_eq!(
            udp_verdict(&sample(10_000, Some(9_500), None, 50), Some(&prev)),
            down(500)
        );
    }

    #[tokio::test]
    async fn udp_monitor_alerts_on_icmp_and_recovers_on_reply() {
        let cfg = udp_cfg(9);
        let stats = Arc::new(ForwarderStats::with_protocol(cfg.label(), Protocol::Udp));
        let (notifier, mut rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs(1, 1),
            notifier,
        ));
        wait_reachable(&stats, Some(true)).await;

        let alert = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                tokio::select! {
                    ev = rx.recv() => break ev.expect("channel open"),
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {
                        stats.mark_udp_icmp_error(stats.now_ms(), IcmpKind::PortUnreachable);
                    }
                }
            }
        })
        .await
        .expect("alert must arrive in time");
        match alert {
            Event::TargetUnreachable { last_error, .. } => {
                assert!(
                    last_error.contains("port unreachable from the target"),
                    "{last_error}"
                );
            }
            other => panic!("expected TargetUnreachable, got {other:?}"),
        }

        stats.mark_connect_ok_at(stats.now_ms());
        match next_event(&mut rx, Duration::from_secs(5)).await {
            Event::TargetRecovered { .. } => {}
            other => panic!("expected TargetRecovered, got {other:?}"),
        }
        cancel.cancel();
    }

    #[tokio::test]
    async fn udp_monitor_freezes_without_new_evidence() {
        let cfg = udp_cfg(9);
        let stats = Arc::new(ForwarderStats::with_protocol(cfg.label(), Protocol::Udp));
        let (notifier, mut rx) = Notifier::for_test();
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(run_monitor(
            cfg.clone(),
            cancel.clone(),
            stats.clone(),
            system_resolver(),
            knobs(1, 10),
            notifier,
        ));
        wait_reachable(&stats, Some(true)).await;

        stats.mark_udp_icmp_error(stats.now_ms(), IcmpKind::PortUnreachable);
        let t0 = Instant::now();
        while stats.target_reachable() != Some(false) {
            assert!(
                t0.elapsed() < Duration::from_secs(3),
                "the ICMP error must flip the target to down"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // Hold after the quiet period: the 10 s threshold is never evaluated.
        assert!(
            tokio::time::timeout(Duration::from_secs(6), rx.recv())
                .await
                .is_err(),
            "no alert without fresh evidence"
        );
        assert_eq!(stats.target_reachable(), Some(false));
        cancel.cancel();
    }

    #[tokio::test]
    async fn udp_probe_is_dns_only() {
        let c = (*udp_cfg(9)).clone();
        let reports = probe_all(&[c], system_resolver(), &knobs(30, 300)).await;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].protocol, Protocol::Udp);
        let (addr, _) = reports[0].outcome.as_ref().expect("DNS-only probe");
        assert_eq!(*addr, "127.0.0.1:9".parse::<SocketAddr>().unwrap());
    }
}
