//! UDP forwarding against the real binary: round trips with real counters,
//! sessions (cap, expiry, per-client isolation), hot reload, the shared TCP/UDP
//! port, health, passive monitoring, log rate limiting, SIGTERM and the
//! reference scenario. `udp_bench` is `#[ignore]` and run by its own gate.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    free_dual_port, free_port, free_udp_port, http_get, rfw_command, spawn_echo, spawn_rfw,
    spawn_rfw_logged, spawn_udp_echo, spawn_udp_echo_on, spawn_udp_tagged, stat_of, udp_client,
    udp_roundtrip, wait_for_stats, wait_port, wait_udp_forwarding, wait_udp_port_free, TempConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn udp_label(local: u16, remote: u16) -> String {
    format!("127.0.0.1:{local}->127.0.0.1:{remote}/udp")
}

/// Poll `/metrics` every 100ms until it contains `needle`, or panic after `within`.
async fn wait_metrics_contains(metrics: u16, needle: &str, within: Duration) -> String {
    let deadline = Instant::now() + within;
    loop {
        let body = http_get(metrics, "/metrics").await;
        if body.contains(needle) {
            return body;
        }
        if Instant::now() >= deadline {
            panic!("/metrics never contained {needle:?}; last body: {body}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `/stats` until `field` of `label` satisfies `pred`; returns the value.
async fn wait_stat(
    metrics: u16,
    label: &str,
    field: &str,
    within: Duration,
    pred: impl Fn(u64) -> bool,
) -> u64 {
    let deadline = Instant::now() + within;
    loop {
        let body = http_get(metrics, "/stats").await;
        if let Some(v) = stat_of(&body, label, field) {
            if pred(v) {
                return v;
            }
        }
        if Instant::now() >= deadline {
            panic!("stat {field} of {label} never satisfied the predicate; last body: {body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Send `payload` every 200ms from `client` until a reply starting with `want`
/// arrives, or fail after `within`. Stale replies from the previous backend are skipped.
async fn wait_reply_starting_with(
    client: &tokio::net::UdpSocket,
    port: u16,
    want: &[u8],
    within: Duration,
) -> Vec<u8> {
    let deadline = Instant::now() + within;
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(r) = udp_roundtrip(client, port, b"x", Duration::from_millis(200)).await {
            if r.starts_with(want) {
                return r;
            }
            last = Some(r);
        }
    }
    panic!("never got a reply starting with {want:?}; last reply {last:?}");
}

#[tokio::test]
async fn udp_round_trip_and_metrics() {
    let echo = spawn_udp_echo().await;
    let (p, m) = (free_udp_port(), free_port());
    let label = udp_label(p, echo);
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let metrics_addr = format!("127.0.0.1:{m}");
    let _rfw = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(m).await;

    let client = udp_client().await;
    wait_udp_forwarding(&client, p).await;
    let n0 = stat_of(&http_get(m, "/stats").await, &label, "datagrams_sent").expect("stat");

    for i in 0..10_u32 {
        let payload = format!("datagram-{i}-{}", "x".repeat(i as usize * 7));
        let got = udp_roundtrip(&client, p, payload.as_bytes(), Duration::from_secs(2))
            .await
            .expect("reply");
        assert_eq!(got, payload.as_bytes(), "reply {i} must be identical");
    }

    wait_stat(m, &label, "datagrams_sent", Duration::from_secs(3), |v| {
        v == n0 + 10
    })
    .await;
    let stats = http_get(m, "/stats").await;
    assert!(
        stat_of(&stats, &label, "datagrams_received").expect("received") >= 10,
        "{stats}"
    );
    assert_eq!(
        stat_of(&stats, &label, "datagrams_dropped"),
        Some(0),
        "{stats}"
    );
    assert!(stats.contains("\"protocol\":\"udp\""), "{stats}");

    let metrics = http_get(m, "/metrics").await;
    assert!(
        metrics.contains(&format!("rfw_datagrams_sent_total{{label=\"{label}\"}}")),
        "{metrics}"
    );
    assert!(
        metrics.contains(&format!("rfw_listening{{label=\"{label}\"}} 1")),
        "{metrics}"
    );
}

#[tokio::test]
async fn env_udp_forwarder_works() {
    let echo = spawn_udp_echo().await;
    let p = free_udp_port();
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let (_rfw, _log) = spawn_rfw_logged(&[], &[("RFW_FORWARDER_1", &fwd)]);
    let client = udp_client().await;
    assert_eq!(wait_udp_forwarding(&client, p).await, b"ping");
}

fn udp_yaml(local: u16, remote: u16) -> String {
    format!(
        "forwarders:\n  - {{local_host: \"127.0.0.1\", local_port: {local}, remote_host: \"127.0.0.1\", remote_port: {remote}, protocol: udp}}\n"
    )
}

/// A `protocol: udp` YAML entry works, and re-targeting it on a live reload
/// moves the same client to the new backend.
#[tokio::test]
async fn yaml_protocol_udp_and_hot_reload_retarget() {
    let a = spawn_udp_tagged(b"A").await;
    let b = spawn_udp_tagged(b"B").await;
    let p = free_udp_port();
    let cfg = TempConfig::new("udp-retarget", &udp_yaml(p, a));
    let _rfw = spawn_rfw(&["-f", &cfg.path_str()]);

    let client = udp_client().await;
    let first = wait_udp_forwarding(&client, p).await;
    assert!(first.starts_with(b"A"), "{first:?}");

    cfg.rewrite(&udp_yaml(p, b));
    wait_reply_starting_with(&client, p, b"B", Duration::from_secs(15)).await;
}

fn dual_yaml(port: u16, tcp_backend: u16, udp_backend: Option<u16>) -> String {
    let mut y = format!(
        "forwarders:\n  - {{local_host: \"127.0.0.1\", local_port: {port}, remote_host: \"127.0.0.1\", remote_port: {tcp_backend}}}\n"
    );
    if let Some(u) = udp_backend {
        y.push_str(&format!(
            "  - {{local_host: \"127.0.0.1\", local_port: {port}, remote_host: \"127.0.0.1\", remote_port: {u}, protocol: udp}}\n"
        ));
    }
    y
}

async fn tcp_echo_once(port: u16) {
    let mut c = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    c.write_all(b"tcp-echo").await.unwrap();
    let mut buf = [0_u8; 8];
    tokio::time::timeout(Duration::from_secs(3), c.read_exact(&mut buf))
        .await
        .expect("tcp echo in time")
        .expect("tcp echo read");
    assert_eq!(&buf, b"tcp-echo");
}

#[tokio::test]
async fn tcp_and_udp_share_a_port_independently() {
    let p = free_dual_port();
    let m = free_port();
    let tcp_backend = spawn_echo().await;
    let udp_backend = spawn_udp_tagged(b"U").await;
    let udp_label = udp_label(p, udp_backend);
    let cfg = TempConfig::new("dual", &dual_yaml(p, tcp_backend, Some(udp_backend)));
    let metrics_addr = format!("127.0.0.1:{m}");
    let _rfw = spawn_rfw(&["-f", &cfg.path_str(), "--metrics-addr", &metrics_addr]);
    wait_port(p).await;
    wait_port(m).await;

    tcp_echo_once(p).await;
    let client = udp_client().await;
    let reply = wait_udp_forwarding(&client, p).await;
    assert!(reply.starts_with(b"U"), "{reply:?}");

    cfg.rewrite(&dual_yaml(p, tcp_backend, None));
    wait_for_stats(m, 100, |b| !b.contains(&udp_label)).await;
    wait_udp_port_free(p).await;
    tcp_echo_once(p).await;
}

#[test]
fn duplicate_udp_forwarder_is_rejected() {
    let p = free_udp_port();
    let a = format!("127.0.0.1:{p}:127.0.0.1:1/udp");
    let b = format!("127.0.0.1:{p}:127.0.0.1:2/udp");
    let mut cmd = rfw_command(&[&a, &b]);
    for (key, _) in std::env::vars() {
        if key.starts_with("RFW_FORWARDER_") {
            cmd.env_remove(key);
        }
    }
    let out = cmd
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run rfw");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains(&format!("Duplicate local address '127.0.0.1:{p}/udp'")),
        "{err}"
    );
}

#[tokio::test]
async fn idle_session_expires() {
    let echo = spawn_udp_echo().await;
    let (p, m) = (free_udp_port(), free_port());
    let label = udp_label(p, echo);
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let metrics_addr = format!("127.0.0.1:{m}");
    let _rfw = spawn_rfw(&[
        "--udp-session-timeout",
        "1",
        "--metrics-addr",
        &metrics_addr,
        &fwd,
    ]);
    wait_port(m).await;

    let client = udp_client().await;
    wait_udp_forwarding(&client, p).await;
    wait_stat(
        m,
        &label,
        "active_connections",
        Duration::from_secs(1),
        |v| v == 1,
    )
    .await;
    wait_stat(
        m,
        &label,
        "active_connections",
        Duration::from_secs(5),
        |v| v == 0,
    )
    .await;
}

#[tokio::test]
async fn udp_max_sessions_is_enforced_and_counted() {
    let echo = spawn_udp_echo().await;
    let (p, m) = (free_udp_port(), free_port());
    let label = udp_label(p, echo);
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let metrics_addr = format!("127.0.0.1:{m}");
    let _rfw = spawn_rfw(&[
        "--udp-max-sessions",
        "1",
        "--metrics-addr",
        &metrics_addr,
        &fwd,
    ]);
    wait_port(m).await;

    let a = udp_client().await;
    wait_udp_forwarding(&a, p).await;

    let b = udp_client().await;
    assert!(
        udp_roundtrip(&b, p, b"from-b", Duration::from_secs(1))
            .await
            .is_none(),
        "a second client must be refused while the cap is full"
    );
    wait_stat(
        m,
        &label,
        "rejected_connections",
        Duration::from_secs(3),
        |v| v >= 1,
    )
    .await;
    assert_eq!(
        udp_roundtrip(&a, p, b"again", Duration::from_secs(2))
            .await
            .as_deref(),
        Some(&b"again"[..]),
        "the admitted client keeps working"
    );
}

#[tokio::test]
async fn health_reports_503_while_the_udp_port_is_taken() {
    let echo = spawn_udp_echo().await;
    let (p, m) = (free_udp_port(), free_port());
    let label = udp_label(p, echo);
    let occupant = std::net::UdpSocket::bind(("127.0.0.1", p)).expect("occupy the port");
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let metrics_addr = format!("127.0.0.1:{m}");
    let _rfw = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(m).await;

    let root = http_get(m, "/").await;
    assert!(root.contains("503"), "must be degraded: {root}");
    assert!(root.contains(&label), "{root}");

    drop(occupant);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if http_get(m, "/").await.contains("200 OK") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "must turn healthy once the port is free"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_exits_fast_with_idle_udp_sessions() {
    let echo = spawn_udp_echo().await;
    let (p, m) = (free_udp_port(), free_port());
    let label = udp_label(p, echo);
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let metrics_addr = format!("127.0.0.1:{m}");
    let mut guard = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(m).await;

    let first = udp_client().await;
    wait_udp_forwarding(&first, p).await;
    let mut clients = vec![first];
    for i in 1..50 {
        let c = udp_client().await;
        let got = udp_roundtrip(&c, p, format!("c{i}").as_bytes(), Duration::from_secs(2)).await;
        assert!(got.is_some(), "client {i} must get a reply");
        clients.push(c);
    }
    wait_stat(
        m,
        &label,
        "active_connections",
        Duration::from_secs(3),
        |v| v == 50,
    )
    .await;

    let exited =
        tokio::task::spawn_blocking(move || guard.terminate_and_wait(Duration::from_secs(3)))
            .await
            .unwrap();
    assert!(
        exited,
        "rfw must exit within 3 s of SIGTERM with 50 idle sessions"
    );
    drop(clients);
}

#[tokio::test]
async fn udp_drops_are_rate_limited_in_logs() {
    // (a) session cap: one admitted client, then a burst of new ones.
    {
        let echo = spawn_udp_echo().await;
        let p = free_udp_port();
        let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
        let (_rfw, log) = spawn_rfw_logged(&["--udp-max-sessions", "1", &fwd], &[]);
        let a = udp_client().await;
        wait_udp_forwarding(&a, p).await;

        let mut clients = Vec::new();
        for i in 0..50_u32 {
            let c = udp_client().await;
            let _ = c
                .send_to(format!("burst-{i}").as_bytes(), ("127.0.0.1", p))
                .await;
            clients.push(c);
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        assert!(
            log.wait_for("udp_max_sessions saturated", Duration::from_secs(3))
                .await
        );
        assert_eq!(
            log.count("udp_max_sessions saturated"),
            1,
            "{:#?}",
            log.lines()
        );
        assert_eq!(log.count("UDP session rejected"), 0, "debug-level only");
    }
    // (b) unresolvable remote: a burst of datagrams.
    {
        let p = free_udp_port();
        let fwd = format!("127.0.0.1:{p}:nonexistent.invalid:9/udp");
        let (_rfw, log) = spawn_rfw_logged(&[&fwd], &[]);
        assert!(
            log.wait_for("Listening (udp)", Duration::from_secs(10))
                .await
        );
        let c = udp_client().await;
        for i in 0..50_u32 {
            let _ = c
                .send_to(format!("n-{i}").as_bytes(), ("127.0.0.1", p))
                .await;
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        assert!(
            log.wait_for("UDP target not resolved yet", Duration::from_secs(3))
                .await
        );
        assert_eq!(
            log.count("UDP target not resolved yet"),
            1,
            "{:#?}",
            log.lines()
        );
        assert!(
            log.count("UDP target DNS resolution failed") <= 1,
            "{:#?}",
            log.lines()
        );
    }
}

#[tokio::test]
async fn closed_udp_target_is_reported_unreachable() {
    let q = free_udp_port();
    let (p, m) = (free_udp_port(), free_port());
    let label = udp_label(p, q);
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{q}/udp");
    let metrics_addr = format!("127.0.0.1:{m}");
    let _rfw = spawn_rfw(&[
        "--probe-interval",
        "1",
        "--metrics-addr",
        &metrics_addr,
        &fwd,
    ]);
    wait_port(m).await;

    let client = udp_client().await;
    let sender = tokio::spawn(async move {
        loop {
            let _ = client.send_to(b"hello", ("127.0.0.1", p)).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    wait_metrics_contains(
        m,
        &format!("rfw_target_reachable{{label=\"{label}\"}} 0"),
        Duration::from_secs(10),
    )
    .await;
    spawn_udp_echo_on(q).await;
    wait_metrics_contains(
        m,
        &format!("rfw_target_reachable{{label=\"{label}\"}} 1"),
        Duration::from_secs(10),
    )
    .await;
    sender.abort();
}

fn reference_yaml(p: u16, e_tcp: u16, e_udp: u16, timeout: u64) -> String {
    format!(
        "forwarders:\n  \
         - {{local_host: \"127.0.0.1\", local_port: {p}, remote_host: \"127.0.0.1\", remote_port: {e_tcp}}}\n  \
         - {{local_host: \"127.0.0.1\", local_port: {p}, remote_host: \"127.0.0.1\", remote_port: {e_udp}, protocol: udp}}\n\
         settings:\n  udp_session_timeout_secs: {timeout}\n"
    )
}

/// The reference scenario of the plan: a TCP and a UDP forwarder on one port.
#[tokio::test]
async fn reference_scenario_tcp_and_udp_side_by_side() {
    let p = free_dual_port();
    let m = free_port();
    let e_tcp = spawn_echo().await;
    let e_udp = spawn_udp_echo().await;
    let tcp_label = format!("127.0.0.1:{p}->127.0.0.1:{e_tcp}");
    let udp_label = format!("127.0.0.1:{p}->127.0.0.1:{e_udp}/udp");

    // 1. start
    let cfg = TempConfig::new("reference", &reference_yaml(p, e_tcp, e_udp, 30));
    let metrics_addr = format!("127.0.0.1:{m}");
    let mut rfw = spawn_rfw(&["-f", &cfg.path_str(), "--metrics-addr", &metrics_addr]);
    wait_port(p).await;
    wait_port(m).await;
    wait_metrics_contains(
        m,
        &format!("rfw_listening{{label=\"{udp_label}\"}} 1"),
        Duration::from_secs(10),
    )
    .await;

    // 2. one datagram and one TCP round trip come back identical
    let client = udp_client().await;
    let got = udp_roundtrip(&client, p, b"reference-datagram", Duration::from_secs(3))
        .await
        .expect("udp reply");
    assert_eq!(got, b"reference-datagram");
    tcp_echo_once(p).await;

    // 3. exact labels and protocols in /stats
    wait_stat(
        m,
        &udp_label,
        "datagrams_sent",
        Duration::from_secs(3),
        |v| v == 1,
    )
    .await;
    let stats = http_get(m, "/stats").await;
    assert!(
        stats.contains(&format!("\"label\":\"{tcp_label}\",\"protocol\":\"tcp\"")),
        "{stats}"
    );
    assert!(
        stats.contains(&format!("\"label\":\"{udp_label}\",\"protocol\":\"udp\"")),
        "{stats}"
    );
    assert_eq!(
        stat_of(&stats, &udp_label, "datagrams_sent"),
        Some(1),
        "{stats}"
    );

    // 4. metrics: UDP families only for the UDP label
    let metrics = http_get(m, "/metrics").await;
    assert!(
        metrics.contains(&format!(
            "rfw_datagrams_sent_total{{label=\"{udp_label}\"}} 1"
        )),
        "{metrics}"
    );
    assert!(
        !metrics.contains(&format!(
            "rfw_datagrams_sent_total{{label=\"{tcp_label}\"}}"
        )),
        "{metrics}"
    );

    // 5. reload with a 1 s timeout: the session expires, TCP keeps serving
    wait_stat(
        m,
        &udp_label,
        "active_connections",
        Duration::from_secs(1),
        |v| v == 1,
    )
    .await;
    cfg.rewrite(&reference_yaml(p, e_tcp, e_udp, 1));
    wait_stat(
        m,
        &udp_label,
        "active_connections",
        Duration::from_secs(5),
        |v| v == 0,
    )
    .await;
    tcp_echo_once(p).await;

    // 6. SIGTERM within 3 s
    #[cfg(unix)]
    {
        let exited =
            tokio::task::spawn_blocking(move || rfw.terminate_and_wait(Duration::from_secs(3)))
                .await
                .unwrap();
        assert!(exited, "rfw must exit within 3 s of SIGTERM");
    }
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    sorted[(sorted.len() * pct / 100).min(sorted.len() - 1)]
}

async fn measure_latency(client: &tokio::net::UdpSocket, port: u16) -> Vec<u64> {
    let payload = [0x5a_u8; 64];
    let mut samples = Vec::with_capacity(2000);
    for _ in 0..2000 {
        let t0 = Instant::now();
        let got = udp_roundtrip(client, port, &payload, Duration::from_secs(1)).await;
        assert!(got.is_some(), "latency round trip lost");
        samples.push(t0.elapsed().as_micros() as u64);
    }
    samples.sort_unstable();
    samples
}

/// Release-mode benchmark, run by G-UDP-BENCH only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn udp_bench() {
    let echo = spawn_udp_echo().await;
    let p = free_udp_port();
    let fwd = format!("127.0.0.1:{p}:127.0.0.1:{echo}/udp");
    let _rfw = spawn_rfw(&[&fwd]);
    let probe = udp_client().await;
    wait_udp_forwarding(&probe, p).await;

    // 1. latency: sequential 64 B round trips, direct then through rfw
    let direct = measure_latency(&probe, echo).await;
    let via = measure_latency(&probe, p).await;
    let (d50, r50) = (percentile(&direct, 50), percentile(&via, 50));
    let overhead = r50 as i64 - d50 as i64;
    println!(
        "UDP_BENCH latency direct_p50_us={d50} direct_p99_us={} rfw_p50_us={r50} rfw_p99_us={} p50_overhead_us={overhead}",
        percentile(&direct, 99),
        percentile(&via, 99),
    );

    // 2. windowed: 4 clients x 16 datagrams of 1200 B in flight, for 5 s
    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let (sent, received) = (sent.clone(), received.clone());
        tasks.push(tokio::spawn(async move {
            let c = udp_client().await;
            c.connect(("127.0.0.1", p)).await.unwrap();
            let payload = [0xa5_u8; 1200];
            let mut buf = vec![0_u8; 2048];
            for _ in 0..16 {
                c.send(&payload).await.unwrap();
                sent.fetch_add(1, Ordering::Relaxed);
            }
            let end = Instant::now() + Duration::from_secs(5);
            while Instant::now() < end {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(200), c.recv(&mut buf)).await
                {
                    received.fetch_add(1, Ordering::Relaxed);
                    c.send(&payload).await.unwrap();
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            }
            // drain for 1 s
            let drain_end = Instant::now() + Duration::from_secs(1);
            while Instant::now() < drain_end {
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(100), c.recv(&mut buf)).await
                {
                    received.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let secs = 5.0_f64.max(started.elapsed().as_secs_f64() - 1.0);
    let (s, r) = (
        sent.load(Ordering::Relaxed),
        received.load(Ordering::Relaxed),
    );
    let loss = s.saturating_sub(r);
    println!(
        "UDP_BENCH windowed pps={:.0} mbit_s={:.1} windowed_loss={loss}",
        r as f64 / secs,
        r as f64 * 1200.0 * 8.0 / secs / 1e6,
    );

    // 3. blast: 4 clients x 50 000 datagrams, no waiting (recorded, not asserted)
    let sent = Arc::new(AtomicU64::new(0));
    let echoed = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let (sent, echoed) = (sent.clone(), echoed.clone());
        tasks.push(tokio::spawn(async move {
            let c = Arc::new(udp_client().await);
            c.connect(("127.0.0.1", p)).await.unwrap();
            let done = Arc::new(AtomicBool::new(false));
            let rx = {
                let (c, echoed, done) = (c.clone(), echoed.clone(), done.clone());
                tokio::spawn(async move {
                    let mut buf = vec![0_u8; 2048];
                    let mut idle_since: Option<Instant> = None;
                    loop {
                        match tokio::time::timeout(Duration::from_millis(100), c.recv(&mut buf))
                            .await
                        {
                            Ok(Ok(_)) => {
                                echoed.fetch_add(1, Ordering::Relaxed);
                                idle_since = None;
                            }
                            _ if done.load(Ordering::Relaxed) => {
                                let since = *idle_since.get_or_insert_with(Instant::now);
                                if since.elapsed() >= Duration::from_secs(1) {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                })
            };
            let payload = [0x3c_u8; 1200];
            for _ in 0..50_000 {
                if c.send(&payload).await.is_ok() {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            }
            done.store(true, Ordering::Relaxed);
            let _ = rx.await;
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let (s, e) = (sent.load(Ordering::Relaxed), echoed.load(Ordering::Relaxed));
    let loss_pct = if s == 0 {
        0.0
    } else {
        s.saturating_sub(e) as f64 * 100.0 / s as f64
    };
    println!("UDP_BENCH blast sent={s} echoed={e} loss_pct={loss_pct:.2}");

    assert_eq!(loss, 0, "windowed_loss must be 0");
    assert!(overhead <= 200, "p50_overhead_us {overhead} > 200");
}
