//! Lifecycle, shutdown, and admission-control tests against the real binary.
//!
//! These cover the paths an operator hits in staging but a unit test cannot
//! reach: process signals, startup validation, and the connection cap.

mod common;

use std::time::Duration;

use common::{
    free_port, http_get, rfw_command, spawn_counting_sink, spawn_echo, spawn_rfw, wait_for_stats,
    wait_port,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Startup validation
// ---------------------------------------------------------------------------

/// A setting that would panic a background task must stop the process with a
/// readable message instead. Before validation existed, `--sample-interval 0`
/// started fine and then killed the rate sampler with a panic inside a spawned
/// task, leaving every throughput gauge frozen at zero for the process lifetime.
#[test]
fn zero_intervals_are_rejected_at_startup() {
    for (flag, field) in [
        ("--sample-interval", "sample_interval_secs"),
        ("--report-interval", "report_interval_secs"),
    ] {
        let out = rfw_command(&[flag, "0", "127.0.0.1:1:127.0.0.1:2"])
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("run rfw");
        assert!(!out.status.success(), "{flag} 0 must fail startup");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(field), "expected {field} in stderr: {err}");
    }
}

/// A zero-sized copy buffer made `copy_bidirectional` treat every read as EOF,
/// spinning the connection task at ~67% CPU while forwarding nothing.
#[test]
fn zero_buffer_bytes_is_rejected_at_startup() {
    let out = rfw_command(&["--buffer-bytes", "0", "127.0.0.1:1:127.0.0.1:2"])
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run rfw");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("buffer_bytes"), "{err}");
}

#[test]
fn unparseable_metrics_addr_is_rejected_at_startup() {
    let out = rfw_command(&["--metrics-addr", "not-a-socket-addr", "127.0.0.1:1:h:2"])
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run rfw");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("metrics_addr"), "{err}");
}

#[test]
fn no_forwarders_is_a_clear_error() {
    let out = rfw_command(&[])
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run rfw");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("No forwarders configured"), "{err}");
}

#[test]
fn malformed_forwarder_argument_is_rejected() {
    let out = rfw_command(&["not:enough:parts"])
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run rfw");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("Invalid forwarder format"), "{err}");
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// SIGTERM is what Docker, Kubernetes, and systemd send first. The binary used
/// to listen only for SIGINT, so every container stop killed the process
/// outright and the documented graceful drain never ran outside a terminal.
#[cfg(unix)]
#[tokio::test]
async fn sigterm_shuts_down_gracefully() {
    let local_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let mut guard = spawn_rfw(&[&fwd]);
    wait_port(local_port).await;

    let exited_on_its_own = tokio::task::spawn_blocking(move || {
        let ok = guard.terminate_and_wait(Duration::from_secs(10));
        (ok, guard)
    })
    .await
    .unwrap()
    .0;

    assert!(
        exited_on_its_own,
        "rfw must handle SIGTERM and exit on its own, not be force-killed"
    );
}

/// The drain window exists so a transfer in flight when the signal arrives is
/// finished rather than truncated.
#[cfg(unix)]
#[tokio::test]
async fn sigterm_drains_an_in_flight_transfer() {
    let local_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let mut guard = spawn_rfw(&[&fwd]);
    wait_port(local_port).await;

    // Open a connection and get it flowing before signalling.
    let mut conn = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
    conn.write_all(b"first").await.unwrap();
    let mut buf = [0_u8; 5];
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"first");

    let pid = guard.0.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }

    // The established connection must keep working through the drain window.
    conn.write_all(b"secnd").await.unwrap();
    let mut buf = [0_u8; 5];
    tokio::time::timeout(Duration::from_secs(5), conn.read_exact(&mut buf))
        .await
        .expect("in-flight connection must survive the drain window")
        .expect("in-flight connection must not be reset by SIGTERM");
    assert_eq!(&buf, b"secnd");

    drop(conn);
    let exited = tokio::task::spawn_blocking(move || {
        let ok = guard.terminate_and_wait(Duration::from_secs(15));
        (ok, guard)
    })
    .await
    .unwrap()
    .0;
    assert!(exited, "rfw must exit once the drain completes");
}

// ---------------------------------------------------------------------------
// Admission control
// ---------------------------------------------------------------------------

/// Without a cap, each in-flight connection holds two copy buffers, so a flood
/// is a straight path to the container memory limit. `--max-connections` sheds
/// at the edge and records the rejection.
#[tokio::test]
async fn max_connections_sheds_excess_and_counts_it() {
    let local_port = free_port();
    let metrics_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let _guard = spawn_rfw(&[
        "--max-connections",
        "2",
        "--metrics-addr",
        &metrics_addr,
        &fwd,
    ]);
    wait_port(local_port).await;
    wait_port(metrics_port).await;

    // `wait_port` probes by connecting, and a probe consumes a permit like any
    // other connection. Let it drain before saturating the cap, or the test
    // races its own readiness check for one of the two slots.
    wait_for_stats(metrics_port, 50, |b| b.contains("\"active_connections\":0")).await;

    // Hold two connections open — that saturates the cap.
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut c = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
        c.write_all(b"x").await.unwrap();
        let mut b = [0_u8; 1];
        c.read_exact(&mut b).await.unwrap(); // proves it is fully proxied
        held.push(c);
    }

    let body = wait_for_stats(metrics_port, 50, |b| b.contains("\"active_connections\":2")).await;
    assert!(body.contains("\"rejected_connections\":0"), "{body}");

    // Further connections are accepted by the kernel and then closed by rfw
    // without ever being proxied.
    for _ in 0..5 {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", local_port)).await {
            let _ = c.write_all(b"y").await;
            let mut sink = Vec::new();
            // The peer closes without echoing: read_to_end returns empty.
            let _ = tokio::time::timeout(Duration::from_secs(2), c.read_to_end(&mut sink)).await;
            assert!(sink.is_empty(), "shed connection must not be proxied");
        }
    }

    let body = wait_for_stats(metrics_port, 50, |b| {
        !b.contains("\"rejected_connections\":0")
    })
    .await;
    assert!(
        body.contains("\"active_connections\":2"),
        "the cap must hold at 2: {body}"
    );

    // Releasing the held connections frees the slots again.
    drop(held);
    wait_for_stats(metrics_port, 50, |b| b.contains("\"active_connections\":0")).await;

    let mut c = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
    c.write_all(b"z").await.unwrap();
    let mut b = [0_u8; 1];
    tokio::time::timeout(Duration::from_secs(5), c.read_exact(&mut b))
        .await
        .expect("a freed slot must admit a new connection")
        .expect("new connection must be proxied");
    assert_eq!(&b, b"z");

    let prom = http_get(metrics_port, "/metrics").await;
    assert!(
        prom.contains("rfw_rejected_connections_total"),
        "rejections must be scrapeable: {prom}"
    );
}

// ---------------------------------------------------------------------------
// Data-path edge cases
// ---------------------------------------------------------------------------

/// A client half-close must reach the backend as an EOF, on both data paths.
/// Protocols that signal "request finished" with `shutdown(SHUT_WR)` hang
/// forever if the forwarder swallows it.
#[tokio::test]
async fn half_close_propagates_to_the_backend() {
    for extra in half_close_modes() {
        let local_port = free_port();
        let sink_port = spawn_counting_sink().await;

        let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{sink_port}");
        let mut args = extra.clone();
        args.push(fwd.clone());
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let _guard = spawn_rfw(&arg_refs);
        wait_port(local_port).await;

        let payload = vec![b'Q'; 3 * 1024 * 1024];
        let mut conn = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
        let (mut rd, mut wr) = conn.split();

        let writer = async {
            wr.write_all(&payload).await.unwrap();
            // The backend only answers after it sees EOF.
            wr.shutdown().await.unwrap();
        };
        let reader = async {
            let mut reply = Vec::new();
            rd.read_to_end(&mut reply).await.unwrap();
            reply
        };
        let (_, reply) = tokio::join!(writer, reader);

        let reply = String::from_utf8_lossy(&reply);
        assert_eq!(
            reply,
            format!("TOTAL={}", payload.len()),
            "mode {extra:?}: backend must see every byte and the half-close"
        );
    }
}

fn half_close_modes() -> Vec<Vec<String>> {
    let mut modes = vec![vec![]];
    if cfg!(target_os = "linux") {
        modes.push(vec!["--splice".to_string()]);
    }
    modes
}

/// Many short connections in parallel must all complete and leave no leaked
/// active-connection count behind.
#[tokio::test]
async fn concurrent_connections_all_complete_without_leaking() {
    let local_port = free_port();
    let metrics_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let _guard = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(local_port).await;
    wait_port(metrics_port).await;

    const CONNS: usize = 64;
    const PAYLOAD: usize = 4096;

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..CONNS {
        tasks.spawn(async move {
            let mut c = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
            let payload = vec![(i % 251) as u8; PAYLOAD];
            let (mut rd, mut wr) = c.split();
            let mut got = vec![0_u8; PAYLOAD];
            let w = async {
                wr.write_all(&payload).await.unwrap();
                wr.shutdown().await.unwrap();
            };
            let r = async { rd.read_exact(&mut got).await.unwrap() };
            tokio::join!(w, r);
            assert_eq!(got, payload, "connection {i} corrupted");
        });
    }
    while let Some(r) = tasks.join_next().await {
        r.expect("connection task panicked");
    }

    let expected = CONNS * PAYLOAD;
    let body = wait_for_stats(metrics_port, 100, |b| {
        b.contains("\"active_connections\":0")
    })
    .await;
    assert!(
        body.contains(&format!("\"bytes_sent\":{expected}")),
        "every byte must be accounted for (expected {expected}): {body}"
    );
    assert!(
        body.contains(&format!("\"bytes_received\":{expected}")),
        "echo bytes must be accounted for: {body}"
    );
}

/// A backend that refuses connections must fail the client quickly and leave no
/// active-connection count behind — not hang for the connect timeout.
#[tokio::test]
async fn unreachable_backend_fails_fast_without_leaking() {
    let dead_port = free_port(); // nothing listening
    let local_port = free_port();
    let metrics_port = free_port();

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{dead_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let _guard = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(local_port).await;
    wait_port(metrics_port).await;

    let started = std::time::Instant::now();
    let mut c = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
    let mut sink = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), c.read_to_end(&mut sink))
        .await
        .expect("connection to a dead backend must not hang")
        .ok();
    assert!(sink.is_empty());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a refused connection must not wait out the connect timeout"
    );

    wait_for_stats(metrics_port, 50, |b| b.contains("\"active_connections\":0")).await;
}

// ---------------------------------------------------------------------------
// Container healthcheck probe
// ---------------------------------------------------------------------------

/// `--health-check` exists because the distroless runtime image has no shell
/// and no curl, and probing the process (`pgrep`, `--version`) proves nothing:
/// a process whose forwarders all failed to bind is still running. The probe
/// has to actually reach the stats endpoint.
#[tokio::test]
async fn health_check_probe_reports_liveness() {
    let local_port = free_port();
    let metrics_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");

    // Nothing listening yet: the probe must fail, not hang or succeed.
    let down = tokio::task::spawn_blocking({
        let addr = metrics_addr.clone();
        move || {
            rfw_command(&["--health-check", &addr])
                .output()
                .expect("run probe")
        }
    })
    .await
    .unwrap();
    assert!(
        !down.status.success(),
        "probe must fail when no instance is running"
    );

    let _guard = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(local_port).await;
    wait_port(metrics_port).await;

    let up = tokio::task::spawn_blocking({
        let addr = metrics_addr.clone();
        move || {
            rfw_command(&["--health-check", &addr])
                .output()
                .expect("run probe")
        }
    })
    .await
    .unwrap();
    assert!(
        up.status.success(),
        "probe must succeed against a live instance: {}",
        String::from_utf8_lossy(&up.stderr)
    );
}

/// The probe must fail while a forwarder cannot bind its port, and recover on
/// its own once the port frees up. With `--net=host` this is the everyday
/// failure: something else already listens on the port rfw was given, rfw
/// retries the bind every 5s, and until this fix the process was "healthy".
#[tokio::test]
async fn health_check_fails_while_a_forwarder_cannot_bind_and_recovers() {
    // Hold the port rfw is about to be told to listen on.
    let blocker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taken_port = blocker.local_addr().unwrap().port();
    let metrics_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{taken_port}:127.0.0.1:{echo_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let _guard = spawn_rfw(&["--metrics-addr", &metrics_addr, &fwd]);
    wait_port(metrics_port).await;

    let root = http_get(metrics_port, "/").await;
    assert!(root.starts_with("HTTP/1.1 503"), "must be degraded: {root}");
    assert!(
        root.contains(&format!("127.0.0.1:{taken_port}->127.0.0.1:{echo_port}")),
        "body must name the forwarder that cannot bind: {root}"
    );
    let stats = http_get(metrics_port, "/stats").await;
    assert!(stats.contains("\"listening\":false"), "{stats}");

    let probe = tokio::task::spawn_blocking({
        let addr = metrics_addr.clone();
        move || {
            rfw_command(&["--health-check", &addr])
                .output()
                .expect("run probe")
        }
    })
    .await
    .unwrap();
    assert!(
        !probe.status.success(),
        "probe must fail while a forwarder is not listening"
    );

    // Free the port: the bind-retry loop (5s cadence) claims it and the
    // process turns healthy without a restart.
    drop(blocker);
    let mut healthy = false;
    for _ in 0..160 {
        if http_get(metrics_port, "/")
            .await
            .starts_with("HTTP/1.1 200 OK")
        {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(healthy, "must recover once the port is free");

    let probe = tokio::task::spawn_blocking({
        let addr = metrics_addr.clone();
        move || {
            rfw_command(&["--health-check", &addr])
                .output()
                .expect("run probe")
        }
    })
    .await
    .unwrap();
    assert!(probe.status.success(), "probe must pass after recovery");
}

#[test]
fn health_check_rejects_a_malformed_address() {
    let out = rfw_command(&["--health-check", "not-an-address"])
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run probe");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("host:port"), "{err}");
}

/// Abruptly reset connections must not leave a proxy task parked forever.
///
/// On the splice path this is a real deadlock shape: when one direction errors,
/// the other is left waiting on a backend that is itself waiting for data or
/// EOF from us, and neither socket is closed until the proxy returns. The
/// result is a connection that never finishes — it holds its permit and its
/// active-connection count until the shutdown drain gives up. The error path
/// therefore has to tear down *both* directions.
#[tokio::test]
async fn abrupt_resets_do_not_leak_connections() {
    for extra in half_close_modes() {
        let local_port = free_port();
        let metrics_port = free_port();
        let echo_port = spawn_echo().await;

        let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
        let metrics_addr = format!("127.0.0.1:{metrics_port}");
        let mut args = extra.clone();
        args.push("--metrics-addr".to_string());
        args.push(metrics_addr.clone());
        args.push(fwd.clone());
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let _guard = spawn_rfw(&arg_refs);
        wait_port(local_port).await;
        wait_port(metrics_port).await;

        // Mix of RST-mid-write, immediate close, and clean transfers — the
        // combination that surfaced the deadlock under load.
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..60_usize {
            tasks.spawn(async move {
                let Ok(sock) = TcpStream::connect(("127.0.0.1", local_port)).await else {
                    return;
                };
                match i % 3 {
                    // Reset an *idle* connection: complete a round trip first,
                    // so both proxy directions are parked waiting for more data
                    // when the RST lands. That is the shape that deadlocks —
                    // the client-side direction errors on the reset while the
                    // backend-side direction is blocked on a backend that will
                    // never speak again unless its socket is closed.
                    0 => {
                        let mut sock = sock;
                        let payload = vec![b'R'; 4096];
                        if sock.write_all(&payload).await.is_err() {
                            return;
                        }
                        let mut echoed = vec![0_u8; payload.len()];
                        if sock.read_exact(&mut echoed).await.is_err() {
                            return;
                        }
                        // Both directions are now idle. Reset hard.
                        let s = socket2::SockRef::from(&sock);
                        let _ = s.set_linger(Some(Duration::ZERO));
                        drop(sock); // sends RST
                    }
                    // Connect and vanish.
                    1 => drop(sock),
                    // Clean round trip.
                    _ => {
                        let mut sock = sock;
                        let payload = vec![b'C'; 4096];
                        let (mut rd, mut wr) = sock.split();
                        let mut got = vec![0_u8; payload.len()];
                        let w = async {
                            let _ = wr.write_all(&payload).await;
                            let _ = wr.shutdown().await;
                        };
                        let r = async { rd.read_exact(&mut got).await };
                        let (_, r) = tokio::join!(w, r);
                        assert!(r.is_ok(), "clean transfer must survive noisy neighbours");
                    }
                }
            });
        }
        while let Some(r) = tasks.join_next().await {
            r.expect("client task panicked");
        }

        // Every task must unwind. A stuck direction shows up here as an
        // active-connection count that never returns to zero.
        let body = wait_for_stats(metrics_port, 100, |b| {
            b.contains("\"active_connections\":0")
        })
        .await;
        assert!(
            body.contains("\"active_connections\":0"),
            "mode {extra:?}: connections leaked after abrupt resets: {body}"
        );
    }
}
