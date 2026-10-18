//! Webhook alerting against the real binary.
//!
//! An in-process HTTP sink plays the Google Chat webhook: every POST body is
//! parsed as JSON and its `text` field is handed to the test. The tests then
//! drive the situations an operator wants to hear about (startup, target
//! outage and recovery, a taken port, reload errors, shutdown) and assert on
//! the messages that arrive.

mod common;

use std::time::Duration;

use common::{
    free_port, free_udp_port, rfw_command, spawn_echo, spawn_rfw, spawn_udp_echo,
    spawn_udp_echo_on, udp_client, wait_port, ChildGuard, TempConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

/// Minimal HTTP/1.1 sink that accepts webhook POSTs and yields their `text`.
async fn spawn_webhook_sink() -> (String, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0_u8; 4096];
                let (head_end, len) = loop {
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
                        break (pos + 4, len);
                    }
                };
                while buf.len() < head_end + len {
                    let n = match sock.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8_lossy(&buf[head_end..head_end + len]).to_string();
                let text = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v["text"].as_str().map(str::to_string))
                    .unwrap_or_else(|| format!("<non-json body: {body}>"));
                let _ = tx.send(text);
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (format!("http://{addr}/hook"), rx)
}

/// Wait up to `within` for a message containing every needle, skipping
/// unrelated ones. Panics with everything seen so far on timeout.
async fn expect_message(
    rx: &mut mpsc::UnboundedReceiver<String>,
    within: Duration,
    needles: &[&str],
) -> String {
    let deadline = tokio::time::Instant::now() + within;
    let mut seen = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("no message with {needles:?} within {within:?}; seen: {seen:#?}");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(text)) => {
                if needles.iter().all(|n| text.contains(n)) {
                    return text;
                }
                seen.push(text);
            }
            Ok(None) => panic!("sink closed"),
            Err(_) => panic!("no message with {needles:?} within {within:?}; seen: {seen:#?}"),
        }
    }
}

/// A backend the test can take down and bring back on the same port.
struct StoppableBackend {
    port: u16,
    stop: Option<oneshot::Sender<()>>,
}

impl StoppableBackend {
    async fn start_on(port: Option<u16>) -> Self {
        let listener = match port {
            Some(p) => TcpListener::bind(("127.0.0.1", p)).await.unwrap(),
            None => TcpListener::bind("127.0.0.1:0").await.unwrap(),
        };
        let port = listener.local_addr().unwrap().port();
        let (tx, mut rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => break, // drop the listener: port closes
                    accepted = listener.accept() => {
                        let Ok((sock, _)) = accepted else { break };
                        drop(sock);
                    }
                }
            }
        });
        Self {
            port,
            stop: Some(tx),
        }
    }

    /// Close the listener; connections to `port` are refused from now on.
    fn stop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
    }
}

fn rfw_with_webhook(url: &str, extra: &[&str]) -> ChildGuard {
    let mut args = vec!["--webhook-url", url, "--instance-name", "e2e-instance"];
    args.extend_from_slice(extra);
    spawn_rfw(&args)
}

// ---------------------------------------------------------------------------

/// The startup message lists every forwarder with the outcome of one probe of
/// its target, and names the instance.
#[tokio::test]
async fn startup_alert_reports_every_target() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let echo = spawn_echo().await;
    let dead = free_port();
    let (la, lb) = (free_port(), free_port());
    let fwd_ok = format!("127.0.0.1:{la}:127.0.0.1:{echo}");
    let fwd_dead = format!("127.0.0.1:{lb}:127.0.0.1:{dead}");
    // Alerts name forwarders by their label: local->remote.
    let label_ok = format!("127.0.0.1:{la}->127.0.0.1:{echo}");
    let label_dead = format!("127.0.0.1:{lb}->127.0.0.1:{dead}");

    // The URL travels through the environment, as production should do it.
    let mut cmd = rfw_command(&["--instance-name", "e2e-instance", &fwd_ok, &fwd_dead]);
    cmd.env("RFW_WEBHOOK_URL", &hook);
    let _rfw = ChildGuard(cmd.spawn().expect("spawn rfw"));
    wait_port(la).await;

    let text = expect_message(&mut rx, Duration::from_secs(15), &["*rfw started*"]).await;
    assert!(text.contains("`e2e-instance`"), "{text}");
    assert!(text.contains("2 forwarder(s)"), "{text}");
    assert!(text.contains("1 target(s) unreachable"), "{text}");
    assert!(
        text.contains(&format!("`{label_ok}` ✅ reachable at `127.0.0.1:{echo}`")),
        "{text}"
    );
    assert!(
        text.contains(&format!("`{label_dead}` ❌ unreachable:")),
        "{text}"
    );
    assert!(
        text.contains(&format!("rfw {}", env!("CARGO_PKG_VERSION"))),
        "footer carries the version: {text}"
    );
}

/// Target down for longer than `--unreachable-after` raises exactly one alert;
/// the target coming back raises exactly one recovery.
#[tokio::test]
async fn target_outage_and_recovery_are_alerted() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let mut backend = StoppableBackend::start_on(None).await;
    let local = free_port();
    let fwd = format!("127.0.0.1:{local}:127.0.0.1:{}", backend.port);
    let label = format!("127.0.0.1:{local}->127.0.0.1:{}", backend.port);
    let _rfw = rfw_with_webhook(
        &hook,
        &["--probe-interval", "1", "--unreachable-after", "2", &fwd],
    );
    wait_port(local).await;
    expect_message(
        &mut rx,
        Duration::from_secs(15),
        &["*rfw started*", "✅ reachable"],
    )
    .await;

    backend.stop();
    let text = expect_message(
        &mut rx,
        Duration::from_secs(20),
        &[
            "*Target unreachable*",
            &format!("`127.0.0.1:{}`", backend.port),
        ],
    )
    .await;
    assert!(text.contains(&format!("`{label}`")), "{text}");
    assert!(text.contains("Last error:"), "{text}");

    // Still down: no second alert.
    assert!(
        tokio::time::timeout(Duration::from_secs(4), rx.recv())
            .await
            .is_err(),
        "must not re-alert while the outage continues"
    );

    let _back = StoppableBackend::start_on(Some(backend.port)).await;
    let text = expect_message(
        &mut rx,
        Duration::from_secs(20),
        &["*Target reachable again*", "Was unreachable for"],
    )
    .await;
    assert!(
        text.contains(&format!("Answering at `127.0.0.1:{}`", backend.port)),
        "{text}"
    );
}

/// A UDP target is only resolved at startup, never probed: the message says so
/// and does not call it unreachable.
#[tokio::test]
async fn startup_reports_udp_targets_as_resolved() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let udp_echo = spawn_udp_echo().await;
    let tcp_echo = spawn_echo().await;
    let (lu, lt) = (free_udp_port(), free_port());
    let fwd_udp = format!("127.0.0.1:{lu}:127.0.0.1:{udp_echo}/udp");
    let fwd_tcp = format!("127.0.0.1:{lt}:127.0.0.1:{tcp_echo}");
    let _rfw = rfw_with_webhook(&hook, &[&fwd_udp, &fwd_tcp]);
    wait_port(lt).await;

    let text = expect_message(&mut rx, Duration::from_secs(15), &["*rfw started*"]).await;
    assert!(text.contains("udp: passive check"), "{text}");
    assert!(text.contains("resolved to"), "{text}");
    assert!(!text.contains("unreachable"), "{text}");
    assert!(
        text.contains("✅ reachable at"),
        "the TCP row stays active: {text}"
    );
}

/// ICMP port-unreachable evidence from the data path raises the outage alert for a
/// UDP forwarder; replies from the target afterwards raise the recovery.
#[tokio::test]
async fn udp_outage_is_alerted_and_recovery_reported() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let q = free_udp_port();
    let local = free_udp_port();
    let fwd = format!("127.0.0.1:{local}:127.0.0.1:{q}/udp");
    let _rfw = rfw_with_webhook(
        &hook,
        &["--probe-interval", "1", "--unreachable-after", "2", &fwd],
    );
    expect_message(&mut rx, Duration::from_secs(15), &["*rfw started*"]).await;

    // Keep traffic flowing: every datagram to the closed port draws an ICMP error.
    let client = udp_client().await;
    let sender = tokio::spawn(async move {
        loop {
            let _ = client.send_to(b"hello", ("127.0.0.1", local)).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    expect_message(
        &mut rx,
        Duration::from_secs(20),
        &["*Target unreachable*", "port unreachable from the target"],
    )
    .await;

    spawn_udp_echo_on(q).await;
    expect_message(
        &mut rx,
        Duration::from_secs(15),
        &["*Target reachable again*"],
    )
    .await;
    sender.abort();
}

/// A taken local port is alerted (once per throttle window, not once per 5 s
/// retry) and the eventual bind is alerted as a recovery.
#[tokio::test]
async fn bind_failure_and_recovery_are_alerted() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let echo = spawn_echo().await;
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let local = blocker.local_addr().unwrap().port();
    let fwd = format!("127.0.0.1:{local}:127.0.0.1:{echo}");
    let label = format!("127.0.0.1:{local}->127.0.0.1:{echo}");
    let _rfw = rfw_with_webhook(&hook, &[&fwd]);

    let text = expect_message(
        &mut rx,
        Duration::from_secs(15),
        &["*Cannot bind*", &format!("`127.0.0.1:{local}`")],
    )
    .await;
    assert!(text.contains(&format!("`{label}`")), "{text}");

    // Two more retries happen inside the throttle window: no second message.
    tokio::time::sleep(Duration::from_secs(6)).await;
    drop(blocker);
    let text = expect_message(
        &mut rx,
        Duration::from_secs(20),
        &["*Listening again*", &format!("`127.0.0.1:{local}`")],
    )
    .await;
    assert!(text.contains("failed attempt(s)"), "{text}");
    wait_port(local).await;
}

/// Hot-reload outcomes are alerted: a good edit names what changed, a broken
/// file says the running configuration was kept.
#[tokio::test]
async fn reload_success_and_failure_are_alerted() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let echo = spawn_echo().await;
    let (a, b) = (free_port(), free_port());
    let one = format!(
        "forwarders:\n  - local_host: 127.0.0.1\n    local_port: {a}\n    remote_host: 127.0.0.1\n    remote_port: {echo}\n"
    );
    let cfg = TempConfig::new("webhook-reload", &one);
    let _rfw = rfw_with_webhook(&hook, &["-f", &cfg.path_str()]);
    wait_port(a).await;
    expect_message(&mut rx, Duration::from_secs(15), &["*rfw started*"]).await;

    // Add a forwarder.
    let two = format!(
        "{one}  - local_host: 127.0.0.1\n    local_port: {b}\n    remote_host: 127.0.0.1\n    remote_port: {echo}\n"
    );
    cfg.rewrite(&two);
    wait_port(b).await;
    let text = expect_message(
        &mut rx,
        Duration::from_secs(15),
        &["*Config reloaded*", "2 forwarder(s) active"],
    )
    .await;
    assert!(
        text.contains(&format!("Added: `127.0.0.1:{b}->127.0.0.1:{echo}`")),
        "{text}"
    );

    // Break the file.
    cfg.rewrite("forwarders: [\n  this is not yaml\n");
    let text = expect_message(
        &mut rx,
        Duration::from_secs(15),
        &["*Config reload failed*", "running configuration kept"],
    )
    .await;
    assert!(text.contains("forwarders.yml"), "{text}");
    // Both ports still serve.
    wait_port(a).await;
    wait_port(b).await;
}

/// SIGTERM produces a "stopping" message before the process exits.
#[cfg(unix)]
#[tokio::test]
async fn shutdown_sends_a_stopping_alert() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let echo = spawn_echo().await;
    let local = free_port();
    let fwd = format!("127.0.0.1:{local}:127.0.0.1:{echo}");
    let mut rfw = rfw_with_webhook(&hook, &[&fwd]);
    wait_port(local).await;
    expect_message(&mut rx, Duration::from_secs(15), &["*rfw started*"]).await;

    // `terminate_and_wait` blocks the thread; the webhook sink lives on this
    // test's runtime and must keep answering while rfw flushes its last alert.
    let (exited, _rfw) = tokio::task::spawn_blocking(move || {
        let ok = rfw.terminate_and_wait(Duration::from_secs(10));
        (ok, rfw)
    })
    .await
    .unwrap();
    assert!(exited, "rfw must exit on SIGTERM");
    let text = expect_message(
        &mut rx,
        Duration::from_secs(5),
        &["*rfw stopping*", "SIGTERM"],
    )
    .await;
    assert!(text.contains("`e2e-instance`"), "{text}");
}

/// A malformed webhook URL is a startup error, and the error never echoes
/// the URL (it may carry credentials).
#[test]
fn invalid_webhook_url_is_rejected_at_startup() {
    let out = rfw_command(&[
        "--webhook-url",
        "ftp://chat.example/hook?token=SECRET-TOKEN",
        "127.0.0.1:1:127.0.0.1:2",
    ])
    .stderr(std::process::Stdio::piped())
    .output()
    .expect("run rfw");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("webhook_url"), "{err}");
    assert!(!err.contains("SECRET-TOKEN"), "credentials leaked: {err}");
}

/// The webhook flags show up in `--help`, including the env var binding.
#[test]
fn help_documents_the_webhook_flags() {
    let out = rfw_command(&["--help"])
        .stdout(std::process::Stdio::piped())
        .output()
        .expect("run rfw");
    let help = String::from_utf8_lossy(&out.stdout);
    for flag in [
        "--webhook-url",
        "RFW_WEBHOOK_URL",
        "--instance-name",
        "--probe-interval",
        "--unreachable-after",
    ] {
        assert!(help.contains(flag), "missing {flag} in --help:\n{help}");
    }
}

/// `/metrics` exposes the target gauge and the webhook counters, and `/stats`
/// carries `target_reachable`.
#[tokio::test]
async fn metrics_expose_target_reachability_and_webhook_counters() {
    let (hook, mut rx) = spawn_webhook_sink().await;
    let echo = spawn_echo().await;
    let local = free_port();
    let metrics = free_port();
    let fwd = format!("127.0.0.1:{local}:127.0.0.1:{echo}");
    let label = format!("127.0.0.1:{local}->127.0.0.1:{echo}");
    let _rfw = rfw_with_webhook(
        &hook,
        &["--metrics-addr", &format!("127.0.0.1:{metrics}"), &fwd],
    );
    wait_port(local).await;
    expect_message(&mut rx, Duration::from_secs(15), &["*rfw started*"]).await;

    let stats =
        common::wait_for_stats(metrics, 100, |s| s.contains("\"target_reachable\":true")).await;
    assert!(stats.contains("\"listening\":true"), "{stats}");

    let prom = common::http_get(metrics, "/metrics").await;
    assert!(
        prom.contains(&format!("rfw_target_reachable{{label=\"{label}\"}} 1")),
        "{prom}"
    );
    assert!(prom.contains("rfw_webhook_enabled 1"), "{prom}");
    assert!(prom.contains("rfw_webhook_sent_total 1"), "{prom}");
    assert!(prom.contains("rfw_webhook_failed_total 0"), "{prom}");
}
