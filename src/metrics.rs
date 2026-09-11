use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::stats::{StatsRegistry, StatsSnapshot};

/// Cap on the request bytes we read before giving up (headers only; GET has no body).
const MAX_REQUEST_BYTES: usize = 8192;

/// Max time allowed to read the full request head. Bounds slowloris clients that
/// open a connection and then trickle (or never send) bytes, which would
/// otherwise leak one hung task per connection.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Max concurrent in-flight metrics requests. The endpoint is an operational
/// side channel; it must never be able to spawn unbounded tasks or starve the
/// data plane, however hard something scrapes it.
const MAX_CONCURRENT_REQUESTS: usize = 32;

/// Backoff after an accept error that is not per-connection (EMFILE/ENFILE).
/// Without it the loop spins at 100% CPU for as long as fds stay exhausted.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// Serve a read-only HTTP endpoint exposing live traffic stats.
///
/// Routes: `GET /metrics` -> Prometheus text, `GET /stats` -> JSON,
/// `GET /` -> liveness, anything else -> 404. Connection-per-request (no
/// keep-alive). This is a minimal, dependency-free responder; it only handles
/// GET and ignores the request body.
pub async fn serve(addr: String, registry: Arc<StatsRegistry>, cancel: CancellationToken) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "Metrics server failed to bind {addr}");
            return;
        }
    };
    info!("Metrics endpoint listening on http://{addr}/metrics (and /stats)");

    let limiter = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Metrics server stopped");
                return;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        // Shed instead of queueing: a scraper that piles up is
                        // better off with a closed connection than with the
                        // forwarder's memory.
                        let Ok(permit) = limiter.clone().try_acquire_owned() else {
                            warn!("Metrics request dropped: {MAX_CONCURRENT_REQUESTS} already in flight");
                            continue;
                        };
                        let reg = registry.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = handle_conn(stream, reg).await {
                                warn!(error = %e, "Metrics request error");
                            }
                        });
                    }
                    Err(e) => {
                        // A peer that gave up between SYN and accept is normal;
                        // fd exhaustion is not, and retrying it immediately is
                        // a busy loop.
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::Interrupted
                        ) {
                            continue;
                        }
                        error!(error = %e, "Metrics accept failed, backing off 1s");
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(ACCEPT_BACKOFF) => {}
                        }
                    }
                }
            }
        }
    }
}

/// Probe a running rfw's stats endpoint and report whether it is healthy.
///
/// Exists so the binary can be its own container healthcheck: the distroless
/// runtime image has no shell and no `curl`, and probing the *process* (pgrep,
/// `rfw --version`) proves nothing — a process whose forwarders all failed to
/// bind is still running.
///
/// Healthy means the endpoint answered `200 OK` within the timeout.
pub async fn health_check(addr: &str) -> Result<(), anyhow::Error> {
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    let socket_addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("--health-check '{addr}' is not a host:port address: {e}"))?;

    let response = timeout(PROBE_TIMEOUT, async {
        let mut stream = TcpStream::connect(socket_addr).await?;
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut buf = Vec::with_capacity(256);
        stream.read_to_end(&mut buf).await?;
        Ok::<_, std::io::Error>(buf)
    })
    .await
    .map_err(|_| anyhow::anyhow!("health check timed out after {PROBE_TIMEOUT:?}"))?
    .map_err(|e| anyhow::anyhow!("health check could not reach {addr}: {e}"))?;

    let head = String::from_utf8_lossy(&response);
    if head.starts_with("HTTP/1.1 200 OK") {
        Ok(())
    } else {
        let first = head.lines().next().unwrap_or("<empty response>");
        anyhow::bail!("health check got an unhealthy response from {addr}: {first}")
    }
}

async fn handle_conn(mut stream: TcpStream, registry: Arc<StatsRegistry>) -> std::io::Result<()> {
    // Read request head until CRLFCRLF or the cap is hit, under a hard deadline
    // so a stalled client cannot pin the task open forever.
    let mut buf = Vec::with_capacity(512);
    let read_head = async {
        let mut chunk = [0_u8; 1024];
        // Only the tail needs re-scanning each round: a terminator can straddle
        // a chunk boundary by at most 3 bytes, so rescanning the whole buffer
        // would make header parsing quadratic in the request size.
        let mut scanned = 0_usize;
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let from = scanned.saturating_sub(3);
            if buf[from..].windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= MAX_REQUEST_BYTES {
                break;
            }
            scanned = buf.len();
        }
        Ok::<(), std::io::Error>(())
    };
    match timeout(REQUEST_READ_TIMEOUT, read_head).await {
        Ok(r) => r?,
        Err(_) => return Ok(()), // deadline hit: drop the connection
    }

    let head = String::from_utf8_lossy(&buf);
    let response = match parse_request_target(&head).as_deref() {
        Some("/metrics") => {
            let body = render_prometheus(&registry.collect().await);
            http_response("200 OK", "text/plain; version=0.0.4", &body)
        }
        Some("/stats") => {
            let body = render_json(&registry.collect().await);
            http_response("200 OK", "application/json", &body)
        }
        Some("/") => http_response("200 OK", "text/plain", "rfw ok\n"),
        _ => http_response("404 Not Found", "text/plain", "not found\n"),
    };

    stream.write_all(response.as_bytes()).await?;
    let _ = stream.shutdown().await;
    Ok(())
}

/// Extract the request target from an HTTP request head ("GET /path HTTP/1.1").
/// Returns `None` for non-GET methods or a malformed request line.
pub fn parse_request_target(head: &str) -> Option<String> {
    let first = head.lines().next()?;
    let mut parts = first.split_whitespace();
    if parts.next()? != "GET" {
        return None;
    }
    parts.next().map(|s| s.to_string())
}

fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Escape a label value for a Prometheus / JSON string context.
///
/// Control characters matter here: a label is built from config-supplied
/// hostnames, and a raw newline inside one would terminate the Prometheus
/// exposition line early (and produce invalid JSON), corrupting every metric
/// after it.
fn escape(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for c in label.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Append one metric family (HELP + TYPE + one line per forwarder).
fn push_metric(
    out: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    snaps: &[StatsSnapshot],
    getter: impl Fn(&StatsSnapshot) -> u64,
) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} {kind}\n"));
    for s in snaps {
        out.push_str(&format!(
            "{name}{{label=\"{}\"}} {}\n",
            escape(&s.label),
            getter(s)
        ));
    }
}

/// Render snapshots in Prometheus text exposition format.
pub fn render_prometheus(snaps: &[StatsSnapshot]) -> String {
    let mut out = String::new();
    push_metric(
        &mut out,
        "rfw_bytes_sent_total",
        "Lifetime bytes forwarded client->remote.",
        "counter",
        snaps,
        |s| s.bytes_sent,
    );
    push_metric(
        &mut out,
        "rfw_bytes_received_total",
        "Lifetime bytes forwarded remote->client.",
        "counter",
        snaps,
        |s| s.bytes_received,
    );
    push_metric(
        &mut out,
        "rfw_bytes_sent_rate_bps",
        "Current send throughput (bytes/sec).",
        "gauge",
        snaps,
        |s| s.rate_sent_bps,
    );
    push_metric(
        &mut out,
        "rfw_bytes_received_rate_bps",
        "Current receive throughput (bytes/sec).",
        "gauge",
        snaps,
        |s| s.rate_recv_bps,
    );
    push_metric(
        &mut out,
        "rfw_active_connections",
        "Currently active connections.",
        "gauge",
        snaps,
        |s| s.active_connections,
    );
    push_metric(
        &mut out,
        "rfw_rejected_connections_total",
        "Lifetime connections refused because max_connections was saturated.",
        "counter",
        snaps,
        |s| s.rejected_connections,
    );
    out
}

/// Hand-rolled JSON array of snapshots (avoids a `serde_json` dependency).
pub fn render_json(snaps: &[StatsSnapshot]) -> String {
    let mut out = String::from("[");
    for (i, s) in snaps.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"label\":\"{}\",\"bytes_sent\":{},\"bytes_received\":{},\"active_connections\":{},\"rejected_connections\":{},\"rate_sent_bps\":{},\"rate_recv_bps\":{}}}",
            escape(&s.label),
            s.bytes_sent,
            s.bytes_received,
            s.active_connections,
            s.rejected_connections,
            s.rate_sent_bps,
            s.rate_recv_bps
        ));
    }
    out.push(']');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap() -> StatsSnapshot {
        StatsSnapshot {
            label: "a:1->b:2".to_string(),
            bytes_sent: 100,
            bytes_received: 200,
            active_connections: 1,
            rejected_connections: 3,
            rate_sent_bps: 10,
            rate_recv_bps: 20,
        }
    }

    #[test]
    fn parse_get_target() {
        assert_eq!(
            parse_request_target("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").as_deref(),
            Some("/metrics")
        );
        assert_eq!(parse_request_target("GET / HTTP/1.1").as_deref(), Some("/"));
        assert_eq!(parse_request_target("POST /x HTTP/1.1"), None);
        assert_eq!(parse_request_target(""), None);
    }

    #[test]
    fn prometheus_has_cumulative() {
        let out = render_prometheus(&[snap()]);
        assert!(out.contains("# TYPE rfw_bytes_sent_total counter"));
        assert!(out.contains("rfw_bytes_sent_total{label=\"a:1->b:2\"} 100"));
        assert!(out.contains("rfw_bytes_received_total{label=\"a:1->b:2\"} 200"));
        assert!(out.contains("rfw_active_connections{label=\"a:1->b:2\"} 1"));
    }

    #[test]
    fn json_has_fields() {
        let out = render_json(&[snap()]);
        assert!(out.starts_with('[') && out.ends_with(']'));
        assert!(out.contains("\"bytes_sent\":100"));
        assert!(out.contains("\"bytes_received\":200"));
        assert!(out.contains("\"rate_recv_bps\":20"));
    }

    #[test]
    fn escape_neutralizes_control_characters() {
        // Labels are built from config-supplied hostnames. A raw newline would
        // end the Prometheus line early and corrupt every metric after it.
        assert_eq!(escape("a\nb"), "a\\nb");
        assert_eq!(escape("a\rb"), "a\\rb");
        assert_eq!(escape("a\tb"), "a\\tb");
        assert_eq!(escape("a\u{0}b"), "a\\u0000b");
        assert_eq!(escape(r#"quote" back\slash"#), r#"quote\" back\\slash"#);
        assert_eq!(escape("plain-host:80"), "plain-host:80");
    }

    #[test]
    fn prometheus_output_stays_one_metric_per_line() {
        let hostile = StatsSnapshot {
            label: "evil\nrfw_bytes_sent_total{label=\"spoof\"} 999".to_string(),
            ..snap()
        };
        let out = render_prometheus(&[hostile]);
        for line in out.lines() {
            assert!(
                line.starts_with('#') || line.starts_with("rfw_"),
                "stray line leaked into the exposition format: {line:?}"
            );
        }
    }

    #[test]
    fn prometheus_exposes_rejected_counter() {
        let out = render_prometheus(&[snap()]);
        assert!(out.contains("# TYPE rfw_rejected_connections_total counter"));
        assert!(out.contains("rfw_rejected_connections_total{label=\"a:1->b:2\"} 3"));
    }

    #[test]
    fn prometheus_empty_registry_is_still_valid() {
        // A scrape before any forwarder registers must return HELP/TYPE headers
        // and no samples, not an empty body.
        let out = render_prometheus(&[]);
        assert!(out.contains("# TYPE rfw_bytes_sent_total counter"));
        assert!(!out.contains("{label="));
    }

    #[test]
    fn json_is_well_formed_for_zero_one_and_many() {
        assert_eq!(render_json(&[]), "[]");

        let one = render_json(&[snap()]);
        assert_eq!(one.matches('{').count(), 1);
        assert!(one.contains("\"rejected_connections\":3"));

        let many = render_json(&[snap(), snap(), snap()]);
        assert_eq!(
            many.matches("},{").count(),
            2,
            "objects must be comma-joined"
        );
        assert_eq!(many.matches('{').count(), 3);
    }

    #[test]
    fn parse_request_target_rejects_junk() {
        assert_eq!(parse_request_target("GET"), None, "missing target");
        assert_eq!(parse_request_target("\r\n\r\n"), None);
        assert_eq!(parse_request_target("HEAD / HTTP/1.1"), None);
        assert_eq!(parse_request_target("DELETE /metrics HTTP/1.1"), None);
        // Extra whitespace is tolerated; the target is still the second token.
        assert_eq!(
            parse_request_target("GET   /stats   HTTP/1.1").as_deref(),
            Some("/stats")
        );
    }

    #[test]
    fn http_response_declares_an_accurate_content_length() {
        let body = "hello\n";
        let resp = http_response("200 OK", "text/plain", body);
        let (head, sent_body) = resp.split_once("\r\n\r\n").expect("head/body split");
        assert_eq!(sent_body, body);
        assert!(head.contains(&format!("Content-Length: {}", body.len())));
        assert!(head.contains("Connection: close"));
    }

    // ---- Live endpoint behaviour -------------------------------------------

    /// Spin up `serve` on an ephemeral port and return (port, cancel token).
    async fn spawn_endpoint(registry: Arc<StatsRegistry>) -> (u16, CancellationToken) {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move {
            serve(format!("127.0.0.1:{port}"), registry, c).await;
        });

        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return (port, cancel);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("metrics endpoint never came up on {port}");
    }

    async fn get(port: u16, raw: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(raw.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }

    #[tokio::test]
    async fn endpoint_routes_and_404s() {
        let reg = Arc::new(StatsRegistry::new());
        reg.register("a:1->b:2").await.add_sent(7);
        let (port, cancel) = spawn_endpoint(reg).await;

        let root = get(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(root.starts_with("HTTP/1.1 200 OK"), "{root}");
        assert!(root.contains("rfw ok"));

        let metrics = get(port, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(
            metrics.contains("rfw_bytes_sent_total{label=\"a:1->b:2\"} 7"),
            "{metrics}"
        );

        let stats = get(port, "GET /stats HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(stats.contains("\"bytes_sent\":7"), "{stats}");

        let missing = get(port, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "{missing}");

        // A non-GET method falls through to 404 rather than being served.
        let post = get(port, "POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(post.starts_with("HTTP/1.1 404 Not Found"), "{post}");

        cancel.cancel();
    }

    #[tokio::test]
    async fn endpoint_survives_a_client_that_sends_nothing() {
        // A slowloris client must not pin a task open or wedge the accept loop:
        // the next request has to keep working while it hangs.
        let reg = Arc::new(StatsRegistry::new());
        let (port, cancel) = spawn_endpoint(reg).await;

        let hung = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        let ok = get(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(ok.starts_with("HTTP/1.1 200 OK"), "{ok}");

        drop(hung);
        cancel.cancel();
    }

    #[tokio::test]
    async fn endpoint_caps_an_oversized_request_head() {
        // Headers beyond the cap are truncated rather than buffered without
        // bound; the request line still parses.
        let reg = Arc::new(StatsRegistry::new());
        let (port, cancel) = spawn_endpoint(reg).await;

        let padding = "X".repeat(MAX_REQUEST_BYTES * 2);
        let raw = format!("GET / HTTP/1.1\r\nHost: x\r\nBig: {padding}\r\n\r\n");
        let resp = get(port, &raw).await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");

        cancel.cancel();
    }

    #[tokio::test]
    async fn endpoint_stops_on_cancel() {
        let reg = Arc::new(StatsRegistry::new());
        let (port, cancel) = spawn_endpoint(reg).await;
        cancel.cancel();

        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                return;
            }
        }
        panic!("metrics endpoint still accepting after cancel");
    }
}
