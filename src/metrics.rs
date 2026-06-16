use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::stats::{StatsRegistry, StatsSnapshot};

/// Cap on the request bytes we read before giving up (headers only; GET has no body).
const MAX_REQUEST_BYTES: usize = 8192;

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
                        let reg = registry.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(stream, reg).await {
                                warn!(error = %e, "Metrics request error");
                            }
                        });
                    }
                    Err(e) => error!(error = %e, "Metrics accept failed"),
                }
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, registry: Arc<StatsRegistry>) -> std::io::Result<()> {
    // Read request head until CRLFCRLF or the cap is hit.
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0_u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= MAX_REQUEST_BYTES {
            break;
        }
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

/// Escape a label value for Prometheus / JSON string context.
fn escape(label: &str) -> String {
    label.replace('\\', "\\\\").replace('"', "\\\"")
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
            "{{\"label\":\"{}\",\"bytes_sent\":{},\"bytes_received\":{},\"active_connections\":{},\"rate_sent_bps\":{},\"rate_recv_bps\":{}}}",
            escape(&s.label),
            s.bytes_sent,
            s.bytes_received,
            s.active_connections,
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
}
