//! End-to-end tests: spawn the real `rfw` binary, forward TCP through it to an
//! in-process echo server, and assert byte-exact forwarding + live stats.
//!
//! The forwarder binary is located via `CARGO_BIN_EXE_rfw` (set by Cargo for
//! integration tests), so no library target is required.

use std::net::TcpListener as StdListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Grab a currently-free localhost port (small race window before rebind).
fn free_port() -> u16 {
    StdListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Kills the child rfw process when the guard drops (test end / panic).
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// In-process echo server: every connection echoes bytes back until EOF.
async fn spawn_echo(port: u16) {
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut buf = vec![0_u8; 65536];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if sock.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
}

/// Poll until a TCP port accepts connections, or panic after ~5s.
async fn wait_port(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} never came up");
}

/// Minimal HTTP GET against the metrics endpoint; returns the full response.
async fn http_get(port: u16, path: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).to_string()
}

#[tokio::test]
async fn bandwidth_lossless_and_metrics_live() {
    let echo_port = free_port();
    let local_port = free_port();
    let metrics_port = free_port();

    spawn_echo(echo_port).await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let child = Command::new(env!("CARGO_BIN_EXE_rfw"))
        .args([
            "--metrics-addr",
            &metrics_addr,
            "--report-interval",
            "1",
            "--sample-interval",
            "1",
            &fwd,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rfw");
    let _guard = ChildGuard(child);

    wait_port(local_port).await;
    wait_port(metrics_port).await;

    // Default 8 MiB (crosses many 64 KiB copy buffers); override via RFW_E2E_MB.
    let mb: usize = std::env::var("RFW_E2E_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let total = mb * 1024 * 1024;
    let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();

    // T-E2E1: send `total` bytes through the forwarder, read the echo back.
    let received = {
        let mut conn = TcpStream::connect(("127.0.0.1", local_port)).await.unwrap();
        let (mut rd, mut wr) = conn.split();
        let mut received = vec![0_u8; total];
        let writer = async {
            wr.write_all(&payload).await.unwrap();
            wr.shutdown().await.unwrap();
        };
        let reader = async {
            rd.read_exact(&mut received).await.unwrap();
        };
        tokio::join!(writer, reader);
        received
    }; // client connection dropped here

    assert_eq!(received, payload, "echoed bytes must be byte-identical");

    // T-E2E2: cumulative totals reflect the whole transfer, both directions.
    let body = http_get(metrics_port, "/stats").await;
    assert!(
        body.contains(&format!("\"bytes_sent\":{total}")),
        "bytes_sent should equal {total}: {body}"
    );
    assert!(
        body.contains(&format!("\"bytes_received\":{total}")),
        "bytes_received should equal {total}: {body}"
    );

    // And they must NOT reset across a report tick (report-interval = 1s).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let body2 = http_get(metrics_port, "/stats").await;
    assert!(
        body2.contains(&format!("\"bytes_sent\":{total}")),
        "cumulative total must persist (not reset): {body2}"
    );

    // Prometheus endpoint exposes the same lifetime counter.
    let prom = http_get(metrics_port, "/metrics").await;
    assert!(
        prom.contains("rfw_bytes_sent_total"),
        "prometheus output missing counter: {prom}"
    );

    // T-E2E3: active connection count returns to 0 after the client disconnects.
    let mut no_leak = false;
    for _ in 0..50 {
        if http_get(metrics_port, "/stats")
            .await
            .contains("\"active_connections\":0")
        {
            no_leak = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        no_leak,
        "active_connections must return to 0 after disconnect"
    );
}
