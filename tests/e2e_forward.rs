//! End-to-end tests: spawn the real `rfw` binary, forward TCP through it to an
//! in-process echo server, and assert byte-exact forwarding + live stats.

mod common;

use std::time::Duration;

use common::{free_port, http_get, spawn_echo, spawn_rfw, wait_port};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn bandwidth_lossless_and_metrics_live() {
    run_lossless_scenario(&[]).await;
}

/// The Linux `splice(2)` zero-copy path must be just as lossless as the portable
/// copy path — byte-exact end-to-end with correct cumulative counters.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn bandwidth_lossless_via_splice() {
    run_lossless_scenario(&["--splice"]).await;
}

/// Drive one full transfer through a freshly-spawned `rfw` and assert losslessness,
/// live cumulative metrics, no reset, and no active-connection leak. `extra_args`
/// are appended to the forwarder command (e.g. `--splice`).
async fn run_lossless_scenario(extra_args: &[&str]) {
    let local_port = free_port();
    let metrics_port = free_port();
    let echo_port = spawn_echo().await;

    let fwd = format!("127.0.0.1:{local_port}:127.0.0.1:{echo_port}");
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let mut args = vec![
        "--metrics-addr",
        &metrics_addr,
        "--report-interval",
        "1",
        "--sample-interval",
        "1",
    ];
    args.extend_from_slice(extra_args);
    args.push(&fwd);
    let _guard = spawn_rfw(&args);

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
