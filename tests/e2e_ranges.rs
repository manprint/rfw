//! End-to-end tests for port ranges, TCP and UDP, against the real binary.
//!
//! Every remote backend answers with its own port number, so a test can prove
//! that local port `L+k` reaches remote port `R+k` (a plain echo server would
//! pass even with the offset wrong). Local and remote blocks differ, so a
//! forwarder that ignored the remote offset would fail too.

mod common;

use std::time::Duration;

use common::{
    free_block, port_tag, rfw_command, spawn_port_tag_on, spawn_rfw, spawn_rfw_logged,
    spawn_udp_port_tag_on, udp_client, udp_roundtrip, wait_port, wait_port_closed,
    wait_udp_forwarding, wait_udp_port_free, ForkSafe, TempConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// One request/response through a forwarded TCP port; `None` if the port is
/// closed or the backend never answers.
async fn tcp_ask(port: u16) -> Option<String> {
    let mut c = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
    c.write_all(b"x").await.ok()?;
    let mut buf = [0_u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(3), c.read(&mut buf))
        .await
        .ok()?
        .ok()?;
    Some(String::from_utf8_lossy(&buf[..n]).to_string())
}

/// Assert that every `locals[k]` forwards to `remotes[k]`.
async fn assert_tcp_maps(locals: std::ops::Range<u16>, remotes: std::ops::Range<u16>) {
    for (l, r) in locals.zip(remotes) {
        assert_eq!(
            tcp_ask(l).await.as_deref(),
            Some(port_tag(r).as_str()),
            "local {l} must reach remote {r}"
        );
    }
}

async fn assert_udp_maps(locals: std::ops::Range<u16>, remotes: std::ops::Range<u16>) {
    let client = udp_client().await;
    for (l, r) in locals.zip(remotes) {
        let reply = wait_udp_forwarding(&client, l).await;
        assert_eq!(
            reply,
            port_tag(r).as_bytes(),
            "local {l} must reach remote {r}"
        );
    }
}

async fn spawn_tcp_backends(remotes: std::ops::Range<u16>) {
    for r in remotes {
        spawn_port_tag_on(r).await;
    }
}

async fn spawn_udp_backends(remotes: std::ops::Range<u16>) {
    for r in remotes {
        spawn_udp_port_tag_on(r).await;
    }
}

/// Poll until `local` answers with `want`, or fail after ~15s.
async fn wait_for_tag(local: u16, want: u16) {
    let mut last = None;
    for _ in 0..150 {
        last = tcp_ask(local).await;
        if last.as_deref() == Some(port_tag(want).as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("local {local} never reached remote {want}; last answer {last:?}");
}

fn yaml_range(lo: u16, hi: u16, remote: u16, extra: &str) -> String {
    format!(
        "forwarders:\n  \
         - local_host: \"127.0.0.1\"\n    \
           local_port: {lo}\n    \
           local_port_end: {hi}\n    \
           remote_host: \"127.0.0.1\"\n    \
           remote_port: {remote}\n{extra}"
    )
}

#[tokio::test]
async fn tcp_range_on_the_command_line_maps_port_for_port() {
    const N: u16 = 6;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_tcp_backends(remote..remote + N).await;

    let fwd = format!(
        "127.0.0.1:{local}-{}:127.0.0.1:{remote}-{}",
        local + N - 1,
        remote + N - 1
    );
    let _rfw = spawn_rfw(&[&fwd]);
    for p in local..local + N {
        wait_port(p).await;
    }
    assert_tcp_maps(local..local + N, remote..remote + N).await;
}

#[tokio::test]
async fn udp_range_on_the_command_line_maps_port_for_port() {
    const N: u16 = 6;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_udp_backends(remote..remote + N).await;

    let fwd = format!(
        "127.0.0.1:{local}-{}:127.0.0.1:{remote}-{}/udp",
        local + N - 1,
        remote + N - 1
    );
    let _rfw = spawn_rfw(&[&fwd]);
    assert_udp_maps(local..local + N, remote..remote + N).await;
}

/// The same block served over TCP and UDP at once, as the tunnel use case needs
/// (a TCP and a UDP range may share their local ports).
#[tokio::test]
async fn tcp_and_udp_ranges_share_the_same_local_ports() {
    const N: u16 = 4;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_tcp_backends(remote..remote + N).await;
    spawn_udp_backends(remote..remote + N).await;

    let span = |base: u16| format!("{base}-{}", base + N - 1);
    let tcp = format!("127.0.0.1:{}:127.0.0.1:{}", span(local), span(remote));
    let udp = format!("{tcp}/udp");
    let _rfw = spawn_rfw(&[&tcp, &udp]);
    for p in local..local + N {
        wait_port(p).await;
    }
    assert_tcp_maps(local..local + N, remote..remote + N).await;
    assert_udp_maps(local..local + N, remote..remote + N).await;
}

#[tokio::test]
async fn range_from_environment_variables_maps_port_for_port() {
    const N: u16 = 3;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_tcp_backends(remote..remote + N).await;
    spawn_udp_backends(remote..remote + N).await;

    let span = |base: u16| format!("{base}-{}", base + N - 1);
    let tcp = format!("127.0.0.1:{}:127.0.0.1:{}", span(local), span(remote));
    let udp = format!("{tcp}/udp");
    let (_rfw, log) = spawn_rfw_logged(
        &[],
        &[
            ("RFW_FORWARDER_1", tcp.as_str()),
            ("RFW_FORWARDER_2", udp.as_str()),
        ],
    );
    assert!(
        log.wait_for(
            &format!("Starting rfw with {} forwarder(s)", 2 * N),
            Duration::from_secs(10)
        )
        .await,
        "{:?}",
        log.lines()
    );
    for p in local..local + N {
        wait_port(p).await;
    }
    assert_tcp_maps(local..local + N, remote..remote + N).await;
    assert_udp_maps(local..local + N, remote..remote + N).await;
}

#[tokio::test]
async fn yaml_range_maps_port_for_port_and_mixes_with_single_entries() {
    const N: u16 = 5;
    let (local, remote) = (free_block(N + 1), free_block(N));
    spawn_tcp_backends(remote..remote + N).await;
    let single_remote = free_block(1);
    spawn_port_tag_on(single_remote).await;

    // local .. local+N-1 is the range, local+N a plain single-port entry
    let body = format!(
        "{}  - local_host: \"127.0.0.1\"\n    local_port: {}\n    remote_host: \"127.0.0.1\"\n    remote_port: {single_remote}\n",
        yaml_range(local, local + N - 1, remote, ""),
        local + N
    );
    let cfg = TempConfig::new("range-mixed", &body);
    let _rfw = spawn_rfw(&["-f", &cfg.path_str()]);
    for p in local..=local + N {
        wait_port(p).await;
    }
    assert_tcp_maps(local..local + N, remote..remote + N).await;
    assert_eq!(
        tcp_ask(local + N).await.as_deref(),
        Some(port_tag(single_remote).as_str())
    );
}

#[tokio::test]
async fn yaml_udp_range_with_explicit_remote_end() {
    const N: u16 = 4;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_udp_backends(remote..remote + N).await;

    let body = yaml_range(
        local,
        local + N - 1,
        remote,
        &format!(
            "    remote_port_end: {}\n    protocol: udp\n",
            remote + N - 1
        ),
    );
    let cfg = TempConfig::new("range-udp", &body);
    let _rfw = spawn_rfw(&["-f", &cfg.path_str()]);
    assert_udp_maps(local..local + N, remote..remote + N).await;
}

/// Shrinking a range on a live reload stops only the removed ports; the ones
/// that stay keep their listener and their open connections. Growing it again
/// brings the new ports up.
#[tokio::test]
async fn hot_reload_resizes_a_range_without_touching_the_ports_that_stay() {
    const N: u16 = 6;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_tcp_backends(remote..remote + N).await;

    let cfg = TempConfig::new("range-resize", &yaml_range(local, local + 4, remote, ""));
    let _rfw = spawn_rfw(&["-f", &cfg.path_str()]);
    for p in local..local + 5 {
        wait_port(p).await;
    }
    assert_tcp_maps(local..local + 5, remote..remote + 5).await;
    assert!(
        tcp_ask(local + 5).await.is_none(),
        "port past the range is closed"
    );

    // A connection that is open across the reload on a port that stays.
    let mut live = TcpStream::connect(("127.0.0.1", local + 1)).await.unwrap();
    live.write_all(b"x").await.unwrap();
    let mut buf = [0_u8; 64];
    let n = live.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], port_tag(remote + 1).as_bytes());

    // shrink 0..=4 -> 0..=2
    cfg.rewrite(&yaml_range(local, local + 2, remote, ""));
    wait_port_closed(local + 3).await;
    wait_port_closed(local + 4).await;
    assert_tcp_maps(local..local + 3, remote..remote + 3).await;

    live.write_all(b"x").await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(3), live.read(&mut buf))
        .await
        .expect("open connection on a surviving port must stay alive")
        .unwrap();
    assert_eq!(&buf[..n], port_tag(remote + 1).as_bytes());

    // grow 0..=2 -> 0..=5: the new ports appear with the right remote offsets
    cfg.rewrite(&yaml_range(local, local + 5, remote, ""));
    for k in 3..6 {
        wait_for_tag(local + k, remote + k).await;
    }
    assert_tcp_maps(local..local + 6, remote..remote + 6).await;
}

/// A reload that produces an invalid range must leave the running forwarders
/// untouched, and a later valid one must apply.
#[tokio::test]
async fn hot_reload_to_an_invalid_range_keeps_the_running_configuration() {
    const N: u16 = 4;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_tcp_backends(remote..remote + N).await;

    let cfg = TempConfig::new(
        "range-bad-reload",
        &yaml_range(local, local + 1, remote, ""),
    );
    let _rfw = spawn_rfw(&["-f", &cfg.path_str()]);
    wait_port(local).await;
    wait_port(local + 1).await;

    // length mismatch: 4 local ports, remote range of 2
    cfg.rewrite(&yaml_range(
        local,
        local + 3,
        remote,
        &format!("    remote_port_end: {}\n", remote + 1),
    ));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_tcp_maps(local..local + 2, remote..remote + 2).await;
    assert!(tcp_ask(local + 2).await.is_none(), "invalid reload applied");

    cfg.rewrite(&yaml_range(local, local + 3, remote, ""));
    wait_for_tag(local + 3, remote + 3).await;
    assert_tcp_maps(local..local + 4, remote..remote + 4).await;
}

/// A UDP range shrinks like a TCP one: the dropped ports free their sockets.
#[tokio::test]
async fn hot_reload_shrinks_a_udp_range() {
    const N: u16 = 4;
    let (local, remote) = (free_block(N), free_block(N));
    spawn_udp_backends(remote..remote + N).await;

    let body = |hi: u16| yaml_range(local, hi, remote, "    protocol: udp\n");
    let cfg = TempConfig::new("range-udp-resize", &body(local + 3));
    let _rfw = spawn_rfw(&["-f", &cfg.path_str()]);
    assert_udp_maps(local..local + 4, remote..remote + 4).await;

    cfg.rewrite(&body(local + 1));
    wait_udp_port_free(local + 2).await;
    wait_udp_port_free(local + 3).await;
    assert_udp_maps(local..local + 2, remote..remote + 2).await;
    let client = udp_client().await;
    assert!(
        udp_roundtrip(&client, local + 2, b"x", Duration::from_millis(300))
            .await
            .is_none(),
        "dropped udp port must not answer"
    );
}

/// Run rfw with these arguments and no RFW_FORWARDER_* in the environment;
/// returns (exit success, stderr).
fn run_rfw(args: &[&str]) -> (bool, String) {
    let mut cmd = rfw_command(args);
    for (key, _) in std::env::vars() {
        if key.starts_with("RFW_FORWARDER_") {
            cmd.env_remove(key);
        }
    }
    let out = cmd
        .stderr(std::process::Stdio::piped())
        .output_safe()
        .expect("run rfw");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[test]
fn startup_rejects_a_range_whose_sides_differ_in_length() {
    let p = free_block(20);
    let fwd = format!("127.0.0.1:{p}-{}:127.0.0.1:1-10", p + 19);
    let (ok, err) = run_rfw(&[&fwd]);
    assert!(!ok);
    assert!(err.contains("length mismatch"), "{err}");
    assert!(err.contains("20 port(s)"), "{err}");
}

#[test]
fn startup_rejects_overlapping_ranges_and_names_both_entries() {
    let p = free_block(30);
    let a = format!("127.0.0.1:{p}-{}:127.0.0.1:1-20", p + 19);
    let b = format!("127.0.0.1:{}-{}:127.0.0.1:101-120/tcp", p + 10, p + 29);
    let (ok, err) = run_rfw(&[&a, &b]);
    assert!(!ok);
    assert!(
        err.contains(&format!("Duplicate local address '127.0.0.1:{}'", p + 10)),
        "{err}"
    );
    assert!(
        err.contains(&format!("127.0.0.1:{p}-{}->127.0.0.1:1-20", p + 19)),
        "{err}"
    );
    assert!(err.contains("defined by both"), "{err}");
}

#[test]
fn startup_rejects_a_reversed_range_and_port_zero() {
    let (ok, err) = run_rfw(&["127.0.0.1:30-20:127.0.0.1:30-20"]);
    assert!(!ok);
    assert!(err.contains("greater than end"), "{err}");

    let (ok, err) = run_rfw(&["127.0.0.1:0-5:127.0.0.1:1-6"]);
    assert!(!ok);
    assert!(err.contains("port 0"), "{err}");
}

#[test]
fn startup_rejects_a_yaml_range_with_remote_end_but_no_local_end() {
    let cfg = TempConfig::new(
        "range-bad-yaml",
        "forwarders:\n  - {local_host: 127.0.0.1, local_port: 1000, remote_host: 127.0.0.1, remote_port: 1, remote_port_end: 9}\n",
    );
    let (ok, err) = run_rfw(&["-f", &cfg.path_str()]);
    assert!(!ok);
    assert!(err.contains("needs local_port_end"), "{err}");
}
