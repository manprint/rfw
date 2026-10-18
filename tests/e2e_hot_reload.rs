//! Hot-reload tests against the real binary.
//!
//! Hot-reload is the feature with the most moving parts — a filesystem watcher,
//! a debounce, a partial-parse window, and a listener that has to be torn down
//! and rebuilt on a live port — so it gets its own suite.

mod common;

use std::time::Duration;

use common::{
    free_port, http_get, spawn_echo, spawn_rfw, spawn_rfw_logged, spawn_tagged, wait_port,
    TempConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Open a connection through the forwarder and read whatever the backend
/// answers. Used to identify which backend is currently wired up.
async fn probe(port: u16) -> Option<String> {
    let mut c = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
    c.write_all(b"ping").await.ok()?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), c.read_to_end(&mut buf))
        .await
        .ok()?
        .ok()?;
    Some(String::from_utf8_lossy(&buf).to_string())
}

/// Poll `probe` until it returns `want`, or fail after ~15s.
///
/// The window is generous on purpose: it has to cover the watcher's debounce
/// plus the settle delay. It is *not* generous enough to hide the 5s bind
/// backoff that the rebind race used to cost.
async fn wait_for_backend(port: u16, want: &str) {
    let mut last = None;
    for _ in 0..150 {
        let got = probe(port).await;
        if got.as_deref() == Some(want) {
            return;
        }
        last = got;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("forwarder never switched to {want:?}; last response was {last:?}");
}

fn config_with(local_port: u16, remote_port: u16, settings: &str) -> String {
    format!(
        "forwarders:\n  \
         - local_host: \"127.0.0.1\"\n    \
           local_port: {local_port}\n    \
           remote_host: \"127.0.0.1\"\n    \
           remote_port: {remote_port}\n{settings}"
    )
}

/// Re-targeting a forwarder keeps its local port but changes its label, so the
/// manager has to stop the old accept loop and start a new one on the *same*
/// port. It used to do that without waiting for the old listener to close,
/// hitting EADDRINUSE and leaving the port refusing connections for the whole
/// 5-second bind backoff.
#[tokio::test]
async fn retargeting_a_forwarder_rebinds_its_port_without_an_outage() {
    let local_port = free_port();
    let a_port = spawn_tagged(b"SRV-A").await;
    let b_port = spawn_tagged(b"SRV-B").await;

    let cfg = TempConfig::new("retarget", &config_with(local_port, a_port, ""));
    let _guard = spawn_rfw(&["-f", &cfg.path_str()]);
    wait_port(local_port).await;
    wait_for_backend(local_port, "SRV-A").await;

    cfg.rewrite(&config_with(local_port, b_port, ""));
    wait_for_backend(local_port, "SRV-B").await;

    // And back again, to prove the teardown/rebuild cycle is repeatable rather
    // than working once by luck.
    cfg.rewrite(&config_with(local_port, a_port, ""));
    wait_for_backend(local_port, "SRV-A").await;
}

/// Adding and removing forwarders must not disturb the ones that stayed.
#[tokio::test]
async fn adding_and_removing_forwarders_leaves_the_others_alone() {
    let keep_port = free_port();
    let add_port = free_port();
    let keep_backend = spawn_tagged(b"KEEP").await;
    let add_backend = spawn_tagged(b"ADDED").await;

    let one = format!(
        "forwarders:\n  - {{local_host: \"127.0.0.1\", local_port: {keep_port}, remote_host: \"127.0.0.1\", remote_port: {keep_backend}}}\n"
    );
    let two = format!(
        "{one}  - {{local_host: \"127.0.0.1\", local_port: {add_port}, remote_host: \"127.0.0.1\", remote_port: {add_backend}}}\n"
    );

    let cfg = TempConfig::new("addremove", &one);
    let _guard = spawn_rfw(&["-f", &cfg.path_str()]);
    wait_port(keep_port).await;
    wait_for_backend(keep_port, "KEEP").await;

    // Add the second forwarder.
    cfg.rewrite(&two);
    wait_for_backend(add_port, "ADDED").await;
    assert_eq!(
        probe(keep_port).await.as_deref(),
        Some("KEEP"),
        "the untouched forwarder must keep serving across a reload"
    );

    // Remove it again.
    cfg.rewrite(&one);
    for _ in 0..150 {
        if TcpStream::connect(("127.0.0.1", add_port)).await.is_err() {
            assert_eq!(
                probe(keep_port).await.as_deref(),
                Some("KEEP"),
                "removing a forwarder must not disturb the rest"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("removed forwarder kept accepting on {add_port}");
}

/// An editor saving a large file, or a typo, leaves the config briefly
/// unparseable. The reload used to apply the `settings:` block and the
/// `forwarders:` list in two separate loads, so a failed parse reset every
/// data-plane knob to its default while logging only "Failed to reload config"
/// — the forwarders kept running with silently different buffer settings.
/// Both halves now come from one parse, so a bad file changes nothing.
#[tokio::test]
async fn a_malformed_config_changes_nothing() {
    let local_port = free_port();
    let backend = spawn_tagged(b"SRV-A").await;

    let good = config_with(
        local_port,
        backend,
        "settings:\n  buffer_bytes: 131072\n  use_splice: false\n",
    );
    let cfg = TempConfig::new("malformed", &good);
    let _guard = spawn_rfw(&["-f", &cfg.path_str()]);
    wait_port(local_port).await;
    wait_for_backend(local_port, "SRV-A").await;

    // Truncated mid-key, exactly like a partial write.
    cfg.rewrite("forwarders:\n  - local_host: \"127.0.0.1\"\n    local_p");
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        probe(local_port).await.as_deref(),
        Some("SRV-A"),
        "a malformed config must leave the running forwarders untouched"
    );

    // And the process must recover when the file becomes valid again.
    cfg.rewrite(&good);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(probe(local_port).await.as_deref(), Some("SRV-A"));
}

/// The reload path skipped duplicate-local-address validation that the startup
/// path performed, so an edited file with two forwarders on one port started
/// both: one served traffic, the other retried its bind every 5 seconds
/// forever. A duplicate must now be rejected like any other bad config.
#[tokio::test]
async fn duplicate_local_ports_in_a_reload_are_rejected() {
    let local_port = free_port();
    let clash_port = free_port();
    let backend = spawn_tagged(b"SRV-A").await;
    let other = spawn_tagged(b"SRV-B").await;

    let good = config_with(local_port, backend, "");
    let cfg = TempConfig::new("dupes", &good);
    let _guard = spawn_rfw(&["-f", &cfg.path_str()]);
    wait_port(local_port).await;
    wait_for_backend(local_port, "SRV-A").await;

    // Two entries claiming the same local port.
    let duplicated = format!(
        "forwarders:\n  \
         - {{local_host: \"127.0.0.1\", local_port: {clash_port}, remote_host: \"127.0.0.1\", remote_port: {backend}}}\n  \
         - {{local_host: \"127.0.0.1\", local_port: {clash_port}, remote_host: \"127.0.0.1\", remote_port: {other}}}\n"
    );
    cfg.rewrite(&duplicated);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The whole reload is refused, so the previous config is still serving...
    assert_eq!(
        probe(local_port).await.as_deref(),
        Some("SRV-A"),
        "a config with duplicate local addresses must be rejected wholesale"
    );
    // ...and neither duplicate was started.
    assert!(
        TcpStream::connect(("127.0.0.1", clash_port)).await.is_err(),
        "no forwarder should have been started from the rejected config"
    );
}

/// Data-plane knobs are documented as hot-reloadable. Changing them must apply
/// to new connections without interrupting the listener.
#[tokio::test]
async fn data_plane_knobs_reload_without_dropping_the_listener() {
    let local_port = free_port();
    let echo_port = spawn_echo().await;

    let base = format!(
        "forwarders:\n  - {{local_host: \"127.0.0.1\", local_port: {local_port}, remote_host: \"127.0.0.1\", remote_port: {echo_port}}}\n"
    );
    let cfg = TempConfig::new("knobs", &format!("{base}settings:\n  buffer_bytes: 8192\n"));
    let _guard = spawn_rfw(&["-f", &cfg.path_str()]);
    wait_port(local_port).await;

    let echo_roundtrip = |port: u16| async move {
        let payload = vec![b'K'; 256 * 1024];
        let mut c = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (mut rd, mut wr) = c.split();
        let mut got = vec![0_u8; payload.len()];
        let w = async {
            wr.write_all(&payload).await.unwrap();
            wr.shutdown().await.unwrap();
        };
        let r = async { rd.read_exact(&mut got).await.unwrap() };
        tokio::join!(w, r);
        assert_eq!(got, payload);
    };

    echo_roundtrip(local_port).await;

    // Raise the copy buffer and re-run: traffic must keep flowing byte-exactly.
    cfg.rewrite(&format!("{base}settings:\n  buffer_bytes: 262144\n"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    echo_roundtrip(local_port).await;

    // A reload that sets a dangerous value must be clamped rather than
    // installing a copy buffer that spins the CPU and forwards nothing.
    cfg.rewrite(&format!("{base}settings:\n  buffer_bytes: 0\n"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    tokio::time::timeout(Duration::from_secs(20), echo_roundtrip(local_port))
        .await
        .expect("a zero buffer_bytes in a reload must be clamped, not wedge the data path");
}

/// A reload must not reset the lifetime counters of forwarders it leaves alone
/// — a Prometheus counter that goes backwards is a broken rate in every
/// dashboard built on it.
#[tokio::test]
async fn reload_preserves_counters_of_untouched_forwarders() {
    let keep_port = free_port();
    let add_port = free_port();
    let metrics_port = free_port();
    let echo_port = spawn_echo().await;
    let other_backend = spawn_tagged(b"OTHER").await;

    let one = format!(
        "forwarders:\n  - {{local_host: \"127.0.0.1\", local_port: {keep_port}, remote_host: \"127.0.0.1\", remote_port: {echo_port}}}\n"
    );
    let cfg = TempConfig::new("counters", &one);
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    let _guard = spawn_rfw(&["-f", &cfg.path_str(), "--metrics-addr", &metrics_addr]);
    wait_port(keep_port).await;
    wait_port(metrics_port).await;

    // Push a known amount of traffic through the forwarder we will keep.
    const N: usize = 32 * 1024;
    {
        let payload = vec![b'C'; N];
        let mut c = TcpStream::connect(("127.0.0.1", keep_port)).await.unwrap();
        let (mut rd, mut wr) = c.split();
        let mut got = vec![0_u8; N];
        let w = async {
            wr.write_all(&payload).await.unwrap();
            wr.shutdown().await.unwrap();
        };
        let r = async { rd.read_exact(&mut got).await.unwrap() };
        tokio::join!(w, r);
    }

    let before = http_get(metrics_port, "/stats").await;
    assert!(
        before.contains(&format!("\"bytes_sent\":{N}")),
        "baseline traffic must be counted: {before}"
    );

    // Add an unrelated forwarder; the existing one is untouched.
    cfg.rewrite(&format!(
        "{one}  - {{local_host: \"127.0.0.1\", local_port: {add_port}, remote_host: \"127.0.0.1\", remote_port: {other_backend}}}\n"
    ));
    wait_port(add_port).await;

    let after = http_get(metrics_port, "/stats").await;
    assert!(
        after.contains(&format!("\"bytes_sent\":{N}")),
        "a reload must not reset the counters of a forwarder it did not change: {after}"
    );
    assert_eq!(
        after.matches("\"label\"").count(),
        2,
        "both forwarders must be exposed exactly once: {after}"
    );
}

/// B1: forwarders given on the command line stay pinned across a reload; the
/// file's `forwarders:` list is ignored (its settings still apply).
#[tokio::test]
async fn reload_keeps_cli_forwarders_and_ignores_the_files_list() {
    let cli_port = free_port();
    let file_port = free_port();
    let a = spawn_tagged(b"A").await;
    let b = spawn_tagged(b"B").await;

    let cfg = TempConfig::new("pinned-cli", &config_with(file_port, b, ""));
    let (_guard, log) = spawn_rfw_logged(
        &[
            "-f",
            &cfg.path_str(),
            &format!("127.0.0.1:{cli_port}:127.0.0.1:{a}"),
        ],
        &[],
    );
    wait_port(cli_port).await;
    assert!(
        log.wait_for(
            "Forwarders come from command-line arguments",
            Duration::from_secs(5)
        )
        .await,
        "{:?}",
        log.lines()
    );
    assert!(
        log.wait_for("Watching config file", Duration::from_secs(5))
            .await,
        "{:?}",
        log.lines()
    );

    cfg.rewrite(&config_with(
        file_port,
        b,
        "settings:\n  probe_interval_secs: 7\n",
    ));
    assert!(
        log.wait_for(
            "forwarders list is ignored on reload",
            Duration::from_secs(15)
        )
        .await,
        "{:?}",
        log.lines()
    );
    assert!(
        log.wait_for(
            "Config reloaded: 1 forwarders active",
            Duration::from_secs(5)
        )
        .await,
        "{:?}",
        log.lines()
    );

    assert_eq!(probe(cli_port).await.as_deref(), Some("A"));
    assert!(TcpStream::connect(("127.0.0.1", file_port)).await.is_err());
}

/// B1: same, for forwarders that came from `RFW_FORWARDER_N`.
#[tokio::test]
async fn reload_keeps_env_forwarders() {
    let env_port = free_port();
    let file_port = free_port();
    let a = spawn_tagged(b"A").await;
    let b = spawn_tagged(b"B").await;

    let cfg = TempConfig::new("pinned-env", &config_with(file_port, b, ""));
    let fwd = format!("127.0.0.1:{env_port}:127.0.0.1:{a}");
    let (_guard, log) = spawn_rfw_logged(
        &["-f", &cfg.path_str()],
        &[("RFW_FORWARDER_1", fwd.as_str())],
    );
    wait_port(env_port).await;
    assert!(
        log.wait_for(
            "Forwarders come from RFW_FORWARDER_N environment variables",
            Duration::from_secs(5)
        )
        .await,
        "{:?}",
        log.lines()
    );
    assert!(
        log.wait_for("Watching config file", Duration::from_secs(5))
            .await,
        "{:?}",
        log.lines()
    );

    cfg.rewrite(&config_with(
        file_port,
        b,
        "settings:\n  probe_interval_secs: 7\n",
    ));
    assert!(
        log.wait_for(
            "forwarders list is ignored on reload",
            Duration::from_secs(15)
        )
        .await,
        "{:?}",
        log.lines()
    );
    assert!(
        log.wait_for(
            "Config reloaded: 1 forwarders active",
            Duration::from_secs(5)
        )
        .await,
        "{:?}",
        log.lines()
    );

    assert_eq!(probe(env_port).await.as_deref(), Some("A"));
    assert!(TcpStream::connect(("127.0.0.1", file_port)).await.is_err());
}
