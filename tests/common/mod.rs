//! Shared harness for the integration tests: spawn the real `rfw` binary,
//! talk to it over real sockets, and inspect its live stats endpoint.
//!
//! The forwarder binary is located via `CARGO_BIN_EXE_rfw` (set by Cargo for
//! integration tests), so no library target is required.

#![allow(dead_code)] // each test binary uses a different subset

use std::net::TcpListener as StdListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Ports this test binary has claimed, either by reserving them for an `rfw`
/// instance or by binding an in-process backend on them.
///
/// Tests run in parallel and every port here comes from an ephemeral bind, so
/// without a shared registry the OS happily hands the *same* port to a backend
/// that a pending `rfw` instance is about to bind. When that happened, a client
/// aimed at the forwarder reached an unrelated echo server instead and the test
/// failed on mismatched data rather than on anything real.
fn claimed_ports() -> &'static std::sync::Mutex<std::collections::HashSet<u16>> {
    use std::sync::{Mutex, OnceLock};
    static CLAIMED: OnceLock<Mutex<std::collections::HashSet<u16>>> = OnceLock::new();
    CLAIMED.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

/// Reserve a free localhost port for a process that will bind it itself
/// (i.e. the `rfw` child). The port is recorded so no in-process backend can
/// take it in the gap before `rfw` binds.
pub fn free_port() -> u16 {
    let mut claimed = claimed_ports().lock().unwrap();
    for _ in 0..1000 {
        let port = StdListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if claimed.insert(port) {
            return port;
        }
    }
    panic!("could not reserve an unused ephemeral port");
}

/// Bind an in-process listener on a port nothing else in this binary has
/// claimed. Retries if the OS offers a port already reserved for an `rfw`
/// instance.
async fn bind_unclaimed() -> TcpListener {
    for _ in 0..1000 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        if claimed_ports().lock().unwrap().insert(port) {
            return listener;
        }
        // Already reserved for something else: drop it and ask for another.
    }
    panic!("could not bind an unclaimed ephemeral port");
}

/// Kills the child rfw process when the guard drops (test end / panic).
pub struct ChildGuard(pub Child);

impl ChildGuard {
    /// Send SIGTERM and wait for the process to exit, up to `grace`.
    ///
    /// Returns `true` if it exited on its own — i.e. the signal handler ran.
    /// `false` means it had to be killed, which is exactly the pre-fix
    /// behaviour this exercises.
    #[cfg(unix)]
    pub fn terminate_and_wait(&mut self, grace: Duration) -> bool {
        let pid = self.0.id() as i32;
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let deadline = std::time::Instant::now() + grace;
        while std::time::Instant::now() < deadline {
            match self.0.try_wait() {
                Ok(Some(_)) => return true,
                Ok(None) => std::thread::sleep(Duration::from_millis(25)),
                Err(_) => return false,
            }
        }
        false
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Build the `rfw` command with file logging disabled (tests must not litter a
/// `logs/` directory, and the console output is captured to /dev/null anyway).
pub fn rfw_command(args: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rfw"));
    cmd.arg("--log-dir")
        .arg("")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

/// Spawn `rfw` with the given extra args. Panics if it cannot start.
pub fn spawn_rfw(args: &[&str]) -> ChildGuard {
    ChildGuard(rfw_command(args).spawn().expect("spawn rfw"))
}

/// In-process echo server: every connection echoes bytes back until EOF.
/// Binds an ephemeral port itself and returns it, so it can never collide with
/// a port another test is about to use.
pub async fn spawn_echo() -> u16 {
    let listener = bind_unclaimed().await;
    let port = listener.local_addr().unwrap().port();
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
    port
}

/// Server that answers every connection with a fixed tag and closes. Lets a
/// test tell *which* backend a forwarder is currently pointed at.
pub async fn spawn_tagged(tag: &'static [u8]) -> u16 {
    let listener = bind_unclaimed().await;
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut b = [0_u8; 16];
                let _ = sock.read(&mut b).await;
                let _ = sock.write_all(tag).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

/// Server that drains to EOF, then reports the total byte count it received.
/// Used to assert half-close propagation and byte-exactness together.
pub async fn spawn_counting_sink() -> u16 {
    let listener = bind_unclaimed().await;
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut total: u64 = 0;
                let mut buf = vec![0_u8; 65536];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => total += n as u64,
                    }
                }
                // Reaching here proves the client's half-close reached us.
                let _ = sock.write_all(format!("TOTAL={total}").as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

/// Poll until a TCP port accepts connections, or panic after ~5s.
pub async fn wait_port(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} never came up");
}

/// Poll until a TCP port stops accepting connections, or panic after ~5s.
pub async fn wait_port_closed(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} never stopped accepting");
}

/// Minimal HTTP GET against the metrics endpoint; returns the full response.
pub async fn http_get(port: u16, path: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf).to_string()
}

/// Poll `/stats` until `predicate` holds, or panic after `attempts * 100ms`.
pub async fn wait_for_stats(
    port: u16,
    attempts: usize,
    predicate: impl Fn(&str) -> bool,
) -> String {
    let mut last = String::new();
    for _ in 0..attempts {
        last = http_get(port, "/stats").await;
        if predicate(&last) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("stats never satisfied the predicate; last body: {last}");
}

/// Write `body` to `path`, replacing whatever was there.
pub fn write_config(path: &std::path::Path, body: &str) {
    std::fs::write(path, body).expect("write config");
}

/// A YAML config file in a unique temp dir, removed on drop.
pub struct TempConfig {
    pub dir: std::path::PathBuf,
    pub path: std::path::PathBuf,
}

impl TempConfig {
    pub fn new(name: &str, body: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "rfw-it-{name}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("forwarders.yml");
        write_config(&path, body);
        Self { dir, path }
    }

    pub fn rewrite(&self, body: &str) {
        write_config(&self.path, body);
    }

    pub fn path_str(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
