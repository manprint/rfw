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

/// Captured stdout+stderr lines of an `rfw` child, ANSI stripped.
pub struct LogTail {
    lines: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl LogTail {
    /// Poll every 50ms until a captured line contains `needle`; false on timeout.
    pub async fn wait_for(&self, needle: &str, within: Duration) -> bool {
        let deadline = std::time::Instant::now() + within;
        loop {
            if self.count(needle) > 0 {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Number of captured lines containing `needle`.
    pub fn count(&self, needle: &str) -> usize {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.contains(needle))
            .count()
    }

    /// Snapshot of every captured line so far.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }
}

/// Remove ANSI CSI sequences: ESC '[' ... final byte in 0x40..=0x7E.
pub fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for f in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&f) {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Like `spawn_rfw`, but stdout+stderr are piped and captured line by line (ANSI stripped).
/// Forces RUST_LOG=info; removes inherited RFW_FORWARDER_*, RFW_WEBHOOK_URL and
/// RFW_INSTANCE_NAME (so a developer shell can never leak a real webhook into a test),
/// then applies `envs`.
pub fn spawn_rfw_logged(args: &[&str], envs: &[(&str, &str)]) -> (ChildGuard, LogTail) {
    use std::io::{BufRead, BufReader};

    let mut cmd = rfw_command(args);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    for (key, _) in std::env::vars() {
        if key.starts_with("RFW_FORWARDER_") {
            cmd.env_remove(key);
        }
    }
    cmd.env_remove("RFW_WEBHOOK_URL")
        .env_remove("RFW_INSTANCE_NAME")
        .env("RUST_LOG", "info");
    for (k, v) in envs {
        cmd.env(k, v);
    }

    let mut child = cmd.spawn().expect("spawn rfw");
    let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    spawn_line_reader(stdout, lines.clone());
    spawn_line_reader(stderr, lines.clone());
    return (ChildGuard(child), LogTail { lines });

    fn spawn_line_reader<R: std::io::Read + Send + 'static>(
        pipe: R,
        lines: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        std::thread::spawn(move || {
            for line in BufReader::new(pipe).lines() {
                let Ok(line) = line else { break };
                lines.lock().unwrap().push(strip_ansi(&line));
            }
        });
    }
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

// ---------------------------------------------------------------------------
// UDP helpers
// ---------------------------------------------------------------------------

/// Reserve a free UDP port on 127.0.0.1 for a process that will bind it itself
/// (the `rfw` child). Claimed in the shared set, like [`free_port`].
pub fn free_udp_port() -> u16 {
    let mut claimed = claimed_ports().lock().unwrap();
    for _ in 0..1000 {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if claimed.insert(port) {
            return port;
        }
    }
    panic!("could not reserve an unused UDP port");
}

/// Reserve a port number that is free for TCP *and* UDP on 127.0.0.1.
pub fn free_dual_port() -> u16 {
    let mut claimed = claimed_ports().lock().unwrap();
    for _ in 0..1000 {
        let tcp = StdListener::bind("127.0.0.1:0").unwrap();
        let port = tcp.local_addr().unwrap().port();
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_err() {
            continue;
        }
        if claimed.insert(port) {
            return port;
        }
    }
    panic!("could not reserve a port free for both TCP and UDP");
}

/// Bind an in-process UDP socket on a port nothing else in this binary has claimed.
async fn bind_unclaimed_udp() -> tokio::net::UdpSocket {
    for _ in 0..1000 {
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = sock.local_addr().unwrap().port();
        if claimed_ports().lock().unwrap().insert(port) {
            return sock;
        }
    }
    panic!("could not bind an unclaimed UDP port");
}

/// Serve `sock`: answer every datagram with `tag` followed by the payload.
fn serve_udp(sock: tokio::net::UdpSocket, tag: &'static [u8]) {
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 65536];
        let mut out = Vec::with_capacity(65536 + tag.len());
        loop {
            let Ok((n, from)) = sock.recv_from(&mut buf).await else {
                break;
            };
            out.clear();
            out.extend_from_slice(tag);
            out.extend_from_slice(&buf[..n]);
            let _ = sock.send_to(&out, from).await;
        }
    });
}

/// In-process UDP echo server on a fresh ephemeral port.
pub async fn spawn_udp_echo() -> u16 {
    let sock = bind_unclaimed_udp().await;
    let port = sock.local_addr().unwrap().port();
    serve_udp(sock, b"");
    port
}

/// UDP echo server on a given port (e.g. one reserved with [`free_udp_port`]).
/// Panics if the bind fails.
pub async fn spawn_udp_echo_on(port: u16) {
    let sock = tokio::net::UdpSocket::bind(("127.0.0.1", port))
        .await
        .unwrap_or_else(|e| panic!("bind udp echo on {port}: {e}"));
    serve_udp(sock, b"");
}

/// UDP server answering every datagram with `tag` + payload: tells which backend answered.
pub async fn spawn_udp_tagged(tag: &'static [u8]) -> u16 {
    let sock = bind_unclaimed_udp().await;
    let port = sock.local_addr().unwrap().port();
    serve_udp(sock, tag);
    port
}

/// A UDP client socket bound to 127.0.0.1:0.
pub async fn udp_client() -> tokio::net::UdpSocket {
    bind_unclaimed_udp().await
}

/// Send `payload` to 127.0.0.1:`port` and wait up to `within` for one reply.
pub async fn udp_roundtrip(
    c: &tokio::net::UdpSocket,
    port: u16,
    payload: &[u8],
    within: Duration,
) -> Option<Vec<u8>> {
    c.send_to(payload, ("127.0.0.1", port)).await.ok()?;
    let mut buf = vec![0_u8; 65536];
    let (n, _) = tokio::time::timeout(within, c.recv_from(&mut buf))
        .await
        .ok()?
        .ok()?;
    buf.truncate(n);
    Some(buf)
}

/// Retry "ping" every 100ms for up to 10s until rfw answers; returns the reply.
/// Afterwards the client socket is drained, so late replies to earlier retries
/// cannot be mistaken for answers to later requests.
pub async fn wait_udp_forwarding(c: &tokio::net::UdpSocket, port: u16) -> Vec<u8> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if let Some(reply) = udp_roundtrip(c, port, b"ping", Duration::from_millis(100)).await {
            let mut buf = vec![0_u8; 65536];
            while let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(100), c.recv_from(&mut buf)).await
            {
            }
            return reply;
        }
    }
    panic!("udp port {port} never forwarded");
}

/// Poll every 50ms for up to 5s until binding UDP 127.0.0.1:`port` succeeds.
pub async fn wait_udp_port_free(port: u16) {
    for _ in 0..100 {
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("udp port {port} never became free");
}

/// Value of `"field":N` inside the /stats object whose label is `label`.
pub fn stat_of(body: &str, label: &str, field: &str) -> Option<u64> {
    let start = body.find(&format!("\"label\":\"{label}\""))?;
    let obj = &body[start..];
    let obj = &obj[..obj.find('}')?];
    let rest = &obj[obj.find(&format!("\"{field}\":"))? + field.len() + 3..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}
