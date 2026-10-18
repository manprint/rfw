use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Instant;

/// `--key value` / `--flag` argument bag.
pub struct Args(HashMap<String, String>);

impl Args {
    pub fn parse(argv: &[String]) -> Args {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < argv.len() {
            if let Some(k) = argv[i].strip_prefix("--") {
                if i + 1 < argv.len() && !argv[i + 1].starts_with("--") {
                    m.insert(k.to_string(), argv[i + 1].clone());
                    i += 2;
                } else {
                    m.insert(k.to_string(), "true".to_string());
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        Args(m)
    }
    pub fn str(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    pub fn num<T: std::str::FromStr>(&self, k: &str, d: T) -> T {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    pub fn flag(&self, k: &str) -> bool {
        self.0.contains_key(k)
    }
}

pub fn resolve(host: &str, port: u16) -> SocketAddr {
    (host, port)
        .to_socket_addrs()
        .unwrap_or_else(|e| die(&format!("cannot resolve {host}:{port}: {e}")))
        .find(|a| a.is_ipv4())
        .unwrap_or_else(|| die(&format!("no IPv4 for {host}")))
}

pub fn die(msg: &str) -> ! {
    eprintln!("rfwbench: {msg}");
    std::process::exit(2)
}

/// Raise the soft RLIMIT_NOFILE to the hard limit (many-connection modes).
pub fn raise_nofile() -> u64 {
    unsafe {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 {
            rl.rlim_cur = rl.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
            libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl);
        }
        rl.rlim_cur as u64
    }
}

pub fn set_i32(fd: i32, level: i32, name: i32, val: i32) {
    unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<i32>() as u32,
        );
    }
}

pub fn ns_since(t0: Instant) -> u64 {
    t0.elapsed().as_nanos() as u64
}

/// xorshift64* stream used by the integrity check (deterministic per seed).
pub struct Prng(pub u64);
impl Prng {
    pub fn fill(&mut self, buf: &mut [u8]) {
        for c in buf.chunks_mut(8) {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            let v = x.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes();
            c.copy_from_slice(&v[..c.len()]);
        }
    }
}
