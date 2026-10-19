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
        #[allow(clippy::unnecessary_cast)] // rlim_t is u32 on 32-bit targets
        {
            rl.rlim_cur as u64
        }
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

/// `pin --cpus 0,2 --pids 12,34`: sched_setaffinity on every thread of every
/// pid (run from a privileged --pid=host container to pin root-owned
/// processes such as docker-proxy, which Docker itself starts unpinned).
pub fn pin(a: &Args) {
    let cpus: Vec<usize> = a
        .str("cpus", "0")
        .split(',')
        .filter_map(|c| c.trim().parse().ok())
        .collect();
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for c in &cpus {
        unsafe { libc::CPU_SET(*c, &mut set) };
    }
    let (mut ok, mut fail) = (0, 0);
    for pid in a.str("pids", "").split(',').filter(|p| !p.is_empty()) {
        let Ok(rd) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
            fail += 1;
            continue;
        };
        for t in rd.flatten() {
            let tid: i32 = t.file_name().to_string_lossy().parse().unwrap_or(0);
            let r = unsafe {
                libc::sched_setaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &set)
            };
            if r == 0 {
                ok += 1;
            } else {
                fail += 1;
            }
        }
    }
    println!("{{\"pinned_threads\":{ok},\"failed\":{fail}}}");
}

/// `pipeprobe --count N --size B`: create N pipes, ask for B bytes of capacity
/// (as rfw --splice does) and report what the kernel granted. Shows the
/// fs.pipe-user-pages-soft limit biting: past it, unprivileged pipes shrink to
/// one page and F_SETPIPE_SZ fails with EPERM.
pub fn pipeprobe(a: &Args) {
    let count: usize = a.num("count", 100);
    let size: i32 = a.num("size", 65536);
    let mut fds = Vec::new();
    let (mut set_fail, mut min_cap, mut small) = (0, i32::MAX, 0);
    let mut first_small: i64 = -1;
    for i in 0..count {
        let mut p = [0i32; 2];
        if unsafe { libc::pipe2(p.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            break;
        }
        if unsafe { libc::fcntl(p[1], libc::F_SETPIPE_SZ, size) } < 0 {
            set_fail += 1;
        }
        let cap = unsafe { libc::fcntl(p[1], libc::F_GETPIPE_SZ) };
        min_cap = min_cap.min(cap);
        if cap < 65536 {
            small += 1;
            if first_small < 0 {
                first_small = i as i64;
            }
        }
        fds.push(p);
    }
    println!(
        "{{\"pipes\":{},\"requested\":{size},\"set_failures\":{set_fail},\"below_64k\":{small},\"first_below_64k_at\":{first_small},\"min_capacity\":{},\"uid\":{}}}",
        fds.len(),
        if min_cap == i32::MAX { 0 } else { min_cap },
        unsafe { libc::getuid() }
    );
}
