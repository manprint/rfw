//! TCP client modes: rr (closed/open loop request-response), cps (connection
//! rate), hold (idle connections, for memory), integrity (checksummed stream).

use crate::hist::Hist;
use crate::util::*;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn connect(host: &str, port: u16) -> std::io::Result<TcpStream> {
    let s = TcpStream::connect_timeout(&resolve(host, port), Duration::from_secs(5))?;
    s.set_nodelay(true)?;
    Ok(s)
}

/// Request/response against `base + (i % ports)`.
///
/// Closed loop (default): next request goes out as soon as the reply arrives.
/// Open loop (`--rate R`, total req/s): request k of a connection is *due* at a
/// fixed time and latency is measured from that due time, so a stalled
/// forwarder is charged for the queueing it causes (no coordinated omission).
pub fn rr(a: &Args) {
    raise_nofile();
    let host = a.str("host", "127.0.0.1");
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let conns: usize = a.num("conns", 1);
    let size: usize = a.num("size", 64).max(8);
    let dur: f64 = a.num("duration", 10.0);
    let warm: f64 = a.num("warmup", 1.0);
    let rate: f64 = a.num("rate", 0.0);
    let errors = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let measure_from = Duration::from_secs_f64(warm);
    let end = Duration::from_secs_f64(warm + dur);
    let period = if rate > 0.0 { Duration::from_secs_f64(conns as f64 / rate) } else { Duration::ZERO };

    let hs: Vec<_> = (0..conns)
        .map(|i| {
            let host = host.clone();
            let errors = errors.clone();
            std::thread::spawn(move || {
                let mut h = Hist::new();
                let port = base + (i as u16 % ports);
                let mut s = match connect(&host, port) {
                    Ok(s) => s,
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        return h;
                    }
                };
                let mut req = vec![0u8; size];
                let mut rsp = vec![0u8; size];
                // Stagger open-loop connections across one period.
                let mut due = t0 + measure_from.min(Duration::from_millis(0))
                    + if rate > 0.0 { period.mul_f64(i as f64 / conns as f64) } else { Duration::ZERO };
                let mut seq: u64 = 0;
                loop {
                    let now = Instant::now();
                    if now.duration_since(t0) >= end {
                        break;
                    }
                    let sent_at = if rate > 0.0 {
                        if due > now {
                            std::thread::sleep(due - now);
                        }
                        let d = due;
                        due += period;
                        d
                    } else {
                        Instant::now()
                    };
                    req[..8].copy_from_slice(&seq.to_le_bytes());
                    seq += 1;
                    if s.write_all(&req).is_err() || s.read_exact(&mut rsp).is_err() || rsp[..8] != req[..8] {
                        errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    if sent_at.duration_since(t0) >= measure_from {
                        h.record(sent_at.elapsed().as_nanos() as u64);
                    }
                }
                h
            })
        })
        .collect();
    let mut total = Hist::new();
    for h in hs {
        if let Ok(h) = h.join() {
            total.merge(&h);
        }
    }
    let secs = dur.max(0.001);
    println!(
        "{{\"mode\":\"rr\",\"conns\":{conns},\"size\":{size},\"rate_target\":{rate},\"msgs\":{},\"msgs_per_s\":{:.0},\"mbit_per_s\":{:.1},\"errors\":{},\"lat_us\":{}}}",
        total.n,
        total.n as f64 / secs,
        total.n as f64 * size as f64 * 2.0 * 8.0 / secs / 1e6,
        errors.load(Ordering::Relaxed),
        total.json_us()
    );
}

/// Connection-per-request: connect, one `size`-byte round trip, close.
/// Fixed total count (bounded TIME_WAIT/conntrack use), spread over `ports`
/// destination ports so no 4-tuple space is exhausted.
pub fn cps(a: &Args) {
    raise_nofile();
    let host = a.str("host", "127.0.0.1");
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let conns: usize = a.num("conns", 16);
    let total: u64 = a.num("total", 20000);
    let size: usize = a.num("size", 1).max(1);
    let next = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let hs: Vec<_> = (0..conns)
        .map(|_| {
            let (host, next, errors) = (host.clone(), next.clone(), errors.clone());
            std::thread::spawn(move || {
                let mut h = Hist::new();
                let mut hc = Hist::new();
                let req = vec![7u8; size];
                let mut rsp = vec![0u8; size];
                loop {
                    let k = next.fetch_add(1, Ordering::Relaxed);
                    if k >= total {
                        break;
                    }
                    let t = Instant::now();
                    let port = base + (k % ports as u64) as u16;
                    let r = connect(&host, port).and_then(|mut s| {
                        hc.record(t.elapsed().as_nanos() as u64);
                        s.write_all(&req)?;
                        s.read_exact(&mut rsp)?;
                        Ok(())
                    });
                    match r {
                        Ok(()) => h.record(t.elapsed().as_nanos() as u64),
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                (h, hc)
            })
        })
        .collect();
    let mut total_h = Hist::new();
    let mut conn_h = Hist::new();
    for h in hs {
        if let Ok((h, hc)) = h.join() {
            total_h.merge(&h);
            conn_h.merge(&hc);
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "{{\"mode\":\"cps\",\"conns\":{conns},\"done\":{},\"secs\":{:.3},\"cps\":{:.0},\"errors\":{},\"txn_lat_us\":{},\"connect_lat_us\":{}}}",
        total_h.n,
        secs,
        total_h.n as f64 / secs,
        errors.load(Ordering::Relaxed),
        total_h.json_us(),
        conn_h.json_us()
    );
}

/// Open `conns` connections (one verified round trip each so the forwarder has
/// dialled its backend), print a `ready` line, hold them `hold` seconds, close.
/// The harness samples forwarder memory while this process sits in the hold.
pub fn hold(a: &Args) {
    let lim = raise_nofile();
    let host = a.str("host", "127.0.0.1");
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let conns: usize = a.num("conns", 1000);
    let hold_s: f64 = a.num("hold", 10.0);
    // `--burst B`: echo B bytes through every connection (16 KiB chunks) before
    // holding, so the forwarder's per-connection copy buffers are really touched.
    // The echo is ping-pong, so a slow-latency forwarder (Nagle + delayed ACK)
    // needs many connections in flight to finish in reasonable time.
    let burst: usize = a.num("burst", 0);
    let threads: usize = a.num("threads", if burst > 0 { 128 } else { 8 }).min(conns.max(1));
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let host = host.clone();
            std::thread::spawn(move || {
                let mut socks = Vec::new();
                let mut failed = 0u64;
                let mut i = t;
                while i < conns {
                    let port = base + (i as u16 % ports);
                    match connect(&host, port).and_then(|mut s| {
                        s.write_all(&[1])?;
                        let mut b = [0u8; 1];
                        s.read_exact(&mut b)?;
                        let mut chunk = vec![0x5au8; 16384];
                        let mut left = burst;
                        while left > 0 {
                            let n = left.min(chunk.len());
                            s.write_all(&chunk[..n])?;
                            s.read_exact(&mut chunk[..n])?;
                            left -= n;
                        }
                        Ok(s)
                    }) {
                        Ok(s) => socks.push(s),
                        Err(_) => failed += 1,
                    }
                    i += threads;
                }
                (socks, failed)
            })
        })
        .collect();
    let mut all = Vec::new();
    let mut failed = 0;
    for h in hs {
        if let Ok((s, f)) = h.join() {
            all.extend(s);
            failed += f;
        }
    }
    println!(
        "{{\"phase\":\"ready\",\"established\":{},\"failed\":{failed},\"setup_s\":{:.3},\"client_nofile\":{lim}}}",
        all.len(),
        t0.elapsed().as_secs_f64()
    );
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    std::thread::sleep(Duration::from_secs_f64(hold_s));
    // Liveness check after the hold: every connection must still work.
    let mut alive = 0u64;
    for s in all.iter_mut() {
        let mut b = [0u8; 1];
        if s.write_all(&[2]).is_ok() && s.read_exact(&mut b).is_ok() {
            alive += 1;
        }
    }
    println!("{{\"phase\":\"done\",\"alive_after_hold\":{alive},\"established\":{},\"failed\":{failed}}}", all.len());
}

/// Send `bytes` of pseudo-random data per connection through the path, half
/// close, and verify the echo byte-for-byte plus clean EOF. Also measures the
/// single-stream echo throughput. Exit status 1 on any mismatch.
pub fn integrity(a: &Args) {
    raise_nofile();
    let host = a.str("host", "127.0.0.1");
    let port: u16 = a.num("port", 7000);
    let bytes: u64 = a.num("bytes", 256 << 20);
    let conns: usize = a.num("conns", 1);
    let chunk: usize = a.num("chunk", 64 * 1024);
    let slow_us: u64 = a.num("slow-read-us", 0);
    let bad = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let hs: Vec<_> = (0..conns)
        .map(|i| {
            let host = host.clone();
            let bad = bad.clone();
            std::thread::spawn(move || {
                let s = match connect(&host, port) {
                    Ok(s) => s,
                    Err(_) => {
                        bad.store(true, Ordering::Relaxed);
                        return 0u64;
                    }
                };
                let seed = 0x9E3779B97F4A7C15u64 ^ ((i as u64 + 1) * 0x1000193);
                let mut w = s.try_clone().unwrap();
                let wt = std::thread::spawn(move || {
                    let mut p = Prng(seed);
                    let mut buf = vec![0u8; chunk];
                    let mut left = bytes;
                    while left > 0 {
                        let n = (left as usize).min(chunk);
                        p.fill(&mut buf[..n]);
                        if w.write_all(&buf[..n]).is_err() {
                            return false;
                        }
                        left -= n as u64;
                    }
                    w.shutdown(Shutdown::Write).is_ok()
                });
                let mut r = s;
                let mut p = Prng(seed);
                let mut got = vec![0u8; chunk];
                let mut exp = vec![0u8; chunk];
                let mut total = 0u64;
                let mut ok = true;
                while total < bytes {
                    let want = ((bytes - total) as usize).min(chunk);
                    match r.read(&mut got[..want]) {
                        Ok(0) | Err(_) => {
                            ok = false;
                            break;
                        }
                        Ok(n) => {
                            // Expected stream is generated in `chunk`-sized blocks,
                            // exactly as the writer produced it.
                            let mut off = 0;
                            while off < n {
                                let pos = (total as usize + off) % chunk;
                                if pos == 0 {
                                    let blk = ((bytes - (total + off as u64)) as usize).min(chunk);
                                    p.fill(&mut exp[..blk]);
                                }
                                let take = (n - off).min(chunk - pos);
                                if got[off..off + take] != exp[pos..pos + take] {
                                    ok = false;
                                }
                                off += take;
                            }
                            total += n as u64;
                            if slow_us > 0 {
                                std::thread::sleep(Duration::from_micros(slow_us));
                            }
                        }
                    }
                    if !ok {
                        break;
                    }
                }
                // After the data, the path must deliver a clean EOF.
                if ok {
                    let mut b = [0u8; 1];
                    ok = matches!(r.read(&mut b), Ok(0));
                }
                let wrote_ok = wt.join().unwrap_or(false);
                if !ok || !wrote_ok {
                    bad.store(true, Ordering::Relaxed);
                }
                total
            })
        })
        .collect();
    let mut sum = 0u64;
    for h in hs {
        sum += h.join().unwrap_or(0);
    }
    let secs = t0.elapsed().as_secs_f64();
    let ok = !bad.load(Ordering::Relaxed) && sum == bytes * conns as u64;
    println!(
        "{{\"mode\":\"integrity\",\"ok\":{ok},\"conns\":{conns},\"bytes_each\":{bytes},\"echoed\":{sum},\"secs\":{:.3},\"mbit_per_s_each_way\":{:.1}}}",
        secs,
        sum as f64 * 8.0 / secs / 1e6
    );
    if !ok {
        std::process::exit(1);
    }
}
