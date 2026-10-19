//! UDP client modes: udprr (closed-loop RTT / DNS QPS), udpflood (open-loop
//! rate with loss, reordering and latency), udphold (many flows, for memory).

use crate::hist::Hist;
use crate::util::*;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn sock(host: &str, port: u16, rcvbuf: i32) -> UdpSocket {
    let s = UdpSocket::bind("0.0.0.0:0").unwrap_or_else(|e| die(&format!("udp bind: {e}")));
    use std::os::unix::io::AsRawFd;
    set_i32(s.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF, rcvbuf);
    s.connect(resolve(host, port))
        .unwrap_or_else(|e| die(&format!("udp connect: {e}")));
    s
}

/// Minimal DNS query (A example.org, RD set) with the given id.
fn dns_query(id: u16) -> Vec<u8> {
    let mut q = vec![
        (id >> 8) as u8,
        id as u8,
        0x01,
        0x00,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
    ];
    for l in ["example", "org"] {
        q.push(l.len() as u8);
        q.extend_from_slice(l.as_bytes());
    }
    q.extend_from_slice(&[0, 0, 1, 0, 1]);
    q
}

pub fn udprr(a: &Args) {
    let host = a.str("host", "127.0.0.1");
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let flows: usize = a.num("flows", 1);
    let size: usize = a.num("size", 64).max(16);
    let dur: f64 = a.num("duration", 10.0);
    let warm: f64 = a.num("warmup", 1.0);
    let dns = a.flag("dns");
    let errors = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let end = Duration::from_secs_f64(warm + dur);
    let hs: Vec<_> = (0..flows)
        .map(|i| {
            let host = host.clone();
            let errors = errors.clone();
            std::thread::spawn(move || {
                let mut h = Hist::new();
                let s = sock(&host, base + (i as u16 % ports), 1 << 20);
                s.set_read_timeout(Some(Duration::from_millis(500))).ok();
                let mut buf = vec![0u8; 2048];
                let mut seq: u16 = i as u16;
                let mut pay = vec![0u8; size];
                while t0.elapsed() < end {
                    seq = seq.wrapping_add(1);
                    let q = if dns {
                        dns_query(seq)
                    } else {
                        pay[..2].copy_from_slice(&seq.to_be_bytes());
                        pay.clone()
                    };
                    let t = Instant::now();
                    if s.send(&q).is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    // Late replies to an earlier (timed-out) query are skipped.
                    loop {
                        match s.recv(&mut buf) {
                            Ok(n) if n >= 2 && buf[..2] == seq.to_be_bytes() => {
                                if t.duration_since(t0) >= Duration::from_secs_f64(warm) {
                                    h.record(t.elapsed().as_nanos() as u64);
                                }
                                break;
                            }
                            Ok(_) => continue,
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
                h
            })
        })
        .collect();
    let mut tot = Hist::new();
    for h in hs {
        if let Ok(h) = h.join() {
            tot.merge(&h);
        }
    }
    println!(
        "{{\"mode\":\"udprr\",\"flows\":{flows},\"size\":{size},\"dns\":{dns},\"msgs\":{},\"msgs_per_s\":{:.0},\"errors_or_timeouts\":{},\"lat_us\":{}}}",
        tot.n,
        tot.n as f64 / dur,
        errors.load(Ordering::Relaxed),
        tot.json_us()
    );
}

/// Open-loop UDP: `rate` datagrams/s in total over `flows` sockets, payload
/// carries (flow-local seq, send timestamp). The echo comes back to a reader
/// per flow: loss = sent - received, reorder = seq below the highest seen.
pub fn udpflood(a: &Args) {
    let host = a.str("host", "127.0.0.1");
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let flows: usize = a.num("flows", 1);
    let size: usize = a.num("size", 64).max(16);
    let rate: f64 = a.num("rate", 100_000.0);
    let dur: f64 = a.num("duration", 10.0);
    let sent = Arc::new(AtomicU64::new(0));
    let recvd = Arc::new(AtomicU64::new(0));
    let reord = Arc::new(AtomicU64::new(0));
    let send_err = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let per_flow = rate / flows as f64;
    let interval = Duration::from_secs_f64(1.0 / per_flow);
    let mut rd = Vec::new();
    let mut sd = Vec::new();
    for i in 0..flows {
        let s = sock(&host, base + (i as u16 % ports), 8 << 20);
        s.set_read_timeout(Some(Duration::from_millis(100))).ok();
        let r = s.try_clone().unwrap();
        {
            let (recvd, reord, stop) = (recvd.clone(), reord.clone(), stop.clone());
            rd.push(std::thread::spawn(move || {
                let mut h = Hist::new();
                let mut buf = vec![0u8; 65536];
                let mut hi = 0u64;
                let mut idle = 0;
                loop {
                    match r.recv(&mut buf) {
                        Ok(n) if n >= 16 => {
                            idle = 0;
                            let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                            let ts = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                            h.record(ns_since(t0).saturating_sub(ts));
                            if seq < hi {
                                reord.fetch_add(1, Ordering::Relaxed);
                            } else {
                                hi = seq;
                            }
                            recvd.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(_) => {}
                        Err(_) => {
                            // Stop once the sender is done and the path is drained.
                            if stop.load(Ordering::Relaxed) {
                                idle += 1;
                                if idle >= 5 {
                                    break;
                                }
                            }
                        }
                    }
                }
                h
            }));
        }
        let (sent, send_err) = (sent.clone(), send_err.clone());
        sd.push(std::thread::spawn(move || {
            let mut pay = vec![0u8; size];
            let mut seq = 0u64;
            let start = Instant::now();
            let end = Duration::from_secs_f64(dur);
            let mut next = start;
            loop {
                let now = Instant::now();
                if now.duration_since(start) >= end {
                    break;
                }
                if next > now {
                    let gap = next - now;
                    if gap > Duration::from_micros(200) {
                        std::thread::sleep(gap - Duration::from_micros(100));
                    } else {
                        std::hint::spin_loop();
                    }
                    continue;
                }
                pay[..8].copy_from_slice(&seq.to_le_bytes());
                pay[8..16].copy_from_slice(&ns_since(t0).to_le_bytes());
                match s.send(&pay) {
                    Ok(_) => {
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        send_err.fetch_add(1, Ordering::Relaxed);
                    }
                }
                seq += 1;
                next += interval;
                // Fell behind (sender CPU-bound): do not burst to catch up.
                if next + Duration::from_millis(5) < Instant::now() {
                    next = Instant::now();
                }
            }
        }));
    }
    for h in sd {
        let _ = h.join();
    }
    let send_secs = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let mut lat = Hist::new();
    for h in rd {
        if let Ok(h) = h.join() {
            lat.merge(&h);
        }
    }
    let (s, r) = (sent.load(Ordering::Relaxed), recvd.load(Ordering::Relaxed));
    println!(
        "{{\"mode\":\"udpflood\",\"flows\":{flows},\"size\":{size},\"rate_target\":{rate},\"sent\":{s},\"sent_pps\":{:.0},\"received\":{r},\"recv_pps\":{:.0},\"loss_pct\":{:.3},\"reordered\":{},\"send_errors\":{},\"mbit_per_s_recv\":{:.1},\"lat_us\":{}}}",
        s as f64 / send_secs,
        r as f64 / send_secs,
        if s > 0 { (s.saturating_sub(r)) as f64 * 100.0 / s as f64 } else { 0.0 },
        reord.load(Ordering::Relaxed),
        send_err.load(Ordering::Relaxed),
        r as f64 * size as f64 * 8.0 / send_secs / 1e6,
        lat.json_us()
    );
}

/// `flows` distinct source sockets, each doing a few verified round trips so
/// the forwarder holds one session per flow; hold them idle, then verify again.
pub fn udphold(a: &Args) {
    let lim = raise_nofile();
    let host = a.str("host", "127.0.0.1");
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let flows: usize = a.num("flows", 1000);
    let hold_s: f64 = a.num("hold", 10.0);
    let t0 = Instant::now();
    let socks: Vec<UdpSocket> = (0..flows)
        .map(|i| sock(&host, base + (i as u16 % ports), 1 << 20))
        .collect();
    let probe = |socks: &[UdpSocket], tag: u8| -> u64 {
        // Pace in batches so the forwarder's socket buffer is never the limit
        // being measured; retry stragglers once.
        let mut ok = 0u64;
        let mut pending: Vec<usize> = (0..socks.len()).collect();
        for _attempt in 0..3 {
            for chunk in pending.chunks(200) {
                for &i in chunk {
                    let _ = socks[i].send(&[tag, (i & 0xff) as u8, (i >> 8) as u8, 0]);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            let deadline = Instant::now() + Duration::from_millis(1500);
            let mut still = Vec::new();
            let mut buf = [0u8; 64];
            for &i in &pending {
                socks[i]
                    .set_read_timeout(Some(Duration::from_millis(1)))
                    .ok();
                let mut got = false;
                while Instant::now() < deadline {
                    match socks[i].recv(&mut buf) {
                        Ok(n) if n >= 3 && buf[0] == tag => {
                            got = true;
                            break;
                        }
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
                if got {
                    ok += 1;
                } else {
                    still.push(i);
                }
            }
            pending = still;
            if pending.is_empty() {
                break;
            }
        }
        ok
    };
    let ok = probe(&socks, 1);
    println!(
        "{{\"phase\":\"ready\",\"established\":{ok},\"failed\":{},\"setup_s\":{:.3},\"client_nofile\":{lim}}}",
        flows as u64 - ok,
        t0.elapsed().as_secs_f64()
    );
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    std::thread::sleep(Duration::from_secs_f64(hold_s));
    let alive = probe(&socks, 2);
    println!(
        "{{\"phase\":\"done\",\"alive_after_hold\":{alive},\"established\":{ok},\"failed\":{}}}",
        flows as u64 - ok
    );
}
