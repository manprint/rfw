//! Multi-port TCP+UDP echo backend. One epoll loop per thread; listeners use
//! SO_REUSEPORT so the kernel spreads connections/flows over the threads.

use crate::util::*;
use std::mem::{size_of, zeroed};

enum Kind {
    Listener,
    Udp,
    Conn { pending: Vec<u8>, off: usize },
}

fn sockaddr(port: u16) -> libc::sockaddr_in {
    let mut a: libc::sockaddr_in = unsafe { zeroed() };
    a.sin_family = libc::AF_INET as u16;
    a.sin_port = port.to_be();
    a.sin_addr.s_addr = 0; // 0.0.0.0
    a
}

fn bound_socket(ty: i32, port: u16, listen: bool) -> i32 {
    unsafe {
        let fd = libc::socket(
            libc::AF_INET,
            ty | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        );
        if fd < 0 {
            die("socket() failed");
        }
        set_i32(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, 1);
        set_i32(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1);
        if ty == libc::SOCK_DGRAM {
            // Never let the echo be the bottleneck of a UDP flood.
            set_i32(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, 8 << 20);
            set_i32(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, 8 << 20);
        }
        let a = sockaddr(port);
        if libc::bind(
            fd,
            &a as *const _ as *const libc::sockaddr,
            size_of::<libc::sockaddr_in>() as u32,
        ) != 0
        {
            die(&format!(
                "bind :{port} failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        if listen && libc::listen(fd, 4096) != 0 {
            die("listen failed");
        }
        fd
    }
}

fn ctl(ep: i32, op: i32, fd: i32, ev: u32) {
    let mut e = libc::epoll_event {
        events: ev,
        u64: fd as u64,
    };
    unsafe { libc::epoll_ctl(ep, op, fd, &mut e) };
}

fn run_thread(base: u16, ports: u16, tcp: bool, udp: bool) {
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    let mut kinds: Vec<Option<Kind>> = Vec::new();
    let put = |kinds: &mut Vec<Option<Kind>>, fd: i32, k: Kind| {
        let i = fd as usize;
        if kinds.len() <= i {
            kinds.resize_with(i + 1, || None);
        }
        kinds[i] = Some(k);
    };
    for p in 0..ports {
        if tcp {
            let fd = bound_socket(libc::SOCK_STREAM, base + p, true);
            put(&mut kinds, fd, Kind::Listener);
            ctl(ep, libc::EPOLL_CTL_ADD, fd, libc::EPOLLIN as u32);
        }
        if udp {
            let fd = bound_socket(libc::SOCK_DGRAM, base + p, false);
            put(&mut kinds, fd, Kind::Udp);
            ctl(ep, libc::EPOLL_CTL_ADD, fd, libc::EPOLLIN as u32);
        }
    }
    let mut buf = vec![0u8; 256 * 1024];
    let mut events: Vec<libc::epoll_event> = vec![unsafe { zeroed() }; 512];
    loop {
        let n = unsafe { libc::epoll_wait(ep, events.as_mut_ptr(), events.len() as i32, -1) };
        if n < 0 {
            continue;
        }
        for ev in &events[..n as usize] {
            let fd = ev.u64 as i32;
            let flags = ev.events;
            let idx = fd as usize;
            match kinds.get(idx).and_then(|k| k.as_ref()) {
                Some(Kind::Listener) => loop {
                    let c = unsafe {
                        libc::accept4(
                            fd,
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                        )
                    };
                    if c < 0 {
                        break;
                    }
                    set_i32(c, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1);
                    put(
                        &mut kinds,
                        c,
                        Kind::Conn {
                            pending: Vec::new(),
                            off: 0,
                        },
                    );
                    ctl(
                        ep,
                        libc::EPOLL_CTL_ADD,
                        c,
                        (libc::EPOLLIN | libc::EPOLLRDHUP) as u32,
                    );
                },
                Some(Kind::Udp) => {
                    for _ in 0..64 {
                        let mut from: libc::sockaddr_in = unsafe { zeroed() };
                        let mut fl = size_of::<libc::sockaddr_in>() as u32;
                        let r = unsafe {
                            libc::recvfrom(
                                fd,
                                buf.as_mut_ptr() as *mut _,
                                buf.len(),
                                0,
                                &mut from as *mut _ as *mut libc::sockaddr,
                                &mut fl,
                            )
                        };
                        if r < 0 {
                            break;
                        }
                        unsafe {
                            libc::sendto(
                                fd,
                                buf.as_ptr() as *const _,
                                r as usize,
                                0,
                                &from as *const _ as *const libc::sockaddr,
                                fl,
                            );
                        }
                    }
                }
                Some(Kind::Conn { .. }) => {
                    let mut close = false;
                    if let Some(Some(Kind::Conn { pending, off })) = kinds.get_mut(idx) {
                        // Drain what is still queued before reading more.
                        if *off < pending.len() {
                            let w = unsafe {
                                libc::write(
                                    fd,
                                    pending[*off..].as_ptr() as *const _,
                                    pending.len() - *off,
                                )
                            };
                            if w > 0 {
                                *off += w as usize;
                            } else if w < 0
                                && std::io::Error::last_os_error().kind()
                                    != std::io::ErrorKind::WouldBlock
                            {
                                close = true;
                            }
                            if *off >= pending.len() {
                                pending.clear();
                                *off = 0;
                                ctl(
                                    ep,
                                    libc::EPOLL_CTL_MOD,
                                    fd,
                                    (libc::EPOLLIN | libc::EPOLLRDHUP) as u32,
                                );
                            }
                        }
                        if !close
                            && pending.is_empty()
                            && flags
                                & (libc::EPOLLIN
                                    | libc::EPOLLRDHUP
                                    | libc::EPOLLHUP
                                    | libc::EPOLLERR) as u32
                                != 0
                        {
                            loop {
                                let r = unsafe {
                                    libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len())
                                };
                                if r == 0 {
                                    // Peer half-closed: everything it sent has been
                                    // echoed, propagate the EOF and finish.
                                    unsafe { libc::shutdown(fd, libc::SHUT_WR) };
                                    close = true;
                                    break;
                                }
                                if r < 0 {
                                    if std::io::Error::last_os_error().kind()
                                        != std::io::ErrorKind::WouldBlock
                                    {
                                        close = true;
                                    }
                                    break;
                                }
                                let r = r as usize;
                                let w = unsafe { libc::write(fd, buf.as_ptr() as *const _, r) };
                                let w = if w < 0 { 0 } else { w as usize };
                                if w < r {
                                    pending.extend_from_slice(&buf[w..r]);
                                    *off = 0;
                                    ctl(
                                        ep,
                                        libc::EPOLL_CTL_MOD,
                                        fd,
                                        (libc::EPOLLOUT | libc::EPOLLRDHUP) as u32,
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    if close {
                        unsafe { libc::close(fd) };
                        kinds[idx] = None;
                    }
                }
                None => {}
            }
        }
    }
}

pub fn run(a: &Args) {
    let lim = raise_nofile();
    let base: u16 = a.num("base", 7000);
    let ports: u16 = a.num("ports", 1);
    let threads: usize = a.num("threads", 2);
    let tcp = !a.flag("no-tcp");
    let udp = !a.flag("no-udp");
    eprintln!(
        "echo: ports {base}..{} threads {threads} tcp={tcp} udp={udp} nofile={lim}",
        base + ports - 1
    );
    let hs: Vec<_> = (0..threads)
        .map(|_| std::thread::spawn(move || run_thread(base, ports, tcp, udp)))
        .collect();
    println!("{{\"phase\":\"listening\"}}");
    for h in hs {
        let _ = h.join();
    }
}
