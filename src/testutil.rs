//! Test-only helpers shared across modules: a fake authoritative DNS server
//! and a real hickory resolver pointed at it, configured exactly like the
//! production resolver.
//!
//! The production use case is a database behind a cloud DNS name whose IP
//! changes on failover (RDS-style, TTL of a few seconds). Tests stand up the
//! fake resolver in-process, flip its answer, and check that the forwarder
//! and the target monitor follow it.

#![cfg(test)]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use hickory_resolver::TokioResolver;

/// Build a minimal DNS response for `query`: an A record with `ip`/`ttl`
/// for A questions, an empty NOERROR for anything else. Echoes the
/// question section verbatim.
pub(crate) fn fake_dns_response(query: &[u8], ip: Ipv4Addr, ttl: u32) -> Option<Vec<u8>> {
    if query.len() < 12 {
        return None;
    }
    // Walk QNAME labels.
    let mut off = 12;
    loop {
        let len = *query.get(off)? as usize;
        off += 1;
        if len == 0 {
            break;
        }
        off += len;
    }
    let qtype = u16::from_be_bytes([*query.get(off)?, *query.get(off + 1)?]);
    let q_end = off + 4;
    query.get(..q_end)?;

    let is_a = qtype == 1;
    let mut resp = Vec::with_capacity(q_end + 16);
    resp.extend_from_slice(&query[..2]); // ID
    resp.extend_from_slice(&[0x81, 0x80]); // QR, RD, RA, NOERROR
    resp.extend_from_slice(&[0, 1]); // QDCOUNT
    resp.extend_from_slice(&[0, u8::from(is_a)]); // ANCOUNT
    resp.extend_from_slice(&[0, 0, 0, 0]); // NSCOUNT, ARCOUNT
    resp.extend_from_slice(&query[12..q_end]); // question
    if is_a {
        resp.extend_from_slice(&[0xC0, 0x0C]); // name: pointer to question
        resp.extend_from_slice(&[0, 1, 0, 1]); // TYPE A, CLASS IN
        resp.extend_from_slice(&ttl.to_be_bytes());
        resp.extend_from_slice(&[0, 4]); // RDLENGTH
        resp.extend_from_slice(&ip.octets());
    }
    Some(resp)
}

/// Fake resolver on an ephemeral UDP port. Answers every A question with
/// the IP currently stored in `answer` (as a `u32` from `Ipv4Addr`) and
/// counts A questions in `a_queries`.
pub(crate) async fn spawn_fake_dns(
    answer: Arc<AtomicU32>,
    a_queries: Arc<AtomicUsize>,
    ttl: u32,
) -> SocketAddr {
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0_u8; 512];
        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                break;
            };
            let ip = Ipv4Addr::from(answer.load(Ordering::Relaxed));
            if let Some(resp) = fake_dns_response(&buf[..n], ip, ttl) {
                // ANCOUNT == 1 means it was an A question.
                if resp[7] == 1 {
                    a_queries.fetch_add(1, Ordering::Relaxed);
                }
                let _ = sock.send_to(&resp, peer).await;
            }
        }
    });
    addr
}

/// A real hickory resolver pointed at the fake server, configured exactly
/// the way `ForwarderManager::new` configures the production one.
pub(crate) fn resolver_for(dns: SocketAddr) -> TokioResolver {
    use hickory_resolver::config::{
        ConnectionConfig, NameServerConfig, ResolveHosts, ResolverConfig,
    };
    use hickory_resolver::net::runtime::TokioRuntimeProvider;

    let mut conn = ConnectionConfig::udp();
    conn.port = dns.port();
    let mut ns = NameServerConfig::udp(dns.ip());
    ns.connections = vec![conn];
    let mut config = ResolverConfig::default();
    config.add_name_server(ns);

    let mut builder = TokioResolver::builder_with_config(config, TokioRuntimeProvider::default());
    crate::manager::apply_resolver_policy(builder.options_mut());
    // Keep the host's /etc/hosts out of the picture.
    builder.options_mut().use_hosts_file = ResolveHosts::Never;
    builder.build().unwrap()
}
