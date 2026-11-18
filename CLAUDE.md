# rfw

Rust TCP/UDP port forwarder (tokio). MSRV 1.89, edition 2021.

## Layout

- `src/main.rs` — startup, signals, shutdown ordering
- `src/config.rs` — CLI/env/YAML parsing, port ranges (`MAX_RANGE_PORTS = 4096`)
- `src/manager.rs` — forwarder lifecycle, hot-reload (label-keyed sync), config file watcher
- `src/forwarder.rs` — TCP proxy, Happy-Eyeballs `connect_racing`, opt-in Linux `splice`
- `src/udp.rs` — UDP sessions per client, GRO/GSO offload, DNS refresher
- `src/monitor.rs`, `src/notify.rs` — target probes, Google Chat webhook alerts
- `src/stats.rs`, `src/metrics.rs` — counters, HTTP metrics endpoint
- `src/fdlimit.rs`, `src/logging.rs` — RLIMIT_NOFILE, log setup
- `bench/rfwbench` — benchmark tool (separate crate)

## Commands

```bash
cargo test                                  # unit + e2e
cargo clippy --all-targets -- -D warnings   # CI gate
cargo audit
./build.sh                                  # cross-target release builds
```

## Notes

- Hosts are stored without brackets; print them with `config::host_port` / `ForwarderConfig::{local_addr,remote_addr}` (IPv6 gets `[..]`).
- YAML structs are `deny_unknown_fields`: a new key needs a field, or old configs break.
- Open-file sizing lives in `fdlimit::Budget` only (TCP + UDP sessions + listeners, additive).
- UDP hostname lookups go through `udp::resolve_shared` (one per host per second, process-wide).
- Docker: builder `rust:1-trixie` and runtime `distroless/cc-debian13` must move together (glibc).

- `forwarder::tests::connect_racing_does_not_wait_for_a_hung_first_address` needs `198.51.100.7` to be a blackhole; it fails behind a proxy that answers the connect.
- `cargo outdated` needs `TMPDIR` outside `/tmp` when a stray `/tmp/Cargo.toml` exists.
