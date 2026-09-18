# rfw — Rust Forwarder

**rfw** (Rust Forwarder) is a fast, cross-platform TCP port forwarder. It listens on local ports and forwards all traffic to remote destinations — similar to `socat` but specialized and simpler for TCP-only use cases.

## Features

- **TCP forwarding** — Bidirectional proxy between local and remote hosts
- **Multiple forwarders** — Run many forwarders in a single process
- **Configuration sources** — CLI args, YAML file, or environment variables
- **Hot-reload** — Watch YAML config file for changes, apply without restart
- **Auto-reconnect** — Each forwarder retries independently on failure
- **Dynamic DNS** — Resolves remote hostnames per connection, honouring the record TTL, so a remote whose IP changes (cloud database failover) is followed by the next connection without a restart; fails over across all resolved addresses
- **TCP keepalive** — On by default on both sides of every connection, so a peer that vanishes without closing is reaped in ~90s instead of holding a slot forever
- **Cross-platform** — Windows, macOS, Linux (amd64 + arm64), Android (arm64)
- **Traffic stats** — Lifetime cumulative bytes + live throughput rate per forwarder, in periodic logs and an optional HTTP endpoint (`/metrics`, `/stats`)
- **Honest healthcheck** — `GET /` answers `200` only while every forwarder is actually bound and accepting; a forwarder stuck retrying a taken port turns it `503`
- **Structured logging** — Console (human-friendly, color) + JSON file (machine-readable)
- **High throughput** — Async I/O with `tokio`, tunable copy/socket buffers, optional Linux `splice(2)` zero-copy path; handles thousands of concurrent connections
- **Graceful shutdown** — `SIGTERM` and `SIGINT` both drain in-flight connections (bounded grace) before exit
- **Admission control** — Optional `max_connections` cap sheds excess at the edge instead of growing memory without bound

## Quick start

```bash
# Single forwarder: localhost:8080 → 172.16.0.5:80
rfw localhost:8080:172.16.0.5:80

# Multiple forwarders
rfw localhost:8080:172.16.0.5:80 localhost:8081:mydatabase.aws.com:3306

# Using a config file
rfw -f forwarders.yml
```

## Installation

### From source

```bash
cargo install --path .
```

### Pre-built binaries

Download from the [releases page](https://github.com/manprint/rfw/releases).

### Docker

```bash
docker pull ghcr.io/manprint/rfw:v1.0.5
docker compose up -d
```

Or run manually:

```bash
docker run --rm -p 8080:8080 \
  -e RFW_FORWARDER_1=0.0.0.0:8080:example.com:80 \
  ghcr.io/manprint/rfw:v1.0.5 --log-dir ""
```

For the "database behind a cloud DNS name on the host network" deployment, see
[Production recipe](#production-recipe-database-behind-a-cloud-dns-name-socat-replacement).

## Forwarder syntax

```
local_host:local_port:remote_host:remote_port
```

| Part | Description | Example |
|---|---|---|
| `local_host` | IP/hostname to bind locally | `127.0.0.1`, `0.0.0.0`, `localhost` |
| `local_port` | Port to listen on locally | `8080`, `3306`, `443` |
| `remote_host` | Destination hostname/IP | `example.com`, `192.168.1.5` |
| `remote_port` | Destination port | `80`, `443`, `5432` |

**Examples:**

```
127.0.0.1:8080:192.168.1.100:80         → local web proxy
0.0.0.0:3306:db-prod.example.com:3306   → database tunnel
0.0.0.0:443:app.azure.com:443           → cloud service proxy
```

## Configuration

### 1. CLI arguments (positional)

```bash
rfw localhost:8080:172.16.0.5:80 localhost:8081:mydatabase.aws.com:3306
```

### 2. YAML config file

```yaml
# forwarders.yml
forwarders:
  - local_host: "127.0.0.1"
    local_port: 8080
    remote_host: "172.16.0.5"
    remote_port: 80

  - local_host: "0.0.0.0"
    local_port: 3306
    remote_host: "db-prod.example.com"
    remote_port: 3306
```

Usage:

```bash
rfw -f forwarders.yml
```

The YAML file is **watched for changes** — edit it and rfw picks up new forwarders / removes stopped ones without restart. The `forwarders:` list and the data-plane knobs (`buffer_bytes`, `socket_buffer_bytes`, `use_splice`, timeouts, keepalive) are hot-reloaded; interval, `max_connections` and `metrics_addr` changes need a restart.

### 3. Environment variables

```bash
export RFW_FORWARDER_1=127.0.0.1:8080:172.16.0.5:80
export RFW_FORWARDER_2=0.0.0.0:3306:db-prod.example.com:3306
rfw
```

**Precedence (highest to lowest):** Env vars > CLI args > YAML file

## Full CLI reference

```
Usage: rfw [OPTIONS] [FORWARDERS]...

Arguments:
  [FORWARDERS]...  Forwarders in format local_host:local_port:remote_host:remote_port

Options:
  -f, --file <CONFIG_FILE>        Path to YAML configuration file
      --metrics-addr <HOST:PORT>  Enable HTTP stats endpoint (/metrics, /stats)
      --report-interval <SECS>    Traffic-report log cadence (default 60)
      --sample-interval <SECS>    Throughput-rate sampling cadence (default 1)
      --buffer-bytes <BYTES>      Per-direction copy buffer size (default 65536)
      --socket-buffer-bytes <N>   Kernel socket buffer size per socket (default: OS default)
      --splice                    Use the Linux splice(2) zero-copy path (Linux only)
      --max-connections <N>       Cap concurrent connections (default: unbounded)
      --connect-timeout <SECS>    Per-address TCP connect timeout (default 10)
      --dns-timeout <SECS>        DNS resolution timeout (default 5)
      --keepalive-idle <SECS>     TCP keepalive idle time on both sockets of
                                  every connection (default 60; 0 disables)
      --keepalive-interval <SECS> Seconds between keepalive probes (default 10)
      --keepalive-retries <N>     Unanswered probes before a connection is
                                  declared dead (default 3)
      --log-dir <DIR>             Rolling JSON log directory; "" disables file
                                  logging (default "logs")
      --health-check <HOST:PORT>  Probe a running instance's stats endpoint and
                                  exit 0 (healthy) or 1; starts no forwarders
  -h, --help                      Print help
  -V, --version                   Print version
```

### Invalid settings are rejected at startup

Values that would break the process are refused with a clear message rather
than failing later in a background task:

```bash
$ rfw --sample-interval 0 127.0.0.1:8080:example.com:80
Error: sample_interval_secs must be >= 1 (0 would panic the rate sampler)

$ rfw --buffer-bytes 0 127.0.0.1:8080:example.com:80
Error: buffer_bytes must be between 1024 and 16777216 (got 0)
```

On **hot-reload** the same values are *clamped* instead of rejected (a live
process must not abort over an edit), and each correction is logged.

These runtime options can also be set in the YAML config under a `settings:`
block (CLI flags take precedence). See `forwarders.yml`.

## Configuration override behavior

1. If **environment variables** (`RFW_FORWARDER_1`, `RFW_FORWARDER_2`, ...) are set, they **completely replace** CLI args and YAML config.
2. If **CLI args** are provided, they **replace** the YAML config (but can be overridden by env).
3. If **neither** env vars nor CLI args are given, the **YAML config file** is used.
4. If **no config source** is found, rfw prints an error and exits.

## Hot-reload

When using `-f forwarders.yml`, rfw watches the YAML file for changes:

- **New forwarders** added to the file are started automatically
- **Removed forwarders** stop accepting new connections (in-flight connections keep running until they close)
- **Unchanged forwarders** are unaffected

Changes are debounced (1s window) to avoid reload storms.

> **Note:** Hot-reload applies the `forwarders:` list and the data-plane knobs
> (`buffer_bytes`, `socket_buffer_bytes`, `use_splice`, `connect_timeout_secs`,
> `dns_timeout_secs`, `keepalive_idle_secs`, `keepalive_interval_secs`,
> `keepalive_retries`) live — new connections pick up the new values; in-flight
> connections keep theirs. Changes to `report_interval_secs`,
> `sample_interval_secs`, `max_connections`, and `metrics_addr` still require a
> restart.

**A reload is applied all-or-nothing.** The file is parsed once, and both the
`forwarders:` list and the `settings:` block come from that single parse. If the
file is unparseable (a typo, or a half-written save), the reload is refused and
the running configuration — forwarders *and* knobs — is left untouched, with the
reason logged. Duplicate local addresses are rejected the same way.

Re-targeting a forwarder (same local port, different remote) tears down its
listener and rebuilds it on the same port; the old socket is fully released
before the rebind, so the port does not go dark in between.

## How it works

Each forwarder runs as an independent async task:

```
┌─────────────────────────────────────────────────┐
│                    rfw process                    │
│                                                   │
│  ┌──────────┐         ┌──────────────────────┐   │
│  │  Config   │ ─────→  │  Forwarder Manager   │   │
│  │  Loader   │         │  (start/stop/sync)   │   │
│  └──────────┘          └──────────┬───────────┘   │
│                                   │                │
│  ┌────────────────────────────────┼────────────┐  │
│  │  Forwarder 1                   │            │  │
│  │  Listen 127.0.0.1:8080         │            │  │
│  │    ├─ conn 1 → resolve DNS → proxy         │  │
│  │    ├─ conn 2 → resolve DNS → proxy         │  │
│  │    └─ ...                                  │  │
│  ├─ Forwarder 2                 │             │  │
│  │  Listen 0.0.0.0:3306         │             │  │
│  │    └─ ...                                  │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  ┌────────────┐  ┌─────────────┐  ┌───────────┐ │
│  │  Config    │  │  DNS        │  │  Stats    │ │
│  │  Watcher   │  │  Resolver   │  │  Reporter │ │
│  └────────────┘  └─────────────┘  └───────────┘ │
└─────────────────────────────────────────────────┘
```

- **Connection handling:** Every incoming TCP connection is accepted and proxied to the remote destination using `tokio::io::copy_bidirectional_with_sizes` (64 KiB buffers by default), with `TCP_NODELAY` and TCP keepalive set on both sides.
- **DNS resolution:** Each connection resolves the remote hostname through hickory-resolver, configured from the system `/etc/resolv.conf`. Answers are cached for their TTL, so a burst of connections costs one lookup, and a record that changes is followed by the first connection after the TTL expires. A queries are answered first and AAAA is only consulted when A yields nothing, so a resolver that is slow to answer AAAA cannot stall connections.
- **Traffic stats:** Bytes sent/received are counted per forwarder as **lifetime cumulative** counters (never reset). A sampler derives the current throughput rate (bytes/sec, EWMA-smoothed). Both are logged periodically and exposed live via the optional HTTP endpoint.
- **Logging:** All connection/disconnection/error events are logged to stdout (color, human-friendly) and to `logs/rfw.log` (JSON, daily rotation).

## Performance & tuning

rfw is built for throughput: the release profile uses fat LTO,
`codegen-units = 1`, and `opt-level = 3`; the data plane copies with
`copy_bidirectional_with_sizes` over per-connection buffers, sets `TCP_NODELAY`
on both sockets, shares each forwarder's config by `Arc` (no per-connection
allocation), and counts bytes with a single lock-free atomic per poll.

For maximum bandwidth:

- **`--buffer-bytes <BYTES>`** — Per-direction copy buffer (default 65536). On
  fast LAN / 10 GbE links, larger buffers (e.g. `262144` or `1048576`) cut
  syscall count and raise throughput. Cost is memory: two buffers per direction
  per active connection, so scale it against your expected connection count.

- **`--socket-buffer-bytes <N>`** — Sets the kernel send/receive buffers
  (`SO_SNDBUF`/`SO_RCVBUF`) on both sockets. The single biggest lever on
  high bandwidth-delay (WAN / high-latency) links, where the default buffers
  cap the in-flight window. Left at the OS default when unset.

- **`--splice`** *(Linux only)* — Uses the `splice(2)` zero-copy data path:
  bytes move directly between the two sockets through a kernel pipe, never
  copied into user space. Lower CPU and higher throughput for pure forwarding;
  ignored with a warning on non-Linux targets. Byte counting is preserved.

  ```bash
  rfw --splice --socket-buffer-bytes 1048576 --buffer-bytes 1048576 \
      0.0.0.0:8080:fileserver.lan:80
  ```

- **Connection count** — Each connection is one async task on tokio's
  multi-threaded runtime (one worker per CPU core); rfw handles thousands of
  concurrent connections without extra tuning.

- **`--max-connections <N>`** — Caps concurrently proxied connections across all
  forwarders. Unbounded by default. Each in-flight connection costs roughly
  `2 * buffer_bytes` of heap on the portable path, so under a container memory
  limit this is what keeps a connection flood from becoming an OOM kill.
  Connections beyond the cap are closed immediately at accept and counted in
  `rfw_rejected_connections_total`. Size it as
  `memory_budget / (2 * buffer_bytes)` with headroom.

- **`--connect-timeout <SECS>`** — Per-address TCP connect timeout (default 10).
  Connect attempts to a multi-address host are **staggered and raced**
  (RFC 8305 "Happy Eyeballs", 250 ms apart), so a dead or black-holed first
  DNS record costs a stagger rather than a full timeout.

- **DNS** — A remote given as an IP literal skips the resolver entirely.
  Hostnames are resolved per connection under `--dns-timeout` (default 5s);
  hickory caches answers for their TTL, so a burst of connections costs one
  lookup and an IP change is picked up by the first connection after the TTL.

- **`--keepalive-idle / --keepalive-interval / --keepalive-retries`** — TCP
  keepalive on **both** sockets of every proxied connection, on by default
  (60s idle, 10s between probes, 3 probes ⇒ a dead peer is detected in ~90s).
  This is what turns a half-open connection into an error the forwarder can
  see. Without it, a client that crashed, lost its VPN, or had its NAT mapping
  expire — or a remote that failed over without sending a reset — leaves a
  connection that never ends and keeps holding its `max_connections` slot, its
  backend session and its two copy buffers. Database clients with pooled,
  idle-for-hours sessions are exactly the shape that leaks. Set
  `--keepalive-idle 0` to disable.

The buffer, socket-buffer, splice, timeout and keepalive knobs above are
**hot-reloadable** via the YAML `settings:` block — new connections adopt the
new values without a restart. `max_connections` is fixed at startup.

## Logging

Logs go to:

- **Console** — Human-readable with colors and timestamps
- **File** — `logs/rfw.log` (JSON, daily rotation), unless disabled

Both sinks honour `RUST_LOG` (default `info`). Set `RUST_LOG=debug` for
per-connection detail, or `RUST_LOG=warn` to quieten both.

Per-connection lifecycle records ("Connected" / "Disconnected") are at **debug**
level. At `info` a busy forwarder logs only startup, reloads, the periodic
traffic report, and genuine problems.

> **Operational note:** rotated log files are never deleted. For a long-lived
> deployment either point `--log-dir` at a volume managed by logrotate, or run
> with `--log-dir ""` and let the container runtime collect stdout:
>
> ```bash
> rfw --log-dir "" 0.0.0.0:8080:example.com:80
> ```

## Signals & shutdown

`SIGTERM` (what Docker, Kubernetes, and systemd send) and `SIGINT` (Ctrl+C) both
trigger the same graceful shutdown:

1. Stop accepting new connections on every forwarder.
2. Drain in-flight connections, up to a 10-second grace window.
3. Flush logs and exit.

Give the container at least that long to stop:

```yaml
stop_grace_period: 15s     # docker compose
terminationGracePeriodSeconds: 15   # kubernetes
```

## Traffic report

Every `report_interval` seconds (default 60), rfw logs a traffic summary. Totals
are **cumulative since process start** (not a per-window bucket); `tx`/`rx` are
the current throughput rate:

```
─── Traffic Report ──────────────────────────────────
  127.0.0.1:8080->172.16.0.5:80      sent=  1.230 GB  recv=  4.560 GB  tx=12.300 MB/s  rx=45.600 MB/s  active=3
  TOTAL (all forwarders, lifetime)    sent=  1.230 GB  recv=  4.560 GB
──────────────────────────────────────────────────────
```

## Bandwidth metrics endpoint

Enable a read-only HTTP endpoint to read bandwidth **continuously**, at any
moment, while rfw runs:

```bash
rfw --metrics-addr 127.0.0.1:9090 127.0.0.1:8080:172.16.0.5:80
```

- **`GET /metrics`** — Prometheus text format (scrape-friendly):

  ```
  # TYPE rfw_bytes_sent_total counter
  rfw_bytes_sent_total{label="127.0.0.1:8080->172.16.0.5:80"} 1321205760
  # TYPE rfw_bytes_received_total counter
  rfw_bytes_received_total{label="127.0.0.1:8080->172.16.0.5:80"} 4896202752
  # TYPE rfw_bytes_sent_rate_bps gauge
  rfw_bytes_sent_rate_bps{label="127.0.0.1:8080->172.16.0.5:80"} 12897484
  # TYPE rfw_active_connections gauge
  rfw_active_connections{label="127.0.0.1:8080->172.16.0.5:80"} 3
  # TYPE rfw_rejected_connections_total counter
  rfw_rejected_connections_total{label="127.0.0.1:8080->172.16.0.5:80"} 0
  # TYPE rfw_listening gauge
  rfw_listening{label="127.0.0.1:8080->172.16.0.5:80"} 1
  ```

- **`GET /stats`** — JSON array of per-forwarder snapshots:

  ```json
  [{"label":"127.0.0.1:8080->172.16.0.5:80","bytes_sent":1321205760,"bytes_received":4896202752,"active_connections":3,"rejected_connections":0,"rate_sent_bps":12897484,"rate_recv_bps":48234234,"listening":true}]
  ```

- **`GET /`** — Health probe. `200 OK` (body `rfw ok`) while **every**
  configured forwarder has its listener bound and accepting;
  `503 Service Unavailable` otherwise, with a body naming the forwarders that
  are not listening (e.g. stuck in the bind-retry loop because the port is
  already taken).

A non-zero `rfw_rejected_connections_total` means `max_connections` is the
bottleneck, not the link — the first thing to check when throughput plateaus.

### Container healthcheck

`--health-check` makes the binary its own probe, which matters in the
distroless runtime image (no shell, no `curl`). It performs `GET /` and exits
`0` only on `200`, so it inherits the rule above: a process whose forwarder
cannot bind its port is running but **not** healthy, and the probe says so.
Once the port frees up, the bind-retry loop claims it and the probe turns green
without a restart.

```bash
rfw --health-check 127.0.0.1:9090 && echo healthy
```

```yaml
healthcheck:
  test: ["CMD", "/usr/local/bin/rfw", "--health-check", "127.0.0.1:9090"]
  interval: 30s
  timeout: 10s
  retries: 3
```

`bytes_*_total` are lifetime cumulative counters; `*_rate_bps` are the current
EWMA-smoothed throughput in bytes/sec. The endpoint is **disabled by default**
(no `--metrics-addr` / `settings.metrics_addr` ⇒ no port opened).

> **Security:** The endpoint is unauthenticated. Bind it to a loopback or
> trusted interface (e.g. `127.0.0.1:9090`), not a public address.

## Cross-platform builds

Use the included `build.sh` script to compile rfw for all supported platforms via Docker:

```bash
./build.sh
```

To build a single platform, pass a filter such as `linux`, `macos`, `windows`, or `android`:

```bash
./build.sh android
```

Output binaries go to `dist/`:

```
dist/
├── rfw-linux-amd64
├── rfw-linux-arm64
├── rfw-macos-amd64
├── rfw-macos-arm64
├── rfw-windows-amd64.exe
├── rfw-windows-arm64.exe
└── rfw-android-arm64
```

Requirements: Docker

## Docker

Every `v<major>.<minor>.<patch>` tag publishes `ghcr.io/manprint/rfw` with three tags: the Git tag itself, `latest`, and the full commit SHA.

### Pull and run

```bash
# Pull the latest published image
docker pull ghcr.io/manprint/rfw:latest

# Run with env vars
docker run --rm -p 8080:8080 -p 8081:8081 \
    -e RFW_FORWARDER_1=0.0.0.0:8080:example.com:80 \
    -e RFW_FORWARDER_2=0.0.0.0:8081:api.example.com:443 \
  ghcr.io/manprint/rfw:latest

# Or with a config file mounted
docker run --rm -p 8080:8080 \
    -v ./forwarders.yml:/etc/rfw/forwarders.yml:ro \
  ghcr.io/manprint/rfw:latest -f /etc/rfw/forwarders.yml
```

### Docker Compose

```bash
docker compose up -d
```

In production pin a release tag (`ghcr.io/manprint/rfw:v1.0.5`) rather than
`latest`, which moves on every release. Edit `docker-compose.yml` to configure
your forwarders via env vars.

### Production recipe: database behind a cloud DNS name (socat replacement)

The canonical deployment: clients connect to port `5432` on a host, and rfw
forwards to a managed database (Amazon RDS, Cloud SQL, Azure Database, …) that
is reachable only through a **DNS name whose IP changes** on failover or
maintenance. This replaces the classic

```bash
socat tcp-listen:5432,fork,reuseaddr tcp-connect:$(dig +short db.example.com | head -1):5432
```

which resolves the name **once at start** and pins the IP: after a failover it
keeps forwarding to the old address until someone restarts the container.

What rfw does differently, and what to expect:

| Event | Behaviour |
|---|---|
| Normal operation | Every new connection resolves the name; hickory caches the answer for its TTL (RDS publishes 5s), so a burst of connections costs one lookup. |
| DNS record changes (failover) | The first connection after the TTL expires goes to the new IP. No restart, no reload, no signal. Verified end-to-end in the test suite. |
| Connections already open at failover | Stay on the old IP (this is L4 forwarding; the bytes are in flight). They end when the old instance resets them, or when keepalive declares them dead (~90s). Clients reconnect and land on the new IP. |
| Record temporarily points at a dead IP | The client's connection fails after `--connect-timeout` (not a hang); recovery is automatic once the record is fixed. |
| Client vanishes without closing (crash, VPN/NAT drop) | Keepalive reaps the half-open connection in ~90s, freeing its `max_connections` slot and the database session behind it. |
| Port already taken on the host | The forwarder retries the bind every 5s and the healthcheck reports `503` until it succeeds. |

**`docker run`** (host network, exactly like the socat container it replaces):

```bash
docker rm -f rfw-db 2>/dev/null

docker run -d \
  --name=rfw-db \
  --net=host \
  --restart=always \
  --ulimit nofile=65536:65536 \
  --stop-timeout 15 \
  --health-cmd '/usr/local/bin/rfw --health-check 127.0.0.1:9090' \
  --health-interval 30s --health-timeout 10s --health-retries 3 \
  -e RFW_FORWARDER_1=0.0.0.0:5432:mydb.xxxxxxxxxxxx.eu-south-1.rds.amazonaws.com:5432 \
  -e RUST_LOG=info \
  ghcr.io/manprint/rfw:v1.0.5 \
    --log-dir "" \
    --max-connections 1000 \
    --metrics-addr 127.0.0.1:9090 \
    --connect-timeout 5 \
    --dns-timeout 3
```

- `--net=host` binds `0.0.0.0:5432` directly on the host, like `socat` did, and
  uses the host's `/etc/resolv.conf` for DNS.
- `--ulimit nofile=65536:65536`: each proxied connection holds two sockets and
  Docker's default soft limit is often 1024, i.e. ~500 connections before
  `accept()` fails with `EMFILE`.
- `--stop-timeout 15` covers the 10s in-flight drain on `docker stop`.
- `--metrics-addr 127.0.0.1:9090`: on the host network `0.0.0.0` would expose
  the unauthenticated stats endpoint on every interface. Loopback keeps it
  local; the healthcheck reaches it from inside the same network namespace.
- `--max-connections` should stay below the database's own connection limit;
  with keepalive on, dead clients no longer eat into it.
- `--log-dir ""` leaves logging to the Docker log driver (rfw never prunes its
  own rotated files).

**Docker Compose**, same deployment:

```yaml
services:
  rfw-db:
    image: ghcr.io/manprint/rfw:v1.0.5
    container_name: rfw-db
    restart: always
    network_mode: host          # 0.0.0.0:5432 on the host, host DNS
    environment:
      - RFW_FORWARDER_1=0.0.0.0:5432:mydb.xxxxxxxxxxxx.eu-south-1.rds.amazonaws.com:5432
      - RUST_LOG=info
    command:
      - --log-dir
      - ""
      - --max-connections
      - "1000"
      - --metrics-addr
      - 127.0.0.1:9090
      - --connect-timeout
      - "5"
      - --dns-timeout
      - "3"
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    stop_grace_period: 15s
    healthcheck:
      test: ["CMD", "/usr/local/bin/rfw", "--health-check", "127.0.0.1:9090"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s
```

To manage the forwarder list from a file instead (hot-reload, several
databases), mount the **directory** containing `forwarders.yml` rather than the
single file — editors save by replacing the inode, and a single-file bind mount
keeps pointing at the old one so the watcher never sees the change:

```yaml
    volumes:
      - ./rfw:/etc/rfw:ro
    command: ["-f", "/etc/rfw/forwarders.yml", "--log-dir", "", "--metrics-addr", "127.0.0.1:9090"]
```

Keepalive (`keepalive_idle_secs` 60 / `keepalive_interval_secs` 10 /
`keepalive_retries` 3) is on by default and needs no flag; tune it in the
`settings:` block or with `--keepalive-*` if your network drops idle flows
faster than 60s.

## Development

### Prerequisites

- Rust 1.89 or newer (stable)
- Linux/macOS (for cross-compilation build script: Docker)

### Build

```bash
cargo build --release
```

### Test

```bash
cargo test
```

The suite has four parts:

| Target | What it covers |
|---|---|
| unit tests (`src/**`) | settings validation and clamping, runtime knobs, keepalive socket options, stats/EWMA, Prometheus + JSON rendering, the metrics endpoint's routing, health verdict and timeouts, accept-error classification, admission control, DNS/IP-literal resolution, **DNS change propagation against an in-test fake resolver** (new connections follow the record, held connections stay pinned, dead records fail fast and recover), staggered connect racing, manager start/stop/sync |
| `tests/e2e_forward.rs` | byte-exact forwarding and live cumulative metrics, on both the portable and `splice` paths |
| `tests/e2e_lifecycle.rs` | startup validation, `SIGTERM` drain, `max_connections` shedding, half-close propagation, abrupt-reset leak checks, the healthcheck probe including `503` while a port cannot be bound and recovery once it frees |
| `tests/e2e_hot_reload.rs` | re-targeting a port without an outage, add/remove, malformed and duplicate configs, live knob reloads, counter continuity |

The e2e suites spawn the real binary and drive it over real sockets, so they
catch integration failures (signals, rebinds, shedding) that unit tests cannot.

For a bigger end-to-end payload:

```bash
RFW_E2E_MB=256 cargo test --test e2e_forward
```

### Lint

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

### Run

```bash
cargo run -- localhost:8080:example.com:80
```

## Project structure

```
├── Cargo.toml          # Rust dependencies and project metadata
├── build.sh            # Cross-platform build script
├── Dockerfile          # Production Docker image
├── Dockerfile.build    # Cross-compilation toolchain image
├── docker-compose.yml  # Docker Compose example
├── forwarders.yml      # Example YAML config
├── cargo-cross.toml    # Cross-linker config for build
├── src/
│   ├── main.rs         # Entry point, CLI parsing
│   ├── config.rs       # Config loading (CLI, YAML, env)
│   ├── forwarder.rs    # TCP forwarding logic + reporter + rate sampler
│   ├── manager.rs      # Forwarder lifecycle management
│   ├── stats.rs        # Traffic statistics (cumulative counters + rate)
│   ├── metrics.rs      # HTTP /metrics + /stats endpoint
│   └── logging.rs      # Tracing/logging setup
├── tests/
│   ├── common/mod.rs     # Shared e2e harness (spawn rfw, backends, probes)
│   ├── e2e_forward.rs    # Byte-exact forwarding + live metrics
│   ├── e2e_lifecycle.rs  # Startup validation, SIGTERM, admission control
│   └── e2e_hot_reload.rs # Config watcher, rebinds, malformed/duplicate configs
└── README.md           # This file
```

## License

MIT
