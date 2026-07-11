# rfw — Rust Forwarder

**rfw** (Rust Forwarder) is a fast, cross-platform TCP port forwarder. It listens on local ports and forwards all traffic to remote destinations — similar to `socat` but specialized and simpler for TCP-only use cases.

## Features

- **TCP forwarding** — Bidirectional proxy between local and remote hosts
- **Multiple forwarders** — Run many forwarders in a single process
- **Configuration sources** — CLI args, YAML file, or environment variables
- **Hot-reload** — Watch YAML config file for changes, apply without restart
- **Auto-reconnect** — Each forwarder retries independently on failure
- **Dynamic DNS** — Resolves remote hostnames fresh per connection (picks up DNS changes), with failover across all resolved addresses
- **Cross-platform** — Windows, macOS, Linux (amd64 + arm64), Android (arm64)
- **Traffic stats** — Lifetime cumulative bytes + live throughput rate per forwarder, in periodic logs and an optional HTTP endpoint (`/metrics`, `/stats`)
- **Structured logging** — Console (human-friendly, color) + JSON file (machine-readable)
- **High throughput** — Async I/O with `tokio`, tunable copy/socket buffers, optional Linux `splice(2)` zero-copy path; handles thousands of concurrent connections
- **Graceful shutdown** — In-flight connections are drained (bounded grace) before exit

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
docker pull ghcr.io/manprint/rfw:latest
docker compose up -d
```

Or run manually:

```bash
docker run --rm -p 8080:8080 \
  -e RFW_FORWARDER_1=0.0.0.0:8080:example.com:80 \
  ghcr.io/manprint/rfw:latest
```

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

The YAML file is **watched for changes** — edit it and rfw picks up new forwarders / removes stopped ones without restart. The `forwarders:` list and the data-plane knobs (`buffer_bytes`, `socket_buffer_bytes`, `use_splice`) are hot-reloaded; interval and `metrics_addr` changes need a restart.

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
  -f, --file <CONFIG_FILE>       Path to YAML configuration file
      --metrics-addr <HOST:PORT>  Enable HTTP stats endpoint (/metrics, /stats)
      --report-interval <SECS>    Traffic-report log cadence (default 60)
      --sample-interval <SECS>    Throughput-rate sampling cadence (default 1)
      --buffer-bytes <BYTES>      Per-direction copy buffer size (default 65536)
      --socket-buffer-bytes <N>   Kernel socket buffer size per socket (default: OS default)
      --splice                    Use the Linux splice(2) zero-copy path (Linux only)
  -h, --help                      Print help
  -V, --version                   Print version
```

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
> (`buffer_bytes`, `socket_buffer_bytes`, `use_splice`) live — new connections
> pick up the new values; in-flight connections keep theirs. Changes to
> `report_interval_secs`, `sample_interval_secs`, and `metrics_addr` still
> require a restart.

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

- **Connection handling:** Every incoming TCP connection is accepted and proxied to the remote destination using `tokio::io::copy_bidirectional_with_sizes` (64 KiB buffers by default), with `TCP_NODELAY` set on both sides.
- **DNS resolution:** Each connection resolves the remote hostname via the system DNS resolver (using hickory-resolver), so DNS changes are picked up immediately.
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

All three knobs above are **hot-reloadable** via the YAML `settings:` block —
new connections adopt the new values without a restart.

## Logging

Logs go to:

- **Console** — Human-readable with colors and timestamps
- **File** — `logs/rfw.log` (JSON, daily rotation)

Set `RUST_LOG=debug` or `RUST_LOG=trace` for more verbosity.

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
  ```

- **`GET /stats`** — JSON array of per-forwarder snapshots:

  ```json
  [{"label":"127.0.0.1:8080->172.16.0.5:80","bytes_sent":1321205760,"bytes_received":4896202752,"active_connections":3,"rate_sent_bps":12897484,"rate_recv_bps":48234234}]
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

If you need to pin a specific image version, use `ghcr.io/manprint/rfw:v1.0.1` instead of `latest`.

Edit `docker-compose.yml` to configure your forwarders via env vars.

## Development

### Prerequisites

- Rust (latest stable)
- Linux/macOS (for cross-compilation build script: Docker)

### Build

```bash
cargo build --release
```

### Test

```bash
cargo test
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
│   └── e2e_forward.rs  # End-to-end forwarding + metrics test
└── README.md           # This file
```

## License

MIT
