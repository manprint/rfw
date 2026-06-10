# rfw — Rust Forwarder

**rfw** (Rust Forwarder) is a fast, cross-platform TCP port forwarder. It listens on local ports and forwards all traffic to remote destinations — similar to `socat` but specialized and simpler for TCP-only use cases.

## Features

- **TCP forwarding** — Bidirectional proxy between local and remote hosts
- **Multiple forwarders** — Run many forwarders in a single process
- **Configuration sources** — CLI args, YAML file, or environment variables
- **Hot-reload** — Watch YAML config file for changes, apply without restart
- **Auto-reconnect** — Each forwarder retries independently on failure
- **Dynamic DNS** — Resolves remote hostnames fresh per connection (picks up DNS changes)
- **Cross-platform** — Windows, macOS, Linux (amd64 + arm64), Android (arm64)
- **Traffic stats** — Periodic reports (every 60s) with bytes sent/received per forwarder
- **Structured logging** — Console (human-friendly, color) + JSON file (machine-readable)
- **Efficient** — Async I/O with `tokio`, handles thousands of concurrent connections

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

Download from the [releases page](https://github.com/fabiocorneti/rfw/releases).

### Docker

```bash
docker compose up -d
```

Or run manually:

```bash
docker run --rm -p 8080:8080 rfw \
    -e RFW_FORWARDER_1=0.0.0.0:8080:example.com:80
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

The YAML file is **watched for changes** — edit it and rfw picks up new forwarders / removes stopped ones without restart.

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
  -f, --file <CONFIG_FILE>  Path to YAML configuration file
  -h, --help                Print help
  -V, --version             Print version
```

## Configuration override behavior

1. If **environment variables** (`RFW_FORWARDER_1`, `RFW_FORWARDER_2`, ...) are set, they **completely replace** CLI args and YAML config.
2. If **CLI args** are provided, they **replace** the YAML config (but can be overridden by env).
3. If **neither** env vars nor CLI args are given, the **YAML config file** is used.
4. If **no config source** is found, rfw prints an error and exits.

## Hot-reload

When using `-f forwarders.yml`, rfw watches the YAML file for changes:

- **New forwarders** added to the file are started automatically
- **Removed forwarders** are stopped (existing connections drain naturally)
- **Unchanged forwarders** are unaffected

Changes are debounced (1s window) to avoid reload storms.

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

- **Connection handling:** Every incoming TCP connection is accepted and proxied to the remote destination using `tokio::io::copy_bidirectional`.
- **DNS resolution:** Each connection resolves the remote hostname via the system DNS resolver (using hickory-resolver), so DNS changes are picked up immediately.
- **Traffic stats:** Bytes sent/received are counted per forwarder and reported as a summary every 60 seconds.
- **Logging:** All connection/disconnection/error events are logged to stdout (color, human-friendly) and to `logs/rfw.log` (JSON, daily rotation).

## Logging

Logs go to:

- **Console** — Human-readable with colors and timestamps
- **File** — `logs/rfw.log` (JSON, daily rotation)

Set `RUST_LOG=debug` or `RUST_LOG=trace` for more verbosity.

## Traffic report

Every 60 seconds, rfw prints a traffic summary:

```
─── Traffic Report ──────────────────────────────────
  127.0.0.1:8080->172.16.0.5:80      send=   1.23 MiB  recv=   4.56 MiB  active=3
  0.0.0.0:3306->db-prod:3306          send=   0.00 B    recv=   0.00 B    active=0
  TOTAL (all forwarders)              send=   1.23 MiB  recv=   4.56 MiB
──────────────────────────────────────────────────────
```

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

### Build and run

```bash
# Build the image
docker build -t rfw .

# Run with env vars
docker run --rm -p 8080:8080 -p 8081:8081 \
    -e RFW_FORWARDER_1=0.0.0.0:8080:example.com:80 \
    -e RFW_FORWARDER_2=0.0.0.0:8081:api.example.com:443 \
    rfw

# Or with a config file mounted
docker run --rm -p 8080:8080 \
    -v ./forwarders.yml:/etc/rfw/forwarders.yml:ro \
    rfw -f /etc/rfw/forwarders.yml
```

### Docker Compose

```bash
docker compose up -d
```

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
│   ├── forwarder.rs    # TCP forwarding logic
│   ├── manager.rs      # Forwarder lifecycle management
│   ├── stats.rs        # Traffic statistics
│   └── logging.rs      # Tracing/logging setup
└── README.md           # This file
```

## License

MIT
