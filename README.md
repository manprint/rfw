# rfw — Rust Forwarder

**rfw** (Rust Forwarder) is a fast, cross-platform TCP and UDP port forwarder. It listens on local ports and forwards all traffic to remote destinations — similar to `socat` but specialized and simpler for TCP and UDP forwarding.

## Features

- **TCP and UDP forwarding** — Bidirectional proxy between local and remote hosts; UDP keeps one upstream socket per client address (session), preserving datagram boundaries
- **UDP sessions** — Idle timeout (`udp_session_timeout_secs`, default 60s, hot-reloadable), global cap `udp_max_sessions` (default 16384), drops counted, never blocking other clients
- **UDP GRO/GSO (Linux, opt-in)** — `udp_gro_gso` / `--udp-gro-gso` receives coalesced UDP bursts and forwards them with segmented sends, cutting syscalls on high-packet-rate flows; datagram boundaries and order are unchanged (see [UDP GRO/GSO](#udp-grogso-linux-opt-in))
- **Multiple forwarders** — Run many forwarders in a single process
- **Port ranges** — `0.0.0.0:30000-30050:host:30000-30050` forwards a whole block, port for port, over TCP or UDP; one line on the CLI, in an env var or in YAML (`local_port_end`), validated for length, bounds and overlaps (see [Port ranges](#port-ranges))
- **Configuration sources** — CLI args, YAML file, or environment variables
- **Hot-reload** — Watch YAML config file for changes, apply without restart
- **Auto-reconnect** — Each forwarder retries independently on failure
- **Dynamic DNS** — Resolves remote hostnames per connection, honouring the record TTL, so a remote whose IP changes (cloud database failover) is followed by the next connection without a restart; fails over across all resolved addresses
- **TCP keepalive** — On by default on both sides of every connection, so a peer that vanishes without closing is reaped in ~90s instead of holding a slot forever
- **Cross-platform** — Windows, macOS, Linux (amd64 + arm64), Android (arm64)
- **Traffic stats** — Lifetime cumulative bytes + live throughput rate per forwarder, in periodic logs and an optional HTTP endpoint (`/metrics`, `/stats`)
- **Honest healthcheck** — `GET /` answers `200` only while every forwarder is actually bound and accepting; a forwarder stuck retrying a taken port turns it `503`
- **Target monitoring** — Each TCP target is probed (resolve + one TCP connect); UDP targets are checked passively, from ICMP errors and replies (DNS only, nothing sent). Probes run on a cadence that backs off while real traffic proves it reachable; exposed as `rfw_target_reachable`
- **Google Chat alerting** — Optional webhook (`--webhook-url` / `RFW_WEBHOOK_URL`) that reports startup with per-target reachability, DNS answer changes, a target unreachable for longer than a configurable threshold (default 5 minutes) and its recovery, ports that cannot be bound, reload errors, connection-limit saturation, internal faults and shutdown; throttled, retried, never on the data path
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
docker pull ghcr.io/manprint/rfw:v1.2.0
docker compose up -d
```

Or run manually:

```bash
docker run --rm -p 8080:8080 \
  -e RFW_FORWARDER_1=0.0.0.0:8080:example.com:80 \
  ghcr.io/manprint/rfw:v1.2.0 --log-dir ""
```

For the "database behind a cloud DNS name on the host network" deployment, see
[Production recipe](#production-recipe-database-behind-a-cloud-dns-name-socat-replacement).

## Forwarder syntax

```
local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp]
```

| Part | Description | Example |
|---|---|---|
| `local_host` | IP/hostname to bind locally | `127.0.0.1`, `0.0.0.0`, `localhost` |
| `local_port` | Port to listen on locally, or a range `first-last` | `8080`, `3306`, `30000-30050` |
| `remote_host` | Destination hostname/IP | `example.com`, `192.168.1.5` |
| `remote_port` | Destination port, or a range of the same length | `80`, `5432`, `30000-30050` |
| `/tcp`, `/udp` | Optional protocol suffix (case-insensitive); default `tcp` | `/udp` |

A TCP and a UDP forwarder may share the same local port; two UDP (or two TCP) forwarders may not.

**Examples:**

```
127.0.0.1:8080:192.168.1.100:80         → local web proxy
0.0.0.0:3306:db-prod.example.com:3306   → database tunnel
0.0.0.0:443:app.azure.com:443           → cloud service proxy
0.0.0.0:1053:1.1.1.1:53/udp             → DNS over UDP
0.0.0.0:30000-30050:tunnel.example.com:30000-30050   → 51 ports, one line
```

### Port ranges

A range on both sides forwards a block of ports, **port for port**: local port
`first + k` goes to remote port `first' + k`. The two sides need not start at
the same number.

```
0.0.0.0:30000-30050:tunnel.example.com:30000-30050      # 30000→30000 … 30050→30050
0.0.0.0:8000-8009:backend.internal:9000-9009            # 8000→9000 … 8009→9009
0.0.0.0:5000-5003:media.example.com:6000-6003/udp       # the same, over UDP
```

A range is expanded when the configuration is loaded, into one ordinary
forwarder per port. Everything else (hot-reload, `/stats`, metrics, logs,
`max_connections`) sees plain single-port forwarders, labelled
`0.0.0.0:30001->host:30001`. In particular, shrinking or growing a range in a
YAML file on a live reload stops or starts only the ports that left or
joined; the others keep their listener and their open connections.

Rules, all checked at load (on a hot-reload a violation keeps the running
configuration and reports the error):

- Both sides must have the **same number of ports**. A range on one side only
  (`30000-30050:host:80`) is an error: list the ports you mean, or use two
  ranges of equal length.
- `first` must not exceed `last`; a range may not include port 0; `last` is at
  most 65535. A one-port range (`80-80`) is allowed.
- At most **4096 ports** per range (`MAX_RANGE_PORTS`): each port is a real
  listener with its own monitor, and a typo such as `1-65535` would otherwise
  exhaust file descriptors.
- Local endpoints must not overlap, between ranges or between a range and a
  single port, in either order. A TCP range and a UDP range may share ports,
  as single forwarders may. The error names the port and both entries:
  `Duplicate local address '0.0.0.0:30040': defined by both
  '0.0.0.0:30000-30050->h:30000-30050' and '0.0.0.0:30040-30060->h:1-21'`.
- Only the port fields take ranges: a hyphen in a hostname
  (`db-prod.example.com`) is not affected.

Each port of a range is monitored separately, as if you had written it by
hand: every port probes its own target (`host:port`) and reports its own
reachability, so a backend that is down for the whole block produces one
"unreachable" alert per port, not one per range. A range of 50 ports against
one host also means 50 probes per `probe_interval` while idle.

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

  - local_host: "0.0.0.0"
    local_port: 1053
    remote_host: "1.1.1.1"
    remote_port: 53
    protocol: udp     # tcp (default) or udp
```

Usage:

```bash
rfw -f forwarders.yml
```

The `protocol` key accepts `tcp` or `udp` (case-insensitive); any other value is a parse error. A misspelled key name is ignored, and the forwarder then stays TCP.

The `settings:` block also accepts `udp_gro_gso: true` (Linux only, default `false`, hot-reloadable) to enable the UDP GRO/GSO fast path; see [UDP GRO/GSO](#udp-grogso-linux-opt-in).

The YAML file is **watched for changes** — edit it and rfw picks up new forwarders / removes stopped ones without restart. The `forwarders:` list and the data-plane knobs (`buffer_bytes`, `socket_buffer_bytes`, `use_splice`, timeouts, keepalive, probe cadence, alert threshold, `udp_session_timeout_secs`, `udp_gro_gso`) are hot-reloaded; interval, `max_connections`, `udp_max_sessions`, `metrics_addr`, `webhook_url` and `instance_name` changes need a restart.

A range in YAML adds `local_port_end` to an entry. `remote_port` is the remote
port that matches `local_port`, and the remote side follows at the same offsets:

```yaml
forwarders:
  # 30000→30000 … 30050→30050 (51 forwarders)
  - local_host: "0.0.0.0"
    local_port: 30000
    local_port_end: 30050
    remote_host: "tunnel.example.com"
    remote_port: 30000

  # 5000→6000 … 5003→6003 over UDP; remote_port_end is optional and, when
  # present, must span the same number of ports
  - local_host: "0.0.0.0"
    local_port: 5000
    local_port_end: 5003
    remote_host: "media.example.com"
    remote_port: 6000
    remote_port_end: 6003
    protocol: udp
```

`remote_port_end` without `local_port_end` is an error. See
[Port ranges](#port-ranges) for the rules.

### 3. Environment variables

```bash
export RFW_FORWARDER_1=127.0.0.1:8080:172.16.0.5:80
export RFW_FORWARDER_2=0.0.0.0:3306:db-prod.example.com:3306
export RFW_FORWARDER_3=0.0.0.0:1053:1.1.1.1:53/udp
export RFW_FORWARDER_4=0.0.0.0:30000-30050:tunnel.example.com:30000-30050   # a range
rfw
```

**Precedence (highest to lowest):** Env vars > CLI args > YAML file

## Full CLI reference

```
Usage: rfw [OPTIONS] [FORWARDERS]...

Arguments:
  [FORWARDERS]...  Forwarders in format local_host:local_port[-end]:remote_host:remote_port[-end][/tcp|/udp] (a port range maps port for port)

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
      --webhook-url <URL>         Google Chat incoming-webhook URL for alerts
                                  [env: RFW_WEBHOOK_URL] (default: off)
      --instance-name <NAME>      Name shown in alerts [env: RFW_INSTANCE_NAME]
                                  (default: hostname)
      --probe-interval <SECS>     Seconds between active target probes
                                  (default 30)
      --unreachable-after <SECS>  Continuous failure before the "target
                                  unreachable" alert (default 300 = 5 min)
      --udp-session-timeout <SECS>
                                  Seconds a UDP session may stay idle before
                                  it is closed (default 60; hot-reloadable)
      --udp-max-sessions <N>      Max concurrent UDP sessions across all UDP
                                  forwarders (default 16384; startup only)
      --udp-gro-gso               Linux only: use UDP GRO/GSO for UDP
                                  forwarders (default off; hot-reloadable)
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
block (CLI flags take precedence, at startup and on hot-reload). See `forwarders.yml`.

## Configuration override behavior

1. If **environment variables** (`RFW_FORWARDER_1`, `RFW_FORWARDER_2`, ...) are set, they **completely replace** CLI args and YAML config.
2. If **CLI args** are provided, they **replace** the YAML config (but can be overridden by env).
3. If **neither** env vars nor CLI args are given, the **YAML config file** is used.
4. If **no config source** is found, rfw prints an error and exits.
5. On hot-reload the same precedence holds: command-line flags are re-applied over the file's `settings:` block, and when the forwarders come from `RFW_FORWARDER_N` or positional arguments the file's `forwarders:` list is ignored (logged at startup and on every reload); only its settings are applied.

## Hot-reload

When using `-f forwarders.yml`, rfw watches the YAML file for changes:

- **New forwarders** added to the file are started automatically
- **Removed forwarders** stop accepting new connections (in-flight connections keep running until they close)
- **Unchanged forwarders** are unaffected
- **Pinned forwarders**: if the forwarders were given via env vars or positional arguments, the `forwarders:` list in the file is not applied on reload

Changes are debounced (1s window) to avoid reload storms.

> **Note:** Hot-reload applies the `forwarders:` list and the data-plane knobs
> (`buffer_bytes`, `socket_buffer_bytes`, `use_splice`, `connect_timeout_secs`,
> `dns_timeout_secs`, `keepalive_idle_secs`, `keepalive_interval_secs`,
> `keepalive_retries`, `probe_interval_secs`, `unreachable_after_secs`,
> `udp_session_timeout_secs`, `udp_gro_gso`) live —
> new connections pick up the new values; in-flight connections keep theirs.
> For `udp_gro_gso` the listener follows within one second (the next sweep
> tick), while a UDP session keeps the mode it was opened with until it closes.
> Turning it off stops GRO on new sessions; a listener that already has
> `UDP_GRO` keeps it until restart (its receives stay GRO-aware, and
> datagram boundaries are still preserved).
> CLI flags still win over the reloaded `settings:` block.
> Changes to `report_interval_secs`, `sample_interval_secs`, `max_connections`,
> `udp_max_sessions`, `metrics_addr`, `webhook_url` and `instance_name` still require a restart.
>
> Every reload outcome is alerted when a webhook is configured: a successful
> one names the forwarders added and removed, a rejected one says the running
> configuration was kept.

**A reload is applied all-or-nothing.** The file is parsed once, and both the
`forwarders:` list and the `settings:` block come from that single parse. If the
file is unparseable (a typo, or a half-written save), the reload is refused and
the running configuration — forwarders *and* knobs — is left untouched, with the
reason logged. Duplicate local addresses are rejected the same way.

Re-targeting a forwarder (same local port, different remote) tears down its
listener and rebuilds it on the same port; the old socket is fully released
before the rebind, so the port does not go dark in between. For a UDP
forwarder the existing sessions are closed with it, and the client's next
datagram opens a new session.

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
│  ┌────────────────────┐  ┌──────────────────┐   │
│  │  Target Monitor    │  │  Webhook Notifier│   │
│  │  (one per fwd)     │  │  (Google Chat)   │   │
│  └────────────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────┘
```

- **Connection handling:** Every incoming TCP connection is accepted and proxied to the remote destination using `tokio::io::copy_bidirectional_with_sizes` (64 KiB buffers by default), with `TCP_NODELAY` and TCP keepalive set on both sides.
- **DNS resolution:** Each connection resolves the remote hostname through hickory-resolver, configured from the system `/etc/resolv.conf`. Answers are cached for their TTL, so a burst of connections costs one lookup, and a record that changes is followed by the first connection after the TTL expires. A queries are answered first and AAAA is only consulted when A yields nothing, so a resolver that is slow to answer AAAA cannot stall connections.
- **Traffic stats:** Bytes sent/received are counted per forwarder as **lifetime cumulative** counters (never reset). A sampler derives the current throughput rate (bytes/sec, EWMA-smoothed). Both are logged periodically and exposed live via the optional HTTP endpoint.
- **Target monitoring:** One monitor task per forwarder resolves the target and opens (then closes) a TCP connection every `probe_interval_secs`, feeding `rfw_target_reachable`. The probe is skipped while real connections keep succeeding, and pulled forward when a real connection fails, so the downtime clock starts within a second of the first client-visible failure. The resolved address set is shared with the data path, so a DNS change is reported exactly once, by whichever sees it first.
- **Alerting:** Events go into a bounded queue drained by one delivery task (`POST {"text": …}` to the webhook, rustls + bundled roots, 3 attempts with backoff, per-kind throttling). Producers never wait on it: a slow or dead webhook costs a counter, not a connection. See [Alerting to Google Chat](#alerting-to-google-chat-webhook).
- **Logging:** All connection/disconnection/error events are logged to stdout (color, human-friendly) and to `logs/rfw.log` (JSON, daily rotation).

### UDP

A UDP forwarder has no connection to accept. Each client address gets a
**session**: one upstream socket, connected to the target, so the kernel
filters the replies and delivers ICMP errors to it. Datagram boundaries are
preserved in both directions, and each session has its own task for the
replies from the target.

- The listener reads up to 64 datagrams per wakeup. It never waits on the
  client-to-target path: when the upstream send buffer is full, the datagram
  is dropped and counted.
- Sessions idle for `udp_session_timeout_secs` are closed; a sweep runs every
  second.
- For a remote hostname, the DNS refresh runs every second. When the set of
  addresses changes, sessions towards addresses that are no longer published
  are closed, and new sessions use the first address of the new set.
- On stop, UDP sessions get up to 2 seconds to finish, after which the port is
  released.

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

  Measured (see [`docs/benchmarks/BENCHMARK_REPORT.md`](docs/benchmarks/BENCHMARK_REPORT.md)):
  on bulk transfers splice moves 1.2–1.7× the data at about half the CPU, and with
  `--buffer-bytes 262144` (pipe size) it matches Docker's userland proxy at the same
  CPU. It does not help tiny-message traffic (a database forwarder), where it costs
  a few percent of saturated throughput, so leave it off there. Splice needs
  kernel pipes: the per-user pipe budget (`fs.pipe-user-pages-soft`) is shared by
  every process of the same uid. When rfw cannot get useful pipes it falls back to
  the buffered path for that connection and logs one warning per minute; in a root
  container add `--cap-add SYS_RESOURCE` (much narrower than `--privileged`) to lift
  the limit.

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

### UDP tuning

- **`--socket-buffer-bytes <N>`** applies to the UDP listener (at bind) and to
  every new upstream socket, best effort. The listener reads back
  `SO_RCVBUF`; when the kernel caps it below the request, rfw logs
  `UDP listener receive buffer capped by the kernel; raise net.core.rmem_max`.
  Check the limits with `sysctl net.core.rmem_max net.core.wmem_max`, and look
  for `RcvbufErrors` in `netstat -su` or `nstat`.
- **`--buffer-bytes`** does not apply to UDP: each receive uses a fixed 64 KiB
  buffer, enough for the largest IPv6 datagram.
- **File descriptors:** each session holds one upstream socket. At startup rfw
  raises the soft open-file limit to `udp_max_sessions + 256` when the hard
  limit allows it. If it cannot, rfw logs a warning naming `LimitNOFILE`, and
  sessions beyond the limit fail to open.
- **Limitation:** with a wildcard bind on a multi-homed host, a reply can leave
  from a different source IP than the one the client contacted (no
  `IP_PKTINFO`). Bind the specific address instead.
- `--udp-max-sessions <N>` (default 16384) caps all UDP sessions together. A
  datagram from a new client beyond the cap is dropped and counted in
  `rfw_rejected_connections_total`; the other sessions keep working.
- `--udp-session-timeout <SECS>` (default 60) sets how long an idle session
  lives, and it is hot-reloadable.

### UDP GRO/GSO (Linux, opt-in)

With `udp_gro_gso: true` (YAML) or `--udp-gro-gso` (CLI), rfw asks the kernel
to coalesce bursts of equal-sized UDP datagrams on receive (`UDP_GRO`) and
sends such a burst on with a single segmented `sendmsg` (`UDP_SEGMENT`), in both
directions. rfw never merges datagrams itself: only a buffer the kernel already
delivered coalesced is sent segmented, with the same segment size, so the
datagrams leave exactly as they arrived (same boundaries, same order). A
datagram that arrives alone is forwarded alone. Counters stay per datagram:
`datagrams_*` count segments and `bytes_*` count payload bytes. A single send
carries at most 64 segments and 65 507 bytes; longer bursts are split.

When it helps: high-packet-rate flows where the peer or the NIC already uses
GSO/GRO, typically bulk QUIC-like transfers. For sparse, single-datagram
traffic it brings no benefit, which is why it is off by default.

- **Requirements:** Linux with kernel 5.0+ (`UDP_GRO`). On an older kernel rfw
  logs `UDP GRO is not supported by this kernel (Linux 5.0+ required); udp_gro_gso has no effect`
  once per forwarder and carries on without it. If setting `UDP_GRO` on the
  listener fails for another reason, rfw logs
  `Setting UDP_GRO on the listener failed; udp_gro_gso has no effect on this forwarder until restart`
  once.
- **Hot reload:** the listener follows the setting at the next sweep tick (at
  most one second) and logs `UDP GRO/GSO enabled on the listener`. A UDP session
  fixes its mode when it opens and keeps it until it closes. Turning the setting
  off stops GRO on **new** sessions and logs
  `UDP GRO/GSO disabled on the listener for new sessions (UDP_GRO stays set on the listener socket until restart)`:
  a listener that already has `UDP_GRO` keeps it until restart, and its receives
  stay GRO-aware, so bursts already queued are still forwarded as separate
  datagrams.
- **Automatic fallback:** if the kernel rejects a segmented send, rfw splits the
  buffer in userspace, delivers every datagram, and sends one datagram at a time
  from then on for that session (client to target) or for the listener (replies). It logs one of
  `UDP GSO rejected by the kernel for a session; it now sends one datagram at a time` (client to target) or
  `UDP GSO rejected by the kernel on the listener; replies now go one datagram at a time` (target to client).
- **Other platforms:** ignored. rfw logs
  `udp_gro_gso is only supported on Linux; ignored` at startup when it is set.
- **Benchmark:** `cargo test --release --test e2e_udp udp_gso_bench -- --ignored --exact --nocapture`
  runs two rfw instances (plain and `--udp-gro-gso`) against a GRO/GSO echo
  backend and prints a `UDP_GSO_BENCH` line with the throughput of each, the
  speedup and the loss.

## Logging

Logs go to:

- **Console** — Human-readable with colors and timestamps
- **File** — `logs/rfw.log` (JSON, daily rotation), unless disabled

Both sinks honour `RUST_LOG` (default `info`). Set `RUST_LOG=debug` for
per-connection detail, or `RUST_LOG=warn` to quieten both.

Per-connection lifecycle records ("Connected" / "Disconnected") are at **debug**
level. At `info` a busy forwarder logs only startup, reloads, the periodic
traffic report, and genuine problems.

For UDP, the startup line is `Listening (udp) on …`. Drops, session-cap
refusals, EMFILE and DNS failures are warnings or errors, rate-limited per
kind, and carry a `suppressed` count. Opening and closing a session is logged at
**debug** (with the client, the local and remote addresses, the reason, the
duration and the byte counts); per-datagram events are never logged above
debug. To see them, run with `RUST_LOG=debug`.

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

UDP forwarders have no in-flight connections to drain: their sessions are
closed at once (each forwarder releases its port within 2 seconds), and the
in-flight count in the shutdown alert covers TCP connections only.

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
  0.0.0.0:1053->1.1.1.1:53/udp       sent=  2.100 MB  recv=  3.400 MB  tx=  0.100 MB/s  rx=  0.200 MB/s  sessions=12  dgrams tx=8800 rx=8795 drop=5
  TOTAL (all forwarders, lifetime)    sent=  1.230 GB  recv=  4.560 GB
──────────────────────────────────────────────────────
```

UDP rows show `sessions=` (live sessions) and `dgrams tx= rx= drop=` (datagrams
forwarded in each direction, and dropped) in place of `active=`.

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
  # TYPE rfw_target_reachable gauge
  rfw_target_reachable{label="127.0.0.1:8080->172.16.0.5:80"} 1
  # TYPE rfw_webhook_enabled gauge
  rfw_webhook_enabled 1
  # TYPE rfw_webhook_sent_total counter
  rfw_webhook_sent_total 4
  # TYPE rfw_webhook_failed_total counter
  rfw_webhook_failed_total 0
  # TYPE rfw_webhook_dropped_total counter
  rfw_webhook_dropped_total 0
  # TYPE rfw_webhook_suppressed_total counter
  rfw_webhook_suppressed_total 0
  ```

  `rfw_target_reachable` is the last probe verdict per forwarder (absent until
  the first probe, so an unknown target never looks like a down one). The
  `rfw_webhook_*` family is process-level: how many alerts were accepted by
  the webhook, given up on after retries, dropped because the queue was full,
  or folded into a later message by the throttle.

  For UDP forwarders the endpoint adds `rfw_datagrams_sent_total`,
  `rfw_datagrams_received_total` and `rfw_datagrams_dropped_total`; these
  families appear only when a UDP forwarder is configured. For a UDP forwarder,
  `rfw_active_connections` counts its live sessions, and
  `rfw_rejected_connections_total` counts datagrams from new clients refused by
  `udp_max_sessions`.

- **`GET /stats`** — JSON array of per-forwarder snapshots:

  ```json
  [{"label":"127.0.0.1:8080->172.16.0.5:80","protocol":"tcp","bytes_sent":1321205760,"bytes_received":4896202752,"active_connections":3,"rejected_connections":0,"rate_sent_bps":12897484,"rate_recv_bps":48234234,"listening":true,"target_reachable":true}]
  ```

  `target_reachable` is `true`/`false` after the first probe and `null` before.
  Each entry carries a `protocol` field (`"tcp"` or `"udp"`). UDP entries also
  carry `datagrams_sent`, `datagrams_received` and `datagrams_dropped`.

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

## Alerting to Google Chat (webhook)

rfw can push operational alerts to a Google Chat space through an
[incoming webhook](https://developers.google.com/workspace/chat/quickstart/webhooks).
In the space: *Apps & integrations → Webhooks → Add webhook*, copy the URL
(`https://chat.googleapis.com/v1/spaces/<space>/messages?key=…&token=…`) and
hand it to rfw:

```bash
# Preferred: the URL carries the space's credentials, keep it out of `ps` and shell history
export RFW_WEBHOOK_URL='https://chat.googleapis.com/v1/spaces/AAAA/messages?key=…&token=…'
rfw --instance-name db-proxy-prod 0.0.0.0:5432:mydb.example.com:5432

# Equivalent flag / YAML key
rfw --webhook-url "$RFW_WEBHOOK_URL" ...
```

```yaml
settings:
  webhook_url: "https://chat.googleapis.com/v1/spaces/AAAA/messages?key=…&token=…"
  instance_name: "db-proxy-prod"   # default: hostname
  probe_interval_secs: 30          # active target probe cadence
  unreachable_after_secs: 300      # 5 minutes of continuous failure before the alert
```

### What gets sent

| Message | When | Level |
|---|---|---|
| **rfw started** | All forwarders spawned. Lists every forwarder with the result of one probe of its target: `✅ reachable at <ip:port> (<ms>)` or `❌ unreachable: <error>`. | info (warning if any target is unreachable) |
| **DNS change** | The target hostname resolves to a different address set than before (seen by a probe or by a real connection, whichever first). Shows old → new. | info |
| **Target unreachable** | Probes of a target have failed continuously for `unreachable_after_secs` (default 300 = 5 min). Sent **once** per outage, with the last error. Shorter blips are logged, not alerted. | critical |
| **Target reachable again** | First successful probe after an alerted outage, with the outage length and the address that answered. | info |
| **Cannot bind** | A forwarder's local port cannot be bound (taken, permission). Repeats of the 5 s bind retry are folded into one message per 5 min. | critical |
| **Listening again** | The port bound after one or more failures. | info |
| **Config reloaded** | Hot-reload applied; names forwarders added and removed. | info |
| **Config reload failed** | The edited file did not parse or validate; running configuration kept. | warning |
| **Connection limit reached** | `max_connections` is saturated and new connections are being refused (at most one message per 5 min per forwarder). | warning |
| **UDP session limit reached** | `udp_max_sessions` is saturated and datagrams from new clients are being dropped (at most one message per 5 min per forwarder). | warning |
| **Internal problem** | Anything else that breaks a component: the stats endpoint cannot bind, the config watcher dies, a forwarder task panics. | warning |
| **rfw stopping** | `SIGTERM`/`SIGINT` received; says how many connections are being drained. Sent while the drain runs. | info |

Every message ends with `<instance> · pid <pid> · rfw <version> · <UTC time>`,
because several rfw instances usually post into one space.

### How the target is probed

Every `probe_interval_secs` the monitor resolves the target name and opens one
TCP connection to it with the same DNS policy and Happy-Eyeballs racing as a
real connection, then closes it. To keep this cheap and quiet:

- the probe is **skipped** while a real connection to that target succeeded
  within the last interval (a busy, healthy forwarder costs the backend no
  extra connections);
- a real connection that **fails** kicks a probe immediately (at most one per
  second), so the downtime clock starts within a second of the first
  client-visible failure rather than at the next tick;
- while the target is down, probes run every 5 s so the alert lands close to
  the threshold and recovery is noticed within seconds.

For a **UDP** target nothing is ever sent to it: the probe is DNS resolution
only. The verdict is passive, from the data path:

- **unreachable** when the forwarder's sessions receive ICMP port, host or
  network unreachable and no reply has arrived since;
- **recovered** at the first reply, or when new datagrams are sent and no ICMP
  error has arrived for 3 seconds;
- with no traffic, the state stays as it was, and no alert is sent.

The startup message says `resolved to … (udp: passive check)`. The same
thresholds apply as for TCP.

For a database target each probe is an empty connection: PostgreSQL logs it as
`incomplete startup packet` at `LOG` level. With the default 30 s cadence and a
forwarder that carries steady traffic, most ticks are skipped; raise
`probe_interval_secs` if the log line bothers you.

### Delivery guarantees

- **Never on the data path.** Events go into a bounded queue (256) with a
  non-blocking send; a full queue drops the event and bumps
  `rfw_webhook_dropped_total`. Accept loops and connections never wait on HTTP.
- **Bounded retries.** `429`, `5xx` and transport errors are retried up to 3
  times (1 s, 2 s backoff, `Retry-After` honoured up to 30 s); other `4xx` are
  not. Failures are logged and counted in `rfw_webhook_failed_total`; they never
  affect forwarding.
- **Throttled.** Repetitive kinds (DNS change, bind failure, limit reached,
  reload failure, internal) are limited to one message per window per
  forwarder; suppressed repeats are reported as a count in the next message.
  Deliveries are paced 1 s apart to stay inside Google Chat's per-space limit.
- **Flushed on shutdown.** Queued alerts (typically "stopping") get up to 3 s to
  go out after the connection drain.

### Security notes

- The webhook URL **is a credential**: whoever has it can post into the space.
  Pass it through `RFW_WEBHOOK_URL` (or a secrets manager that sets it) rather
  than a command-line flag, and keep it out of committed YAML. rfw redacts every
  query value in its own logs (`…/messages?key=***&token=***`) and never echoes
  the URL in an error.
- TLS is rustls with the bundled Mozilla root store, so it works in the
  distroless image without a system CA bundle. Redirects are refused: following
  one would replay the credentials against another host. Plain `http://` is
  accepted for local relays and tests, with a startup warning.
- Messages contain forwarder labels, target hostnames and resolved IPs. Post
  into a space whose members may see them.

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

# UDP: publish the port with the /udp suffix, or Docker forwards TCP only
docker run --rm -p 1053:1053/udp \
    -e RFW_FORWARDER_1=0.0.0.0:1053:1.1.1.1:53/udp \
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

In production pin a release tag (`ghcr.io/manprint/rfw:v1.2.0`) rather than
`latest`, which moves on every release. Edit `docker-compose.yml` to configure
your forwarders via env vars. For a UDP forwarder, publish its port with the
`/udp` suffix (`"1053:1053/udp"`); a port served over both TCP and UDP needs
both entries. With `network_mode: host` no publishing is needed.

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
| **Alerting** (with `RFW_WEBHOOK_URL` set) | Google Chat gets: a startup message confirming the database answered a TCP probe; a message on every DNS answer change (old → new IP); one message if the database stays unreachable for 5 minutes, and one when it answers again; a message if port 5432 cannot be bound; reload and shutdown notices. See [Alerting to Google Chat](#alerting-to-google-chat-webhook). |

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
  -e RFW_WEBHOOK_URL="$RFW_WEBHOOK_URL" \
  -e RFW_INSTANCE_NAME=db-proxy-prod \
  -e RUST_LOG=info \
  ghcr.io/manprint/rfw:v1.2.0 \
    --log-dir "" \
    --max-connections 1000 \
    --metrics-addr 127.0.0.1:9090 \
    --connect-timeout 5 \
    --dns-timeout 3 \
    --probe-interval 30 \
    --unreachable-after 300
```

- `--net=host` binds `0.0.0.0:5432` directly on the host, like `socat` did, and
  uses the host's `/etc/resolv.conf` for DNS.
- `--ulimit nofile=65536:65536`: each proxied connection holds two sockets and
  Docker's default soft limit is often 1024. rfw raises its own soft limit towards
  the hard limit at startup (and warns if `--max-connections` cannot fit), so this
  is no longer required, but it raises the ceiling that bounds that adjustment.
- `--stop-timeout 15` covers the 10s in-flight drain on `docker stop`.
- `--metrics-addr 127.0.0.1:9090`: on the host network `0.0.0.0` would expose
  the unauthenticated stats endpoint on every interface. Loopback keeps it
  local; the healthcheck reaches it from inside the same network namespace.
- `--max-connections` should stay below the database's own connection limit;
  with keepalive on, dead clients no longer eat into it.
- `--log-dir ""` leaves logging to the Docker log driver (rfw never prunes its
  own rotated files).
- `RFW_WEBHOOK_URL` (exported in the shell that runs the command, or injected by
  your secrets manager) turns on the Google Chat alerts; leave it unset to run
  silent. `RFW_INSTANCE_NAME` is what the messages call this host.
- `--probe-interval 30 --unreachable-after 300` are the defaults, spelled out:
  the database is probed every 30 s while idle, and an outage is alerted after
  5 minutes of continuous failure.

**Docker Compose**, same deployment:

```yaml
services:
  rfw-db:
    image: ghcr.io/manprint/rfw:v1.2.0
    container_name: rfw-db
    restart: always
    network_mode: host          # 0.0.0.0:5432 on the host, host DNS
    environment:
      - RFW_FORWARDER_1=0.0.0.0:5432:mydb.xxxxxxxxxxxx.eu-south-1.rds.amazonaws.com:5432
      - RFW_WEBHOOK_URL=${RFW_WEBHOOK_URL}   # from .env (not committed); unset = alerts off
      - RFW_INSTANCE_NAME=db-proxy-prod
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
      - --probe-interval
      - "30"
      - --unreachable-after
      - "300"
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

**Same deployment, configured from a YAML file** (hot-reload, several
databases, every knob in one place). Put this in `/opt/rfw/forwarders.yml` on
the host:

```yaml
# /opt/rfw/forwarders.yml — rfw in front of a managed PostgreSQL
forwarders:
  - local_host: "0.0.0.0"           # host network: this is the host's port 5432
    local_port: 5432
    remote_host: "mydb.xxxxxxxxxxxx.eu-south-1.rds.amazonaws.com"
    remote_port: 5432
  # Add a second database and rfw picks it up on save, no restart:
  # - local_host: "0.0.0.0"
  #   local_port: 5433
  #   remote_host: "replica.xxxxxxxxxxxx.eu-south-1.rds.amazonaws.com"
  #   remote_port: 5432

settings:
  metrics_addr: "127.0.0.1:9090"    # /metrics, /stats, / health — loopback only on host net
  max_connections: 1000             # keep below the database's own connection limit
  connect_timeout_secs: 5
  dns_timeout_secs: 3
  keepalive_idle_secs: 60           # dead clients reaped in 60 + 10 * 3 = 90 s
  keepalive_interval_secs: 10
  keepalive_retries: 3
  # Alerting: the URL is a credential, so it comes from RFW_WEBHOOK_URL in the
  # environment (a CLI/env value overrides this key). Uncomment only for a
  # file with restricted permissions.
  # webhook_url: "https://chat.googleapis.com/v1/spaces/AAAA/messages?key=…&token=…"
  instance_name: "db-proxy-prod"
  probe_interval_secs: 30           # probe the database every 30 s while idle
  unreachable_after_secs: 300       # alert after 5 minutes of continuous failure
```

Mount the **directory** containing the file rather than the single file —
editors save by replacing the inode, and a single-file bind mount keeps
pointing at the old one so the watcher never sees the change:

```bash
docker run -d \
  --name=rfw-db \
  --net=host \
  --restart=always \
  --ulimit nofile=65536:65536 \
  --stop-timeout 15 \
  --health-cmd '/usr/local/bin/rfw --health-check 127.0.0.1:9090' \
  --health-interval 30s --health-timeout 10s --health-retries 3 \
  -v /opt/rfw:/etc/rfw:ro \
  -e RFW_WEBHOOK_URL="$RFW_WEBHOOK_URL" \
  -e RUST_LOG=info \
  ghcr.io/manprint/rfw:v1.2.0 \
    -f /etc/rfw/forwarders.yml \
    --log-dir ""
```

```yaml
services:
  rfw-db:
    image: ghcr.io/manprint/rfw:v1.2.0
    container_name: rfw-db
    restart: always
    network_mode: host
    volumes:
      - /opt/rfw:/etc/rfw:ro
    environment:
      - RFW_WEBHOOK_URL=${RFW_WEBHOOK_URL}
      - RUST_LOG=info
    command: ["-f", "/etc/rfw/forwarders.yml", "--log-dir", ""]
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

Do **not** set `RFW_FORWARDER_N` in this variant: environment forwarders
replace the file's list entirely.

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

The suite has six parts:

| Target | What it covers |
|---|---|
| unit tests (`src/**`) | settings validation and clamping, runtime knobs, keepalive socket options, stats/EWMA, Prometheus + JSON rendering, the metrics endpoint's routing, health verdict and timeouts, accept-error classification, admission control, DNS/IP-literal resolution, **DNS change propagation against an in-test fake resolver** (new connections follow the record, held connections stay pinned, dead records fail fast and recover), staggered connect racing, manager start/stop/sync, **target monitor** (outage alert once past the threshold, recovery, blips below the threshold stay silent, probes skipped under healthy traffic and kicked by failures, DNS change with old/new sets), **webhook notifier** (message rendering per event, URL validation and credential redaction, throttle windows, delivery against a local HTTP sink: 2xx, 5xx retry, 4xx no-retry, dead endpoint, shutdown flush, full-queue drop) |
| `tests/e2e_forward.rs` | byte-exact forwarding and live cumulative metrics, on both the portable and `splice` paths |
| `tests/e2e_lifecycle.rs` | startup validation, `SIGTERM` drain, `max_connections` shedding, half-close propagation, abrupt-reset leak checks, the healthcheck probe including `503` while a port cannot be bound and recovery once it frees |
| `tests/e2e_hot_reload.rs` | re-targeting a port without an outage, add/remove, malformed and duplicate configs, live knob reloads, counter continuity |
| `tests/e2e_udp.rs` | UDP round trips and metrics, env and YAML `protocol: udp` with hot-reload re-target, TCP and UDP sharing a port, duplicate UDP rejected, idle-session expiry, `udp_max_sessions` cap and counting, `/` returning `503` while a UDP port is taken, `SIGTERM` with idle sessions, rate-limited drop logs, closed UDP target reported unreachable, a TCP and UDP reference scenario (also with `--udp-gro-gso`), the GRO/GSO flag preserving datagrams byte-exact, and the GRO/GSO hot-reload toggle; `udp_bench` and `udp_gso_bench` are ignored by default (run them with `cargo test --release --test e2e_udp udp_bench -- --ignored --nocapture` and `cargo test --release --test e2e_udp udp_gso_bench -- --ignored --exact --nocapture`) |
| `tests/e2e_webhook.rs` | the real binary against an in-process webhook sink: startup report with a reachable and an unreachable target, target outage → single alert → recovery, taken port → alert → recovery, reload success and failure, `SIGTERM` "stopping" message, invalid URL rejected without echoing it, `--help`, `rfw_target_reachable` and `rfw_webhook_*` on `/metrics` |

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
│   ├── udp.rs          # UDP forwarding engine (sessions, DNS refresh, fd budget)
│   ├── manager.rs      # Forwarder lifecycle management (+ per-forwarder monitor)
│   ├── monitor.rs      # Target reachability probes, DNS-change detection, outage alerts
│   ├── notify.rs       # Google Chat webhook: events, throttle, delivery worker
│   ├── stats.rs        # Traffic statistics (cumulative counters + rate + target state)
│   ├── metrics.rs      # HTTP /metrics + /stats endpoint
│   ├── logging.rs      # Tracing/logging setup
│   └── testutil.rs     # Test-only fake DNS server + resolver
├── tests/
│   ├── common/mod.rs     # Shared e2e harness (spawn rfw, backends, probes)
│   ├── e2e_forward.rs    # Byte-exact forwarding + live metrics
│   ├── e2e_lifecycle.rs  # Startup validation, SIGTERM, admission control
│   ├── e2e_hot_reload.rs # Config watcher, rebinds, malformed/duplicate configs
│   ├── e2e_udp.rs        # UDP forwarding, sessions, reload, monitor
│   └── e2e_webhook.rs    # Alerts against an in-process webhook sink
└── README.md           # This file
```

## License

MIT
