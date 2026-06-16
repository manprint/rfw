# Performance & Continuous Bandwidth Stats — Design & Implementation Plan

> **Status:** planning (no code written). Implementation handoff doc.
> **Authoring model:** Opus 4.8 (architecture).
> **Implementation models:** annotated per sub-phase (Haiku = mechanical/bulk,
> Sonnet = features/refactor/tests, Opus = architecture-review gates only).
> **Target:** (1) report bandwidth **continuously** — lifetime cumulative totals
> *and* an instantaneous rate (bytes/sec), available at any moment, not only in a
> 60 s bucket that resets; (2) maximize forwarding throughput / minimize wasted
> bandwidth and latency; (3) close internal + e2e test gaps. Minimize token usage
> during implementation (delegate mechanical sub-phases to Haiku).

---

## 1. Context & problem

`rfw` is a Rust async TCP port-forwarder (tokio multi-thread, one task per
connection). Relevant current state, with anchors:

- **Data plane:** `proxy_connection` wraps the inbound stream in `CountingStream`
  then calls `tokio::io::copy_bidirectional(&mut counting, &mut outbound)` —
  `src/forwarder.rs:131-133`. One `tokio::spawn` per accepted connection —
  `src/forwarder.rs:69-74`.
- **Byte counting:** poll-level, inside `CountingStream` — `poll_read`
  `src/stats.rs:134-150`, `poll_write` `src/stats.rs:153-168`. Each poll does
  **two** `fetch_add(Relaxed)`: a per-connection counter (`bytes_read` /
  `bytes_written`, `src/stats.rs:118-119`) **and** the shared `ForwarderStats`.
- **Stats model:** `ForwarderStats` = three `AtomicU64` (`bytes_sent`,
  `bytes_received`, `active_connections`), `src/stats.rs:12-17`.
- **Reporting:** `stats_reporter` ticks a `tokio::time::interval(60 s)`
  (`src/forwarder.rs:167-169`), then calls `collect_and_reset`
  (`src/stats.rs:80-83`) → `snapshot_and_reset` which **`swap(0)`s the counters**
  (`src/stats.rs:51-56`) and logs one block (`src/forwarder.rs:178-204`).
- **Wiring:** `main.rs:64-67` spawns the reporter via
  `manager::start_stats_reporter` (`src/manager.rs:254-262`).
- **Sockets:** no `TCP_NODELAY`, no buffer sizing, default `copy_bidirectional`
  buffer (8 KiB) — nothing set anywhere in `forwarder.rs`.
- **Build:** no `[profile.release]` in `Cargo.toml` (no LTO / codegen-units /
  strip). CI (`.github/workflows/release.yml`) runs `bash ./build.sh` only — **no
  `cargo test` / `clippy` / `fmt` gate at all**.
- **Tests:** 12 unit tests (config + manager + one stats test). **No `tests/`
  dir, no e2e/integration test, no throughput test.** The one stats test
  (`src/stats.rs:184-211`) *depends on* the reset behavior (`snapshot_and_reset`
  returning the windowed value then zeroing).

### Three concrete problems

1. **No continuous bandwidth.** Counters are zeroed every 60 s, so (a) there is
   **no lifetime total** ("how much has this forwarder moved since start?") and
   (b) there is **no rate** — the log prints bytes-in-window but never divides by
   elapsed time, and it is visible only once per minute. You cannot ask "what is
   the bandwidth *right now*".
2. **Throughput / latency left on the table.** Default 8 KiB copy buffer, no
   `TCP_NODELAY` (Nagle adds latency on small writes), no release LTO, a dead
   second atomic increment per poll on the hot path.
3. **Test gaps.** No e2e proof that bytes survive end-to-end (the literal "non
   perdere banda" check), no rate/cumulative tests, no CI quality gate.

### Goal

Replace the "interval bucket + reset" stats model with a **lifetime-monotonic
cumulative** model plus a **derived instantaneous rate** (EWMA-smoothed
bytes/sec), readable at any instant. Expose it (a) in a richer periodic log and
(b) via an **opt-in HTTP endpoint** (`/metrics` Prometheus text + `/stats` JSON)
for continuous scraping. In the same pass, apply hot-path throughput
improvements and add the missing unit + e2e tests and a CI gate.

### Reference scenario (final acceptance test)

```
topology:  client ──▶ rfw (127.0.0.1:18080) ──▶ echo server (127.0.0.1:19000)
           rfw started with metrics endpoint enabled on 127.0.0.1:19090

steps:
  1. client connects, sends 100 MiB, echo returns it, client reads 100 MiB back
  2. during transfer: GET http://127.0.0.1:19090/stats  → rate_sent_bps > 0
  3. after transfer + >60 s idle: GET /metrics

expected:
  - client received exactly 104857600 bytes, byte-identical (no data loss)
  - rfw_bytes_sent_total{label="..."}      == 104857600   (client→remote)
  - rfw_bytes_received_total{label="..."}  == 104857600   (remote→client)
  - these totals are STILL 104857600 after the 60 s report tick (NOT reset to 0)
  - rfw_active_connections == 0 after client disconnects (no leak)
```

---

## 2. Approved design decisions

| # | Decision | Consequence |
|---|----------|-------------|
| **D1** | Counters become **lifetime-monotonic**; never `swap(0)`. | `snapshot_and_reset` → `snapshot` (uses `load`, no reset). Remove `collect_and_reset`/reset paths. The existing stats test (`src/stats.rs:184-211`) **must be rewritten** (loud behavior change). |
| **D2** | Rate is **derived**, not stored cumulatively: a single **sampler task** ticks every `sample_interval` (default **1 s**), computes `Δbytes/Δt`, EWMA-smooths it, and stores `rate_sent_bps` / `rate_recv_bps` as `AtomicU64` on `ForwarderStats`. | One writer (the sampler) → no contention; readers (log + HTTP) just `load`. Rate available at any instant without touching the hot path. Needs `std::time::Instant` in the sampler (prod code — allowed). |
| **D3** | Continuous exposure is an **opt-in HTTP endpoint** (`/metrics` Prometheus text, `/stats` JSON), default **off**. Enabled by config/CLI `metrics_addr`. | Zero behavior change when unset (safe to land). When set, bandwidth is queryable "sempre". Endpoint is **read-only**, GET-only. |
| **D4** | HTTP server is **dependency-free**, hand-rolled on tokio (GET-only, parse request line, fixed responses, connection-per-request). | No new heavy dep (project is lean; `tokio` `full` already present). Restricted surface keeps the hand-rolled parser safe. *Fallback if review rejects hand-rolling:* add `axum` — recorded in Risk register. |
| **D5** | `[profile.release]`: `opt-level=3`, `lto="fat"`, `codegen-units=1`, `strip=true`. **Do NOT set `panic="abort"`.** | Throughput + smaller binary. `panic="abort"` is explicitly rejected: a panic in one connection task must stay isolated, not abort the whole multi-forwarder process. |
| **D6** | Set `TCP_NODELAY(true)` on **both** inbound and outbound sockets. | Removes Nagle latency on small/interactive payloads; neutral for bulk. |
| **D7** | Replace `copy_bidirectional` with `copy_bidirectional_with_sizes`, buffer **64 KiB** each direction (configurable via `buffer_bytes`, default 65536). | Fewer syscalls on fast links → higher throughput. |
| **D8** | Remove the dead per-connection `bytes_read`/`bytes_written` atomics in `CountingStream` (never exported). | One fewer `fetch_add` per poll on the hot path + simpler type. |
| **D9** | `active_connections` decrement moves to an **RAII guard** (`Drop`). | Fixes the leak: today `dec_connections` (`src/forwarder.rs:73`) is skipped if the task panics. Guard guarantees decrement on every exit path. |
| **D10** | Per-connection `cfg.clone()` (`src/forwarder.rs:66`) → `Arc<ForwarderConfig>`. | Drops 2 `String` allocations per accepted connection. |
| **D11** | Gates command = `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings` + `cargo test`. Add a CI workflow running them. | Establishes the quality gate that does not exist today. |
| **D12** | `report_interval` (log cadence, default 60 s) and `sample_interval` (rate cadence, default 1 s) become configurable; log keeps printing every `report_interval` but now shows **cumulative totals + current rate**. | Backward-compatible default (still ~1 log/min) but the data shown is the new model. |

---

## 3. Target architecture

### 3.1 Data model — `ForwarderStats` (revised)

```
ForwarderStats {
    label: String,
    bytes_sent: AtomicU64,        // LIFETIME monotonic (client→remote)
    bytes_received: AtomicU64,    // LIFETIME monotonic (remote→client)
    active_connections: AtomicU64,
    rate_sent_bps: AtomicU64,     // NEW: last EWMA rate, bytes/sec, written only by sampler
    rate_recv_bps: AtomicU64,     // NEW
    // sampler-private previous sample (single-writer, no contention):
    last_sample_sent: AtomicU64,  // NEW
    last_sample_recv: AtomicU64,  // NEW
}
```

- Hot path (`add_sent`/`add_received`, `src/stats.rs:33-39`) **unchanged** — still
  one `fetch_add(Relaxed)` each, now on a counter that never resets.
- `snapshot(&self) -> StatsSnapshot { label, sent, recv, active, rate_sent_bps,
  rate_recv_bps }` via `load(Relaxed)` (replaces `snapshot_and_reset`).

### 3.2 Rate sampler (new mechanism)

```
sampler task (one, global), every `sample_interval` (default 1s):
  for each ForwarderStats s in registry:
      cur_sent = s.bytes_sent.load();  cur_recv = s.bytes_received.load()
      Δsent = cur_sent - s.last_sample_sent;  Δrecv = cur_recv - s.last_sample_recv
      dt = elapsed since previous tick (Instant)
      inst_rate = Δbytes / dt_secs
      ewma = α*inst_rate + (1-α)*prev_ewma     // α = 0.3 (D2)
      s.rate_sent_bps.store(ewma_sent);  s.rate_recv_bps.store(ewma_recv)
      s.last_sample_sent.store(cur_sent);  s.last_sample_recv.store(cur_recv)
```

EWMA state (`prev_ewma`) is held in the sampler task's local map keyed by label
(or read back from `rate_*_bps` — read-then-blend, since the sampler is the only
writer). Single writer ⇒ `Relaxed` is fine.

### 3.3 Data plane / control flow (revised)

```
accept ─▶ spawn task:
            _guard = ConnGuard::new(stats)        // D9: inc now, dec on Drop
            set_nodelay(true) on inbound           // D6
            resolve + connect outbound
            set_nodelay(true) on outbound          // D6
            counting = CountingStream::new(inbound, stats)   // D8: no per-conn atomics
            copy_bidirectional_with_sizes(&mut counting, &mut outbound, BUF, BUF)  // D7
            shutdown both
          // _guard dropped here on ANY exit path (return/err/panic) → dec
```

### 3.4 Exposure paths

```
                 ┌─ stats_reporter (every report_interval, default 60s)
ForwarderStats ──┤     logs: cumulative total + current rate per forwarder + TOTAL
   (registry)    │
                 ├─ sampler (every sample_interval, default 1s) → updates rate_*_bps
                 │
                 └─ http server (opt-in, metrics_addr) [D3/D4]
                       GET /metrics → Prometheus text (counters + gauges)
                       GET /stats   → JSON array of snapshots
                       GET /        → 200 "rfw ok"; else 404
```

### 3.5 Reuse map (do not reinvent)

| Need | Reuse | Location |
|------|-------|----------|
| Iterate all forwarder stats | `StatsRegistry.stats: RwLock<Vec<Arc<ForwarderStats>>>` | `src/stats.rs:60-89` |
| Human byte formatting (log) | `format_bytes(n)` | `src/stats.rs:98-111` |
| Spawn a long-lived bg task w/ cancel | `start_stats_reporter` pattern | `src/manager.rs:254-262` |
| Cancellation wiring | `CancellationToken` usage in `main.rs` | `src/main.rs:63-67,75` |
| Per-connection byte counting | `CountingStream` (keep wrapper; strip dead atomics) | `src/stats.rs:113-177` |
| Config struct + serde derive + CLI | `ForwarderConfig`, `ConfigFile`, `CliArgs` | `src/config.rs:8-48` |
| Registry handle accessor | `manager.stats_registry()` | used at `src/main.rs:65` |
| Test pattern for streams | `tokio::io::duplex` test | `src/stats.rs:184-211` |

---

## 4. New interface (CLI flags / config)

Add to `ForwarderConfig`'s file-level config (top-level `ConfigFile`, not
per-forwarder) and `CliArgs`. Mirror the existing `clap` derive style
(`src/config.rs:34-48`).

| Name | Type | Default | Meaning |
|------|------|---------|---------|
| `--metrics-addr` / `metrics_addr:` | `Option<String>` (`host:port`) | `None` (off) | Bind addr for HTTP stats endpoint. Unset ⇒ no server (no behavior change). |
| `--report-interval` / `report_interval_secs:` | `u64` | `60` | Log report cadence (seconds). |
| `--sample-interval` / `sample_interval_secs:` | `u64` | `1` | Rate sampling cadence (seconds). |
| `--buffer-bytes` / `buffer_bytes:` | `usize` | `65536` | Per-direction copy buffer (D7). |

Precedence: CLI overrides YAML overrides default, same merge approach already used
by `load_forwarders` (`src/config.rs` load path). These are **global** settings
(one HTTP server, one sampler, one reporter for the whole process).

`ConfigFile` gains an optional `settings:` block (serde `#[serde(default)]`) so
existing `forwarders.yml` files keep parsing unchanged (backward-compat).

---

## 5. New data structures

```rust
// src/stats.rs
#[derive(Debug, Clone, serde::Serialize)]
pub struct StatsSnapshot {
    pub label: String,
    pub bytes_sent: u64,        // lifetime
    pub bytes_received: u64,    // lifetime
    pub active_connections: u64,
    pub rate_sent_bps: u64,     // current EWMA, bytes/sec
    pub rate_recv_bps: u64,
}
```

- `serde::Serialize` enables `/stats` JSON for free (`serde` already a dep).
- `ForwarderStats` gains the four new `AtomicU64` fields from §3.1 (additive).
- **Backward-compat / behavior change to flag loudly:**
  `snapshot_and_reset` and `collect_and_reset` are **removed**; callers switch to
  non-resetting `snapshot` / `collect`. The reset semantics disappear — this is
  the whole point (D1). The one test depending on reset is rewritten in 1.4.

---

## 6. Implementation phases

**Global rules:** tests alongside each change; every sub-phase must pass the
gates `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test`; **zero regressions**; update docs when behavior/flags change;
**print the model used per sub-task**.

Each sub-phase lists: **Model · Files · Change · Unit tests · e2e tests · Done.**

---

### Phase 0 — Build profile & CI gate

> Pure additive. No behavior change. Safe to land alone. Establishes the gate the
> rest of the plan relies on.

#### 0.1 Add release profile
- **Model:** Haiku
- **Files:** `Cargo.toml` (append new section).
- **Change:** Add `[profile.release]` with `opt-level = 3`, `lto = "fat"`,
  `codegen-units = 1`, `strip = true`. **Do not** add `panic = "abort"` (D5).
- **Unit tests:** none (build config).
- **e2e tests:** none.
- **Done:** `cargo build --release` succeeds; binary still runs `rfw -f
  forwarders.yml`; gates green.

#### 0.2 Add CI quality-gate workflow
- **Model:** Haiku
- **Files:** new `.github/workflows/ci.yml`.
- **Change:** On push/PR: `cargo fmt --check`, `cargo clippy --all-targets -- -D
  warnings`, `cargo test`. Mirror the runner/setup style of
  `.github/workflows/release.yml`.
- **Unit tests:** n/a.
- **e2e tests:** the workflow itself runs the suite.
- **Done:** workflow file valid YAML; the three commands pass locally.

---

### Phase 1 — Cumulative + rate stats model  ⟶ **Opus design review** (data-model, D1/D2)

> Replaces reset-bucket with lifetime-monotonic counters + derived rate. This is
> the core of the user's request. Independently shippable (log output improves;
> HTTP comes in Phase 2).

#### 1.1 Make counters lifetime-monotonic + add rate fields
- **Model:** Opus design review → **Sonnet** implements.
- **Files:** `src/stats.rs:12-56` (struct + methods), `src/stats.rs:78-88`
  (`StatsRegistry`).
- **Change:** Add fields per §3.1 (`rate_sent_bps`, `rate_recv_bps`,
  `last_sample_sent`, `last_sample_recv`). Replace `snapshot_and_reset` →
  `snapshot(&self) -> StatsSnapshot` using `load` (no `swap`). Replace
  `collect_and_reset` → `collect(&self) -> Vec<StatsSnapshot>`. Add
  `StatsSnapshot` struct (§5). Keep `add_sent`/`add_received` unchanged.
- **Unit tests:** `stats_counters_are_cumulative_not_reset` — add 100, snapshot,
  add 50, snapshot ⇒ second snapshot reads 150 (proves no reset).
- **e2e tests:** none (covered Phase 4).
- **Done:** `snapshot` never zeroes; `cargo test` green; no `swap(` left in
  `stats.rs`.

#### 1.2 Rate sampler task
- **Model:** **Sonnet**
- **Files:** `src/stats.rs` (new `pub async fn rate_sampler(registry, cancel,
  interval)`), or `src/forwarder.rs` next to `stats_reporter` (`:166-207`) —
  implementer's choice, keep beside reporter.
- **Change:** Implement §3.2: tick `sample_interval`, compute Δ/Δt, EWMA (α=0.3),
  store `rate_*_bps`, update `last_sample_*`. Use `std::time::Instant` for dt.
  `tokio::select!` on `cancel` like `stats_reporter` (`src/forwarder.rs:172-177`).
- **Unit tests:** `rate_sampler_computes_bps` — manually set `bytes_sent`,
  invoke the single-tick compute helper with a known dt, assert
  `rate_sent_bps` ≈ expected (factor out the per-tick math into a pure
  `fn compute_rate(prev, cur, dt, prev_ewma) -> u64` and unit-test that directly,
  no timing flakiness).
- **e2e tests:** none.
- **Done:** pure `compute_rate` covered by tests; sampler respects cancel.

#### 1.3 Reporter logs cumulative + rate; configurable interval
- **Model:** **Sonnet**
- **Files:** `src/forwarder.rs:166-207` (`stats_reporter`),
  `src/manager.rs:254-262` (`start_stats_reporter` signature),
  `src/main.rs:62-67` (wiring), plus spawn the sampler from `main.rs`.
- **Change:** `stats_reporter` takes `report_interval: Duration`; calls
  `collect` (not reset); log line shows **cumulative total + current rate**, e.g.
  `label  total_sent=<fmt>  total_recv=<fmt>  rate_tx=<fmt>/s  rate_rx=<fmt>/s
  active=N`. Reuse `format_bytes` (`src/stats.rs:98`). Spawn `rate_sampler` from
  `main.rs` alongside the reporter, sharing `stats_cancel`.
- **Unit tests:** none new (log formatting); rely on 1.1/1.2.
- **e2e tests:** none.
- **Done:** running `rfw` and pushing traffic shows growing cumulative totals
  across multiple report ticks (no drop to 0) and a nonzero rate during transfer.

#### 1.4 Rewrite the reset-dependent test  ⚠ **behavior change**
- **Model:** **Sonnet**
- **Files:** `src/stats.rs:184-211`.
- **Change:** The existing `counting_stream_updates_stats_live_across_snapshots`
  asserts `snapshot_and_reset` zeroes between reads. Rewrite to use `snapshot`
  and assert **cumulative growth**: after `b"ping"` read ⇒ sent==4; after
  `b"pong"` write ⇒ sent==4 (unchanged), recv==4 (cumulative, not windowed).
- **Unit tests:** the rewritten test is the deliverable.
- **e2e tests:** none.
- **Done:** test reflects cumulative semantics; `cargo test` green.

---

### Phase 2 — Continuous HTTP exposure  ⟶ **Opus design review** (new interface/dep surface, D3/D4)

> Opt-in (`metrics_addr` unset ⇒ no server, zero behavior change). Gives the
> "bandwidth always, as the app runs" capability.

#### 2.1 Config + CLI fields
- **Model:** Haiku (mirrors existing `clap`/serde pattern).
- **Files:** `src/config.rs:8-48` (add `settings` block + `CliArgs` flags per §4),
  config merge/load path, `forwarders.yml` (document the new optional block).
- **Change:** Add `metrics_addr`, `report_interval_secs`, `sample_interval_secs`,
  `buffer_bytes` with `#[serde(default)]` + clap defaults. Plumb into `main.rs`.
- **Unit tests:** `test_settings_defaults` (omitted block ⇒ defaults),
  `test_cli_overrides_settings` (CLI wins). Mirror
  `test_load_forwarders_env_overrides_cli_*` (`src/config.rs:264-303`).
- **e2e tests:** none.
- **Done:** existing `forwarders.yml` parses unchanged; defaults applied.

#### 2.2 Hand-rolled HTTP stats server
- **Model:** Opus design review → **Sonnet** implements.
- **Files:** new `src/metrics.rs` (+ `mod metrics;` in `src/main.rs:1-5`); spawn
  from `main.rs` when `metrics_addr.is_some()`.
- **Change:** `pub async fn serve(addr, registry, cancel)`: `TcpListener::bind`,
  accept loop in `tokio::select!` w/ cancel (mirror `run_forwarder`
  `src/forwarder.rs:54-85`). Per conn: read until `\r\n\r\n`, parse `GET <path>`:
  `/metrics` → Prometheus text (`rfw_bytes_sent_total`, `rfw_bytes_received_total`
  as `# TYPE counter`; `rfw_bytes_sent_rate_bps`, `rfw_active_connections` as
  gauges; `label` as a label dimension); `/stats` → JSON `serde_json`-style array
  of `StatsSnapshot` (add `serde_json` dep, or hand-serialize to avoid dep —
  implementer choice, prefer reusing `serde`); `/` → 200; else 404. Close conn
  after response. Cap request read at e.g. 8 KiB to bound memory.
- **Unit tests:** `parse_request_line_extracts_path` (pure parser fn);
  `render_prometheus_contains_cumulative` (feed a `Vec<StatsSnapshot>` ⇒ output
  contains `rfw_bytes_sent_total` with the value). Keep rendering in pure fns so
  they test without a socket.
- **e2e tests:** see T-E2E2 (Phase 4).
- **Done:** with `--metrics-addr 127.0.0.1:19090`, `curl /metrics` and `/stats`
  return correct live values; unset ⇒ no port opened.

---

### Phase 3 — Hot-path throughput & robustness  ⟶ **Opus design review on 3.2** (hot path, D7)

> Each sub-phase independently shippable; all preserve byte-exactness (I-1).

#### 3.1 TCP_NODELAY on both sockets
- **Model:** **Sonnet**
- **Files:** `src/forwarder.rs:64` (inbound, right after accept),
  `src/forwarder.rs:113-118` (outbound, after connect).
- **Change:** `inbound.set_nodelay(true)?;` and `outbound.set_nodelay(true)?;`
  (log+continue on error rather than dropping the connection).
- **Unit tests:** none (socket option); covered by e2e throughput not regressing.
- **e2e tests:** T-E2E1 still passes (byte-exact).
- **Done:** both sockets set; gates green.

#### 3.2 Sized bidirectional copy
- **Model:** Opus design review → **Sonnet** implements.
- **Files:** `src/forwarder.rs:133`.
- **Change:** `copy_bidirectional` → `copy_bidirectional_with_sizes(&mut counting,
  &mut outbound, buffer_bytes, buffer_bytes)` (`buffer_bytes` from config, default
  65536). Thread `buffer_bytes` through `proxy_connection`'s signature
  (`src/forwarder.rs:90-96`).
- **Unit tests:** none direct.
- **e2e tests:** T-E2E1 (byte-exact for 100 MiB) — proves no truncation at buffer
  boundaries.
- **Done:** large transfer byte-identical; gates green.

#### 3.3 Drop dead per-connection counters
- **Model:** Haiku
- **Files:** `src/stats.rs:115-150,153-168` (`CountingStream`).
- **Change:** Remove `bytes_read` / `bytes_written` fields + their `fetch_add`
  in `poll_read`/`poll_write`. Keep the `stats.add_sent/add_received` calls.
- **Unit tests:** rewritten 1.4 test still passes (counts via `ForwarderStats`).
- **e2e tests:** none.
- **Done:** one `fetch_add` per poll direction; no unused-field warnings.

#### 3.4 Arc<ForwarderConfig> per connection
- **Model:** Haiku
- **Files:** `src/forwarder.rs:66-70` (replace `cfg.clone()` with
  `Arc<ForwarderConfig>` clone), `run_forwarder` signature `src/forwarder.rs:17-21`,
  `proxy_connection` signature `src/forwarder.rs:90-96`, caller in
  `src/manager.rs:46-66`.
- **Change:** Pass `Arc<ForwarderConfig>`; clone the `Arc` (refcount bump) per
  connection instead of deep-cloning two `String`s.
- **Unit tests:** none.
- **e2e tests:** none.
- **Done:** no per-connection `String` alloc; gates green.

#### 3.5 RAII connection-count guard
- **Model:** **Sonnet**
- **Files:** `src/forwarder.rs:65,69-74` (+ small guard struct, in `forwarder.rs`
  or `stats.rs`).
- **Change:** `struct ConnGuard(Arc<ForwarderStats>)`; `new` calls
  `inc_connections`, `Drop` calls `dec_connections`. Construct at task start;
  remove the manual `s.dec_connections()` at `src/forwarder.rs:73`. Guarantees
  decrement on return, error, **and panic**.
- **Unit tests:** `conn_guard_decrements_on_drop` — inc via guard, drop, assert
  `active_connections == 0`; and a `std::panic::catch_unwind`/task-panic variant
  asserting no leak.
- **e2e tests:** reference scenario asserts `active==0` after disconnect.
- **Done:** active count returns to 0 even when the proxy task panics.

---

### Phase 4 — Tests & docs

> Locks in the acceptance scenario and documents the new interface.

#### 4.1 Stats / formatting unit tests
- **Model:** **Sonnet**
- **Files:** `src/stats.rs` tests module (`:179+`).
- **Change:** Add `format_bytes` edge cases (`0`→"0 B", `1023`→B, `1024`→"1.000
  KB", a TB-scale value); `compute_rate` boundary (dt→ small, Δ=0 ⇒ rate 0,
  decay toward 0 when traffic stops).
- **Unit tests:** the above (`T-FMT1`, `T-RATE1`).
- **e2e tests:** none.
- **Done:** edge cases covered; gates green.

#### 4.2 e2e integration harness  ⟶ **Opus review** (acceptance assertions)
- **Model:** Opus review (assertions) → **Sonnet** implements.
- **Files:** new `tests/e2e_forward.rs`.
- **Change:** In-process harness: spawn an echo `TcpListener` (remote), start one
  forwarder pointing at it (call `run_forwarder` or launch the built binary —
  prefer calling library fns; may require making `run_forwarder`/manager items
  `pub`), connect a client through the local addr.
  - **T-E2E1 (`bandwidth_is_lossless`):** client sends 100 MiB random bytes,
    reads them back, asserts byte-for-byte equality and length 104857600. Asserts
    `ForwarderStats` cumulative `bytes_sent == bytes_received == 104857600`.
  - **T-E2E2 (`metrics_endpoint_reports_live_cumulative`):** with `metrics_addr`
    set, mid/after transfer `GET /stats` and `/metrics` over a raw `TcpStream`;
    assert totals match and persist (re-query after a forced reporter tick → still
    nonzero, not reset); assert `rate_sent_bps > 0` was observed during transfer.
  - **T-E2E3 (`active_connections_no_leak`):** after client disconnects,
    `active_connections == 0`.
  Use small ports in a high range; bind to `127.0.0.1:0` where possible and read
  back the assigned port to avoid CI port clashes.
- **Unit tests:** n/a (this *is* the integration suite).
- **e2e tests:** T-E2E1, T-E2E2, T-E2E3 = the §1 reference scenario.
- **Done:** `cargo test --test e2e_forward` green locally and in CI (0.2).

#### 4.3 Docs
- **Model:** Haiku
- **Files:** `README.md`, `forwarders.yml` (example `settings` block), optionally
  `docs/`.
- **Change:** Document `--metrics-addr` / `--report-interval` /
  `--sample-interval` / `--buffer-bytes`, the `/metrics` + `/stats` endpoints
  (with sample output), and the new cumulative-vs-rate semantics. Note the
  removed reset behavior.
- **Unit tests:** n/a.
- **e2e tests:** n/a.
- **Done:** README shows a working `curl` example; flags table matches §4.

---

## 7. Invariants to preserve / add

- **I-1 (no data loss):** forwarded bytes are byte-identical end-to-end; buffer
  sizing (3.2) and counting changes (3.3) must not truncate or reorder. Proven by
  T-E2E1.
- **I-2 (cumulative monotonic):** `bytes_sent`/`bytes_received` never decrease and
  are never reset for the process lifetime (D1). Proven by 1.1 unit test +
  T-E2E2.
- **I-3 (no behavior change when metrics off):** `metrics_addr` unset ⇒ identical
  runtime behavior to today (no port, no extra task beyond the existing reporter +
  the always-on sampler). The sampler touches only atomics already present.
- **I-4 (panic isolation):** a panic in one connection task must not abort the
  process (no `panic="abort"`, D5) and must not leak `active_connections` (D9 /
  T-E2E3).
- **I-5 (hot path stays lock-free):** only `Relaxed` atomics on the per-poll path;
  no `Mutex`/`RwLock`/alloc added to `poll_read`/`poll_write`.

---

## 8. Risk register

| Risk | Mitigation |
|------|-----------|
| Hand-rolled HTTP parser mishandles a real scraper (chunked/keep-alive/partial reads). | GET-only, read-until-`\r\n\r\n` with an 8 KiB cap, connection-per-request, ignore body. If review deems it fragile → fallback to `axum` (D4). Covered by T-E2E2 over a raw socket. |
| `lto="fat"` + `codegen-units=1` slows release builds noticeably. | Acceptable for a release artifact; CI debug builds (0.2) are unaffected. Revert to `lto="thin"` if build time is a problem. |
| `TCP_NODELAY` hurts a niche bulk workload. | Neutral-to-positive in practice; if measured regression, make it a `--no-nodelay` opt-out (cheap follow-up). |
| Rate test flakiness from real timers. | `compute_rate` is a pure fn tested with injected `dt` (1.2); the sampler timing itself is not asserted on exact values. |
| Making `run_forwarder`/manager items `pub` for e2e widens the API. | Use `pub(crate)` + a `#[cfg(test)]`/integration shim, or drive the built binary via `assert_cmd`-style spawn if exposing internals is undesirable (decide at 4.2 review). |
| 100 MiB e2e test slow/heavy on CI. | Use 100 MiB only locally; parametrize to e.g. 8 MiB in CI via env, still crossing many 64 KiB buffers to prove I-1. Log the chosen size. |

---

## 9. Verification summary

- **Gates (every sub-phase):** `cargo fmt --check`,
  `cargo clippy --all-targets -- -D warnings`, `cargo test`.
- **Unit tests:** `src/stats.rs` (cumulative, rate math, format, conn guard),
  `src/config.rs` (settings defaults/override), `src/metrics.rs` (parser +
  renderer). Run: `cargo test`.
- **e2e:** `tests/e2e_forward.rs` — `cargo test --test e2e_forward`. Spawns echo
  server + forwarder + client in-process; no external services. Rebuild note: if
  4.2 drives the compiled binary instead of lib fns, `cargo build` before the
  test run (or use a build-dependency helper).
- **Acceptance:** the §1 reference scenario passes via **T-E2E1** (lossless +
  cumulative), **T-E2E2** (live continuous metrics, no reset), **T-E2E3** (no
  active-conn leak).

---

## 10. Model-assignment summary

| Phase | Sub-phases by model | Primary model | Opus review gate |
|-------|---------------------|---------------|------------------|
| 0 | 0.1 Haiku · 0.2 Haiku | Haiku | — |
| 1 | 1.1 Sonnet · 1.2 Sonnet · 1.3 Sonnet · 1.4 Sonnet | Sonnet | **1.1** (data-model) |
| 2 | 2.1 Haiku · 2.2 Sonnet | Sonnet | **2.2** (interface/dep) |
| 3 | 3.1 Sonnet · 3.2 Sonnet · 3.3 Haiku · 3.4 Haiku · 3.5 Sonnet | Sonnet/Haiku | **3.2** (hot path) |
| 4 | 4.1 Sonnet · 4.2 Sonnet · 4.3 Haiku | Sonnet | **4.2** (acceptance assertions) + final docs read |

> Rule of thumb: start Sonnet, drop to Haiku for mechanical/boilerplate
> sub-phases (config fields, profile, docs, dead-code removal), escalate to Opus
> only for the four review gates above. Print the model used per sub-task during
> implementation.
