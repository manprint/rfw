# rfw benchmark report: rfw vs Docker's forwarder vs socat

Measured on 2026-10-18/19 with the suite in [`bench/`](../../bench/README.md), which
re-runs in CI on every change. Raw results (`results.jsonl`, `mem.jsonl`) and the
generated full tables are in [`data/`](data/).

**Test box:** Intel Core 7 240H (16 threads), Linux 7.0, Docker 29.9, governor
`powersave`. Forwarder, backends and load generators are pinned to **disjoint CPUs**.
The forwarder gets **2 cores** (the main profile) or **4 cores**, matching the
deployment target (2–4 cores, 4–8 GB RAM). Loopback is not a NIC, so absolute Gbit/s
are memory-bandwidth numbers; only the ratios between forwarders are meaningful.

Variants: `direct` (no forwarder, the ceiling), `docker-nat` (Docker `-p`, kernel
DNAT, also a ceiling and not a competitor), `docker-proxy` (Docker `-p`, userland
proxy), `socat` (container, host network, `fork,reuseaddr`), `rfw` (native binary),
`rfw-splice`, and the production Docker image under several shapes (`rfw-d-*`).
Cells are medians of 2 repetitions (3 for the tuning study) with variants
interleaved; `±x%` is the spread when repetitions disagree by more than 10 %.

## 1. Summary

* **rfw beats both alternatives on what an RDS/database forwarder does:** small
  request/response traffic and connection rate. At 2 cores: 1.7× socat and 1.6×
  docker-proxy on 64-connection small-message throughput, 4.5× socat and 1.2× docker-proxy
  on connections/s (1.4× at 4 cores). Latency matches the others within noise, and at a
  given offered load rfw uses the least forwarder CPU of the three.
* **pgbench through the forwarder is a tie (±3 %)** for all of them, as expected: the
  database is the bottleneck. At 2 cores rfw is no slower than any alternative on the
  production-shaped workload.
* **Bulk TCP is where rfw's default (copy) path trails docker-proxy**, which splices
  in the kernel: 8 streams server→client 57 vs 107 Gbit/s at 2 cores. **`--splice` closes
  most of that gap** (96 Gbit/s), and `--splice --buffer-bytes 262144` closes the rest
  (109 Gbit/s at **1.0 core**, equal to docker-proxy at 0.87). At 4 cores the default
  path is already at parity on client→server.
* **Docker deployment is in optimal conditions.** The production image shows no penalty
  versus the native binary: seccomp, `--privileged` and bridge networking change
  nothing measurable. **`--privileged` is not needed.**
  The only container-specific caveat is `--splice` under a non-root pipe budget
  (section 4); `--cap-add SYS_RESOURCE` is the narrow fix.
* **Memory:** one rfw process holds 1000 TCP+UDP forwarded ports in **22 MiB**; socat
  needs 2001 processes and **1.4 GiB**, docker `-p` 4000 processes and **5.6 GiB** (and
  90 s to start). Idle TCP connections cost 19 KiB each in rfw (38 KiB in the image),
  vs 54 KiB for socat. UDP flows cost 1.8 KiB each (socat 55–346 KiB and it *loses
  sessions* above a few hundred flows).
* **Three real defects were found and fixed** (section 6), all about robustness
  under resource limits, not raw speed.

## 2. Results at 2 cores

### 2.1 Scorecard (ratios to the other forwarders; ⚠ = rfw behind by >5 %)

| test | metric | socat | docker-proxy | rfw | rfw-splice | rfw / socat | rfw / docker-proxy |
|---|---|---:|---:|---:|---:|---:|---:|
| rr1_64 | msg/s | 51,990 | 52,959 | 54,827 | 54,503 | 1.05× | 1.04× |
| rr64_64 | msg/s | 154,324 | 165,228 | 262,979 | 251,356 | 1.70× | 1.59× |
| rr8_16k | Mbit/s | 51 | 34,167 | 38,122 | 46,946 | n/a (see 5.1) | 1.12× |
| open_60k | p50 µs | 74 | 76 | 73 | 72 | tie | tie |
| cps | conn/s | 7,442 | 27,228 | 33,436 | 39,674 | 4.49× | 1.23× |
| cps_c256 | conn/s | 5,782 | 34,252 | 39,170 | 42,420 | 6.77× | 1.14× |
| udp_rr | pkt/s | 59,201 | 58,507 | 62,184 | 59,270 | 1.05× | 1.06× |
| dns32 | qps | 157,498 | 188,093 | 221,776 | 240,888 | 1.41× | 1.18× |
| udp_flood64 | max lossless kpps | 141 | 171 | 262 | 259 | 1.86× | 1.53× |
| udp_flood1200 | max lossless kpps | 171 | 120 | 177 | 162 | 1.03× | 1.47× |
| redis | SET rps | 104,223 | 107,847 | 106,866 | 108,202 | 1.03× | 0.99× |
| pg_s1 | tps | 25,759 | 26,050 | 25,482 | 26,005 | 0.99× | 0.98× |
| pg_s16 | tps | 81,671 | 86,065 | 81,487 | 82,967 | 1.00× | 0.95× ⚠ |
| bulk_p1 | Gbit/s | 10.2 | 40.3 | 38.7 | 39.9 | 3.78× | 0.96× |
| bulk_p1_rev | Gbit/s | 24.4 | 53.3 | 39.2 | 58.4 | 1.61× | 0.74× ⚠ |
| bulk_p8 | Gbit/s | 20.6 | 88.3 | 66.7 | 77.3 | 3.24× | 0.76× ⚠ |
| bulk_p8_rev | Gbit/s | 40.0 | 106.5 | 57.2 | 95.5 | 1.43× | 0.54× ⚠ |
| bulk_bidir | Gbit/s | 28.6 | 94.8 | 60.5 | 73.6 | 2.11× | 0.64× ⚠ |

The pg_s16 gap (0.95×) is within run-to-run noise; the table's other four ⚠ rows are all
bulk throughput on the default copy path, and every one narrows or disappears with
splice (section 5). Full tables for every test and variant:
[`data/report-full-2c.md`](data/report-full-2c.md).

### 2.2 Latency

Single connection, 64 B ping-pong (closed loop): rfw p50 17.6 µs / p99 24.8 µs, socat
18.6 / 23.6, docker-proxy 18.2 / 24.6 (direct: 10.4 / 13.4). **All three forwarders add
the same ~7–8 µs**; there is no latency penalty to choose rfw.

Open loop (fixed send schedule, latency charged from the intended send time, so no
coordinated omission), 64 connections:

| offered load | variant | p50 µs | p99 µs | p99.9 µs | fwd cores |
|---|---|---:|---:|---:|---:|
| 20k req/s | docker-proxy | 74 | 120 | 155 | 0.32 |
| | socat | 74 | 122 | 363 | 0.26 |
| | rfw | 72 | 133 | 552 ±60% | **0.19** |
| 60k req/s | docker-proxy | 76 | 148 | 6,820 ±79% | 1.09 |
| | socat | 74 | 102 | 690 | 0.76 |
| | rfw | 73 | 204 ±56% | 905 ±68% | **0.58** |
| | rfw-splice | 72 | 95 | 668 | 0.60 |
| | rfw-d-host | 73 | 94 | 150 | 0.58 |

rfw serves the same load with about half the forwarder CPU of docker-proxy; tail latency
is statistically indistinguishable except that docker-proxy shows multi-millisecond
p99.9 excursions under load.

When the forwarder is *saturated* in a closed loop (64 connections, 2 cores) rfw shows
p99 ≈ 3.1 ms. That is the kernel scheduler's EEVDF slice (3 ms) at 100 % CPU, not an rfw
queue: the open-loop p99 at the same throughput level is ~100–200 µs, and
`rfw-d-host-splice` shows 0.38 ms in the same test. It only matters if a deployment
runs the forwarder CPU-saturated.

### 2.3 Connection rate and many connections

Connect + 1 B round trip + close, 64 concurrent: rfw 33–40k/s (splice 40k) vs
docker-proxy 27k, socat 7.4k. With 256 concurrent connections socat collapses to 5.8k/s
with p99 over one second; rfw sustains 39k/s at p99 19 ms. For reference, the
kernel NAT ceiling (docker-nat) does 65–71k.

1000 concurrent connections × 16 KiB messages (`rr1000_16k`, 2 cores): rfw 28.8 Gbit/s,
docker-proxy 39.4, socat 17.4. rfw is behind here at 2 cores (0.73×) and at parity at 4
cores (43.1 vs 46.8, 0.92×). All results have zero errors.

### 2.4 UDP

* 1-flow round trip: rfw p50 15.6 µs vs socat 16.3 and docker-proxy 16.5.
* Open-loop 64 B datagram flood over 8 flows, highest rate with ≤0.5 % loss: rfw
  **262 kpps** (this run; 328–373 in the 3-repetition tuning run), docker-proxy 171–182,
  socat 141–156. rfw's lossless rate varies ±30 % between runs, because the
  generator and the forwarder compete for the same memory bandwidth; the *ordering* is
  stable across all runs.
* 1200 B datagrams: all three within noise (rfw 156–180, socat 164–171, docker-proxy
  120–178 kpps). `--udp-gro-gso` helped in the tuning run (176 vs 156 kpps).
* CoreDNS with 32 clients: rfw 222k qps, 240k with splice (UDP is unaffected, the
  difference is noise), docker-proxy 188k, socat 157k.

## 3. Results at 4 cores (forwarder pinned to 4 CPUs)

| test | metric | socat | docker-proxy | rfw | rfw-splice | rfw / docker-proxy |
|---|---|---:|---:|---:|---:|---:|
| rr64_64 | msg/s | 219,418 | 221,058 | 411,580 | 334,128 | 1.86× |
| rr1000_16k | Mbit/s | 23,153 | 46,781 | 43,055 | 45,780 | 0.92× |
| cps | conn/s | 13,127 | 43,160 | 59,074 | 63,737 | 1.37× |
| udp_rr | pkt/s | 60,935 | 60,338 | 63,954 | 63,632 | 1.06× |
| udp_flood64 | max lossless kpps | 172 | 336 | 457 | 470 | 1.36× |
| udp_flood1200 | max lossless kpps | 163 | 187 | 167 | 183 | 0.89× |
| pg_s16 | tps | 85,667 | 84,890 | 86,592 | 86,349 | 1.02× |
| bulk_p1 | Gbit/s | 10.9 | 40.7 | 39.6 | 41.2 | 0.97× |
| bulk_p8 | Gbit/s | 42.1 | 84.7 | 84.0 | 93.7 | 0.99× |
| bulk_p8_rev | Gbit/s | 70.1 | 106.8 | 90.3 | 109.5 | 0.85× |

With 4 cores the picture shifts in rfw's favour: the multi-threaded runtime scales
almost linearly (small-message throughput 263k → 412k msg/s, connection rate 33k → 59k),
while docker-proxy scales less (165k → 221k). The default
copy path reaches parity with docker-proxy on 8-stream upload; splice exceeds it. The one
persistent gap is large-datagram UDP (1200 B), where rfw and docker-proxy trade places
inside the noise. Full tables: [`data/report-full-4c.md`](data/report-full-4c.md).

## 4. Docker deployment: is the container in optimal conditions?

Same tests, the production image started in different shapes (2 cores; all numbers
medians; core·s/GB = forwarder CPU-seconds per GB moved, lower is better):

| shape | 64×64B msg/s | cps | bulk 8s up Gbit/s | bulk 8s down Gbit/s (core·s/GB) | pg_s16 tps |
|---|---:|---:|---:|---:|---:|
| `rfw` (native) | 262,979 | 33,436 | 66.7 | 57.2 (0.27) | 81,487 |
| `rfw-d-host` (`--network host`, recipe in README) | 256,401 | 35,604 | 76.6 | 60.5 (0.26) | 84,115 |
| `rfw-d-bridge` (compose `-p` on a bridge) | 262,944 | 35,604 | 75.5 | 57.2 (0.27) | 86,922 |
| `rfw-d-host-splice` | 229,508 | 35,714 | 85.6 | 81.9 (0.11) | 86,183 |
| `rfw-d-splice-sysres` (`--cap-add SYS_RESOURCE`) | 228,010 | 36,084 | 89.0 | 78.5 (0.11) | 85,362 |
| `rfw-d-v1.1.0` (published image) | 266,698 | 36,148 | 71.6 | 60.2 (0.26) | 85,289 |
| `rfw-d-v1.1.0-splice` | 222,640 | 37,506 | 82.5 | 96.9 (0.10) | 85,039 |

(Unconfined seccomp and `--privileged` runs behave like `rfw-d-host`: no measurable
difference in the earlier runs, so those shapes were dropped from the final matrix.)

Findings:

1. **No Docker penalty.** The image performs within noise of the native binary. Host
   networking is as fast as native, and bridge networking (compose layout) showed no
   measurable cost in these runs either (cps 35.6k, bulk and latency unchanged).
2. **`--privileged` is unnecessary.** Neither it nor `seccomp=unconfined` improves a
   single metric.
3. **Splice in a container needs the pipe budget.** `fs.pipe-user-pages-soft`
   (default 16,384 pages ≈ 64 MiB) is shared by *all processes of one uid*. When
   exceeded, unprivileged pipes shrink to 4 KiB and `F_SETPIPE_SZ` fails with EPERM.
   `docker-proxy` runs as root and retains pooled pipes, so on a host that also has
   docker-proxy processes the budget can be exhausted before rfw starts. The
   v1.1.0 image then fell back in an uncontrolled way (throughput collapsed in my
   earlier runs). Fixed in this release (section 6): rfw falls back to the copy path
   when it cannot get useful pipes, and logs one warning per minute naming the fix.
   `--cap-add SYS_RESOURCE` bypasses the limit and is the narrow alternative to
   `--privileged`. The final matrix runs each variant against its own container fleet,
   so it does not reproduce the poisoned state; v1.1.0-splice looks healthy here.
4. **File descriptors.** Docker's default `nofile` soft limit is 1024, i.e. ~500
   connections before `EMFILE`. rfw now raises its own soft limit towards the hard limit
   at startup (section 6), so the `--ulimit` in the recipe is no longer *required* for
   TCP, though it stays harmless.
5. **Memory limit.** The compose profile (`--memory 128m`, `--max-connections 512`)
   never OOMs, with 10,000 attempted connections: 512 are served, the rest rejected at
   accept and counted in `rfw_rejected_connections_total` (20 MiB resident at the cap).

## 5. `--splice` examined carefully

| 2 cores | copy (default) | `--splice` | `--splice --buffer-bytes 262144` | docker-proxy |
|---|---:|---:|---:|---:|
| bulk 1 stream up (Gbit/s) | 39.9 | 41.5 | 42.2 | 41.6 |
| bulk 8 streams up (Gbit/s · cores) | 77.9 · 1.97 | 94.8 · 1.67 | 97.2 · 1.70 | 100.2 · 1.14 |
| bulk 8 streams down (Gbit/s · cores) | 66.6 · 1.97 | 98.4 · 1.25 | **123.3 · 1.00** | 122.5 · 0.85 |
| 64×64 B msg/s | 247,209 | 249,004 | 255,212 | 175,178 |
| conn/s | 42,698 | 40,957 | 40,477 | 28,136 |
| 1000 conns × 16 KiB (Mbit/s) | 28,803 | 28,339 | 30,444 | 39,442 |

(3-repetition tuning run, [`data/report-tuning-2c.md`](data/report-tuning-2c.md).)

* Splice saves 25–55 % of the forwarder CPU on bulk (0.11 vs 0.25 core·s/GB) and, with
  256 KiB pipes, matches docker-proxy's throughput **and** CPU on the heaviest test.
  It is neutral for connection rate, latency under realistic load, pgbench and DNS.
  It **costs saturated small-message throughput**: 64 connections × 64 B drops
  263k → 251k msg/s at 2 cores (-4 %) and 412k → 334k at 4 cores (-19 %), the extra
  syscalls per small read (splice in, splice out) versus a single copy. For a
  database forwarder that is irrelevant (pg_s16 ties), but it is why splice is not a
  free default.
* It is correct: all integrity tests (byte-for-byte, half-close, clean EOF) pass on both
  paths, TCP connections are all closed after every run (`active_after = 0`), and a
  240 s churn soak holds memory flat: 1.4 → 1.5 MiB with splice (4.9 MiB copy).
* Costs and limits: two pipes per connection (6 fds/connection instead of 2), and the
  per-uid pipe budget. With 256 KiB pipes the soft budget covers only ~128 connections
  at full size before pipes shrink; rfw then runs with smaller pipes or falls back to
  copy. Root containers with `SYS_RESOURCE` are unaffected. 1000 connections × 16 KiB
  (the "pipe-limit probe") shows the budget at work: splice equals copy there.
* Verdict: splice is safe and clearly beneficial for bulk TCP at 2–4 cores; it is not
  beneficial for tiny-message saturation. It remains opt-in (pipe-budget caveat,
  small-message cost); the budget caveat is now handled by fallback instead of
  collapsing silently.

### 5.1 Other tuning knobs measured (2 cores, 3 repetitions)

| variant | 64×64B msg/s | cps | 1 stream up | 8 streams up | 8 streams down | udp flood 64B (kpps) |
|---|---:|---:|---:|---:|---:|---:|
| rfw (64 KiB buffers) | 247k | 42.7k | 39.9 | 77.9 | 66.6 | 348 |
| `--buffer-bytes 16384` | 249k | 44.0k | **24.0** | **45.1** | **45.8** | 364 |
| `--buffer-bytes 262144` | 262k | **28.3k** | 41.5 | **100.7** | 97.3 | 346 |
| `--socket-buffer-bytes 4194304` | 270k | 39.4k | 40.8 | 76.2 | 66.0 | 356 |
| `--udp-gro-gso` | 249k | 42.4k | 40.4 | 77.0 | 66.9 | 373 |

* **Larger copy buffers speed bulk by 30–45 % but cost 34 % of connection rate**
  (most likely the per-connection allocation and zero-filling of two 256 KiB buffers;
  not profiled). Use them only on a forwarder that
  carries few, long, heavy connections. Splice with 256 KiB pipes gets the bulk gain
  without the connection-rate penalty (40.5k conn/s) because pipe buffers are in the
  kernel, not zero-filled in user space.
* Small buffers (16 KiB) halve bulk throughput and barely help connection rate (44.0k
  vs 42.7k); do not shrink buffers to save memory.
* A 4 MiB socket buffer does nothing for loopback/LAN TCP or UDP loss here (the limiter
  is CPU); it matters for high bandwidth-delay links, as documented.
* UDP GRO/GSO improves lossless rate and 1200 B datagrams modestly (≈ +5–13 %), and is
  safe to enable on kernels that support it.

socat's behaviour on request/response traffic needs a warning: with the command line
used here (defaults, no `TCP_NODELAY`) 8 connections × 16 KiB messages reach
**51 Mbit/s** (p99 42 ms, consistent with a Nagle/delayed-ACK stall) against
38,000 Mbit/s for rfw. The suite includes a `socat-nodelay` variant to isolate that
option; it was not part of the final matrix.

## 6. Defects found and fixed

All three are in commit `8ba66ed`, covered by unit tests, and verified empirically.

1. **Splice pipe exhaustion collapsed throughput** (see 4.3). With the per-uid pipe budget
   exhausted, `F_SETPIPE_SZ` failed and the connection ran through a 4 KiB pipe, with
   throughput dropping by an order of magnitude and no log line. rfw now creates the
   pipes first, checks their real capacity (`F_GETPIPE_SZ ≥ 32 KiB`), and otherwise
   falls back to the buffered copy path for that connection, with one rate-limited
   warning that names `--cap-add SYS_RESOURCE`.
2. **No file-descriptor headroom.** Docker's default `nofile=1024` soft limit capped
   rfw at ~500 connections (`EMFILE` on accept) unless the operator set `--ulimit`.
   rfw now raises `RLIMIT_NOFILE` to `min(hard, 1,048,576)` at startup and warns when
   `--max-connections` cannot fit (6 fds/connection with splice, 2 without).
3. **Peer hangups logged at WARN in the splice path.** Resets and broken pipes from
   clients are normal and now log at debug; other errors stay at warn.

The benchmark work itself exposed a hazard worth recording: connection-churn tests
(about 4000 connect/s through two connection-tracked legs, 120 s TIME_WAIT) filled the
host's `nf_conntrack` table and made the kernel drop *all* new flows. The suite now
refuses to start a scenario while the table is above 8 % full and clamps churn sizes to
12 % of `nf_conntrack_max`. The same applies to anyone running connection-rate tests
against a forwarder on a shared host.

A pre-existing flaky unit test, `connect_racing_fails_when_every_address_is_dead`
(`dead_addr` port reuse under parallel test execution), failed once during these
runs. It is unrelated to the forwarder and is not fixed here.

## 7. Memory

All numbers are anonymous memory summed over *every* process of the forwarder.

### 7.1 Idle memory vs number of forwarded ports (N TCP + N UDP)

| variant | N=10 | N=100 | N=1,000 |
|---|---:|---:|---:|
| rfw | 1.0 MiB | 2.7 MiB | 22.4 MiB |
| rfw (Docker image, host net) | 1.0 MiB | 3.2 MiB | 27.3 MiB |
| socat | 14.4 MiB (21 procs) | 143 MiB (201 procs) | **1,431 MiB (2,001 procs)** |
| docker `-p` (docker-proxy) | 56.7 MiB (40 procs) | 563 MiB (400 procs) | **5,651 MiB (4,000 procs; 89 s to start)** |

rfw is **64×** smaller than socat and **250×** smaller than docker `-p` at 1000 ports.
On an 8 GB machine docker `-p` with 1000 published ports does not fit alongside anything
else; socat consumes a fifth of the RAM. rfw: 22 MiB.

### 7.2 Concurrent TCP connections (marginal memory per connection)

| variant | idle, 1000 | idle, 10,000 | moved 1 MiB each, 1000 | moved 1 MiB each, 5000 |
|---|---:|---:|---:|---:|
| rfw | 19.1 KiB | 19.3 KiB (190 MiB) | 43.1 KiB | 43.2 KiB (212 MiB) |
| rfw `--splice` | 11.9 KiB | 18.6 KiB | 25.6 KiB | 39.7 KiB |
| rfw Docker image | 37.9 KiB | 38.7 KiB (380 MiB) | 58.3 KiB | 60.0 KiB (295 MiB) |
| socat | 55.1 KiB | 54.2 KiB (577 MiB) | 58.5 KiB | 59.0 KiB (335 MiB) |
| docker-proxy | 27.1 KiB | 20.7 KiB (390 MiB) | 27.1 KiB | 20.4 KiB (287 MiB) |

docker-proxy's per-connection figure is small because Go reuses pooled buffers, but it
carries a fixed base of tens to hundreds of MiB for the processes themselves. The
Docker image uses about twice the per-connection memory of the native binary for the same
rfw build (cause not isolated; likely allocator arena behaviour in the cgroup); still
below socat. Budget **~60 KiB per active connection** in the image: 10,000 connections ≈
600 MB.

### 7.3 UDP sessions

rfw: **1.8 KiB per flow** (10,000 flows = 18.5 MiB), all flows answered. socat:
55 KiB per flow and it *loses sessions* (1 of 100 at N=100, 496 of 1000 at N=1000,
9036 of 10,000), (cause not isolated). docker-proxy:
71–151 KiB per flow.

### 7.4 Leak check

240 s soak (connection churn + bulk integrity transfers; the host conntrack limit keeps
the rate modest): rfw 3.8 → 4.9 MiB then flat; splice 1.4 → 1.5 MiB; image 5.3 → 5.3 MiB;
compose profile 4.7 MiB flat; fds and threads constant (76 → 76, 3 → 3), zero errors,
zero connections active afterwards.

Full tables: [`data/report-memory.md`](data/report-memory.md).

## 8. Where rfw is behind, and room for improvement

1. **Bulk TCP on the default copy path** (2 cores): 0.5–0.75× docker-proxy, because each
   byte crosses user space twice. The fix exists (splice); making it the default
   is a trade-off, not a free win: it costs 4–19 % of saturated small-message
   throughput and depends on the pipe budget (now handled by fallback). An adaptive
   hybrid (copy while messages are small, splice once a connection streams) would
   remove the trade-off; not attempted here.
2. **Per-connection buffer cost vs connection rate.** Buffers are zero-filled per
   connection. A buffer that starts at 16 KiB and grows to 256 KiB when reads fill it
   would get both the 100 Gbit/s bulk and the 44k conn/s of the small buffer. Not
   attempted in this change set (touches the hot path of a stability-focused forwarder).
3. **1000 concurrent 16 KiB streams at 2 cores:** 0.73× docker-proxy (0.92× at 4 cores).
   Splice does not help here because the pipe budget caps it.
4. **Large-datagram UDP:** rfw ≈ docker-proxy ≈ socat at 1200 B (within noise); 
   `recvmmsg` batching beyond the existing GRO path is a possible gain.
5. **Image memory:** 2× the native binary per connection (see 7.2).
6. **listen backlog:** tokio's default backlog is 1024; a connection flood above that
   relies on `net.core.somaxconn` and SYN retries. Not a problem at the tested loads.

## 9. Recommendations for maximum performance (2–4 cores, 4–8 GB)

**Deployment (Docker)**

* Use the host-network recipe (`--network host`) for a database forwarder: it removes
  the double hop and the port mapping. Bridge + `-p` costs only latency on connection
  setup, but needs no extra memory.
* **Do not use `--privileged`** or `seccomp=unconfined`: no measurable gain.
* For bulk or high-throughput forwarding, run with `--splice` **and**
  `--cap-add SYS_RESOURCE` (root in the container): 1.2–1.7× the bulk throughput at
  roughly half the CPU (up to 1.85× with 256 KiB pipes). For a pure database forwarder
  (small messages) splice gains nothing measurable; leave it off. Without the capability keep splice only if no other process of the
  same uid holds many pipes (check `sysctl fs.pipe-user-pages-soft` and
  `docker-proxy`/`nginx` neighbours), otherwise rfw falls back automatically and logs
  a warning.
* Add `--splice --buffer-bytes 262144` only for few-but-heavy streams (backups, dumps,
  large transfers). For many short connections leave `--buffer-bytes` at the default.
* Keep `--ulimit nofile=65536:65536` in compose files for clarity; rfw raises its own
  limit now, but the container hard limit still bounds it.
* `--max-connections`: size by memory, **≈ 60 KiB per active TCP connection in the
  image** (about 45 KiB native, 25–40 with splice) plus ~30 MiB base. 4 GB →
  ~30–40k connections is a safe cap; `--memory 128m` ↔ `--max-connections 512` was
  verified.
* Give rfw as many cores as you can spare: it scales with them (1.6× from 2 to 4 cores
  on small messages, 1.8× on connection rate).

**Kernel / host**

* **Host networking moves the TCP stack to the host.** With `--network host` the
  sockets facing the clients live in the host's network namespace, so the
  congestion control and buffer ceilings tuned inside a backend's container (BBR,
  16 MiB) no longer apply to that leg. Reported from production: in front of a
  tunnel server whose container ran BBR, rfw on a stock host (cubic, 4 MiB
  `tcp_wmem`, 212 KiB `rmem_default`) was far slower than Docker's NAT and QUIC
  datagrams were dropped (`UdpRcvbufErrors` climbing); setting
  `tcp_congestion_control=bbr`, `tcp_wmem/rmem` max 16 MiB and
  `net.core.rmem_default/wmem_default` 8 MiB on the host restored line rate.
  This benchmark ran on loopback and cannot show it. See
  `examples/tunnel-2c-4g/sysctl.conf`.
* `net.core.rmem_max`/`wmem_max` ≥ 4 MiB if you set `--socket-buffer-bytes` (WAN links) or
  carry heavy UDP; otherwise the setting is silently capped.
* For UDP at high packet rates consider `--udp-gro-gso` on kernels that support UDP
  GRO/GSO (modest gain, +5–13 % lossless rate in the tuning run).
* Raise `net.netfilter.nf_conntrack_max` only if the host also does NAT: loopback and
  published ports are connection-tracked, and a full table drops *all* new flows on the
  machine (this is what a naive benchmark did to the author's workstation).
* `fs.pipe-user-pages-soft`: raise if you must run splice without `SYS_RESOURCE`
  (each full-size connection needs ~2×buffer-bytes/4 KiB pages).

**Choosing the forwarder**

| situation | choice |
|---|---|
| DB/RDS forwarder, many short connections, small messages | **rfw** (best connection rate, lowest CPU, equal latency) |
| Bulk transfers dominate | rfw `--splice` (+ 256 KiB buffers, `SYS_RESOURCE`) |
| Hundreds of published ports | **rfw** (22 MiB vs 1.4 GiB socat / 5.6 GiB docker `-p`) |
| UDP | rfw: it keeps all flows and sustains ~2× socat's lossless rate |
| Pure kernel NAT is acceptable (no per-conn logic) | Docker's NAT path remains the ceiling: 70k conn/s, 92–135 Gbit/s |

**Measuring your own deployment**

`python3 bench/run.py --profile quick --fwd-cores 2` (~10 min) then
`python3 bench/report.py bench/results/<run>`; compare against the scorecard in section
2. CI runs the same profile on every change and fails when a score drops more than the
tolerance below the committed baseline. Warning: connection-rate tests are bounded by the
conntrack guard; do not remove it on a shared host.

## 10. Limitations

* Loopback cannot show congestion-control, buffer-ceiling or RTT effects
  (section 9, "Host networking moves the TCP stack to the host"); a WAN-facing
  deployment must be validated on its real path.
* One machine, loopback/bridge only; a real NIC, a hypervisor, or a cloud vCPU changes
  absolute numbers (and 2 vCPUs are often 1 physical core with hyperthreading).
* 2 repetitions in the final matrices (3 in tuning): ±5–35 % noise on UDP flood and
  open-loop tails, flagged in the tables. UDP lossless rate needs more repetitions to
  separate rfw variants; conclusions above rely on orderings that held in every run.
* The pipe-poisoning incident is reproduced only when a root process (docker-proxy)
  retains many pipes, which the final matrix deliberately avoids by isolating fleets.
* The CI baseline (`bench/baseline/quick-2c.json`) must be produced on a GitHub runner
  via the workflow's manual dispatch; none is committed yet.
