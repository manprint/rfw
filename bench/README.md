# rfw benchmark suite

Compares **rfw** against the forwarders it replaces — `socat` and Docker's own
port publishing (`docker-proxy` userland, and the kernel DNAT/conntrack path) —
on latency, throughput, connection rate, CPU cost, memory and stability, for
TCP and UDP, on the deployment shapes that matter (native binary and the Docker
image). The results are reproducible, persistent and run in CI
(`.github/workflows/bench.yml`), so every change has a comparison.

Latest analysis: [`docs/benchmarks/BENCHMARK_REPORT.md`](../docs/benchmarks/BENCHMARK_REPORT.md).

## Requirements

Linux, Docker (user in the `docker` group; no root needed), Python 3.8+, Rust
(stable) with the `x86_64-unknown-linux-musl` target, `taskset` (util-linux).
Images are pulled on first use: `alpine`, `alpine/socat`, `networkstatic/iperf3`,
`redis:alpine`, `postgres:17-alpine`, `coredns/coredns`.

```sh
rustup target add x86_64-unknown-linux-musl
cargo build --release                      # the rfw under test (target/release/rfw)
python3 bench/run.py --profile quick       # ~10 min: what CI runs
python3 bench/report.py bench/results/<run>            # Markdown tables
python3 bench/memtest.py --out bench/results/mem1      # memory / scaling / soak
python3 bench/memreport.py bench/results/mem1
```

## What is measured

| Layer | Tool | Notes |
|---|---|---|
| Latency (TCP/UDP), request rate, connection rate, integrity, many-flow holds | `rfwbench` (this repo, `bench/rfwbench`, std + libc only) | closed loop **and open loop** (fixed send schedule, latency charged from the *intended* send time: no coordinated omission) |
| Bulk TCP / UDP | `iperf3` (container, host network) | 1 and 8 streams, both directions, bidirectional, UDP rates |
| Realistic backends | `redis-benchmark`, `pgbench` against redis / postgres, CoreDNS queries | postgres is the production (RDS forwarder) shape |
| CPU cost | cgroup `cpu.stat` (containers) / `/proc/<pid>/stat` (native, docker-proxy) | "fwd cores" = forwarder CPU-seconds per wall second; kernel DNAT cannot be attributed |
| Memory | `Pss_Anon` (native, docker-proxy via a privileged `--pid=host` helper) / cgroup `memory.stat` anon (containers) | anonymous memory summed over *all* processes of a forwarder (socat forks, docker-proxy per port) |

Backends are containers on a private bridge (`rfwb-net`); every forwarder reaches
them through the same bridge. Forwarders and backends are pinned to disjoint
CPUs (`--cpus-fwd/--cpus-backend/--cpus-client`; defaults avoid SMT siblings), and
the forwarder budget is `--fwd-cores 2|4` to mirror small deployment targets.
Docker's own `docker-proxy` processes are pinned to the same CPUs
(`rfwbench pin`, run from a privileged helper), so it gets the same budget.

## Variants

`direct` (no forwarder: the ceiling), `docker-nat` (docker `-p`, kernel path —
a ceiling, not a competitor), `docker-proxy` (docker `-p`, userland, via
127.0.0.1), `socat` / `socat-nodelay` (container, host network, as deployed),
`rfw`, `rfw-splice`, `rfw-gro`, and the Docker image under the shapes people use:
`rfw-d-host` (production recipe), `-splice`, `-unconfined` (seccomp off),
`-privileged`, `-splice-sysres` (`--cap-add SYS_RESOURCE`), `-bridge` (compose
layout: `-p` on a bridge, i.e. a double hop), `-default-ulimit` (no `--ulimit`),
`-v1.1.0` (the published image). `python3 bench/run.py --list` shows all
variants and tests.

## Method notes (read before trusting a number)

* **Interleaving.** Variants run in rotating, alternating order across
  repetitions, so frequency/thermal drift hits all of them alike. Cells are
  medians; `±x%` appears when repetitions differ by more than 10 %. A single
  repetition is only a smoke test.
* **Loopback is not a NIC.** Everything runs on one host, so absolute Gbit/s are
  memory-bandwidth/CPU numbers, not link speeds. Ratios between forwarders on
  the same box are the meaningful output.
* **Connection-rate tests** are fixed-count (not time-boxed) and spread over 32
  ports so TIME_WAIT and conntrack stay bounded.
* **Host safety.** Everything started carries the label `rfwbench=1`; cleanup is
  by label only. The harness never prunes, never changes sysctls, and aborts if
  its port ranges are in use.
* The CPU governor matters: it is recorded in `meta.json`. On laptops
  (`powersave`) expect more noise.

## CI

`bench.yml` builds the production image and `rfwbench`, runs the `quick`
profile (forwarder pinned to 2 cores) plus a memory/scaling subset, uploads
everything as an artifact and writes the tables into the job summary.

* **Hard failures**: byte-for-byte integrity, TCP connections still active after
  the run, a forwarder that does not start or errors in any test.
* **Performance gate**: each score is `variant / direct` measured in the *same*
  job; the job fails (on `main`/schedule) if a score drops more than the
  tolerance below `bench/baseline/quick-2c.json`. On pull requests it only warns.
* **Updating the baseline**: run the workflow manually, download the
  `bench-results` artifact, commit its `baseline.json` as
  `bench/baseline/quick-2c.json`. Do this on a GitHub runner, never from a
  workstation (different CPU = different ratios).

`bench/rfwbench` has its own fmt/clippy/test steps (it is deliberately not part
of the rfw Cargo workspace).
