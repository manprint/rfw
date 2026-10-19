# rfw benchmark report

- host: Intel(R) Core(TM) 7 240H (16 threads), kernel 7.0.0-38-generic, governor `powersave`
- Docker version 29.9.1, build dfc520d; rfw rfw 1.1.0 (git 2a9be9f)
- forwarder pinned to CPUs `0,2`, backends `8,10`, load generators `12,13,14,15`
- profile `full`, 3 repetition(s), variants interleaved; cells are medians (±spread when >10 %)

## Forwarder pinned to 2 core(s)

### Scorecard

| test | primary metric | socat | docker-proxy | rfw | rfw-splice | rfw vs socat | rfw vs docker-proxy |
|---|---|---:|---:|---:|---:|---:|---:|
| rr64_64 | msg/s | 160,190 | 175,178 | 247,209 | 249,004 | 1.54× | 1.41× |
| rr1000_16k | Mbit/s | 17,449 | 39,442 | 28,803 | 28,339 | 1.65× | 0.73× ⚠ |
| cps | conn/s | 7,985 | 28,136 | 42,698 | 40,957 | 5.35× | 1.52× |
| udp_rr | pkt/s | 59,588 | 58,899 | 61,972 | 62,125 | 1.04× | 1.05× |
| udp_flood64 | max lossless kpps | 156 | 182 | 348 | 364 | 2.24× | 1.91× |
| udp_flood1200 | max lossless kpps | 164 | 178 | 156 | 159 | 0.95× | 0.88× ⚠ |
| bulk_p1 | Gbit/s | 10.6 | 41.6 | 39.9 | 41.5 | 3.77× | 0.96× |
| bulk_p8 | Gbit/s | 21.1 | 100.2 | 77.9 | 94.8 | 3.69× | 0.78× ⚠ |
| bulk_p8_rev | Gbit/s | 42.4 | 122.5 | 66.6 | 98.4 | 1.57× | 0.54× ⚠ |

### Details

#### TCP small-message throughput — 64 connections × 64 B (closed loop)

| variant | msg/s | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 175,178 ±9% | 119 ±46% | 1884 ±6% | 1.97 | 1.09× |
| socat | 160,190 | 395 | 561 ±7% | 2.00 |  |
| rfw | 247,209 ±7% | 128 ±8% | 3129 | 1.84 | 1.54× |
| rfw-splice | 249,004 | 211 ±30% | 3113 ±44% | 1.94 | 1.55× |
| rfw-gro | 248,760 | 126 | 3178 | 1.92 | 1.55× |
| rfw-buf16k | 248,786 | 114 ±6% | 3178 | 1.91 | 1.55× |
| rfw-buf256k | 261,673 | 144 ±23% | 3129 | 1.90 | 1.63× |
| rfw-sock4m | 269,669 ±7% | 120 ±5% | 3146 | 1.91 | 1.68× |
| rfw-splice-buf256k | 255,212 | 122 ±10% | 3129 | 1.93 | 1.59× |
| rfw-gro-sock4m | 265,464 | 109 ±22% | 3178 | 1.91 | 1.66× |

#### TCP 1000 concurrent connections × 16 KiB messages (splice pipe-limit probe)

| variant | Mbit/s | p99 µs | errors | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 39,442 ±7% | 81265 ±6% | 0 | 1.85 | 2.26× |
| socat | 17,449 ±11% | 44827 ±10% | 0 | 1.82 |  |
| rfw | 28,803 | 23200 ±65% | 0 | 1.82 | 1.65× |
| rfw-splice | 28,339 ±17% | 52691 ±47% | 0 | 1.67 ±12% | 1.62× |
| rfw-gro | 29,303 ±12% | 44302 ±50% | 0 | 1.76 ±10% | 1.68× |
| rfw-buf16k | 27,993 | 38535 ±60% | 0 | 1.79 | 1.60× |
| rfw-buf256k | 30,763 | 24510 ±8% | 0 | 1.87 | 1.76× |
| rfw-sock4m | 31,353 | 24904 ±9% | 0 | 1.93 | 1.80× |
| rfw-splice-buf256k | 30,444 | 28574 ±17% | 0 | 1.85 ±6% | 1.74× |
| rfw-gro-sock4m | 29,750 | 26214 ±10% | 0 | 1.83 | 1.70× |

#### TCP connection rate — connect + 1 B round trip + close, 64 concurrent

| variant | conn/s | txn p50 µs | txn p99 µs | errors | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 28,136 ±11% | 1728 ±9% | 9962 ±45% | 0 | 3.52× |
| socat | 7,985 | 8028 | 13828 ±7% | 0 |  |
| rfw | 42,698 ±6% | 1483 ±6% | 2163 ±7% | 0 | 5.35× |
| rfw-splice | 40,957 | 1548 ±5% | 2441 ±5% | 0 | 5.13× |
| rfw-gro | 42,397 ±8% | 1491 ±9% | 2277 ±11% | 0 | 5.31× |
| rfw-buf16k | 44,047 | 1442 | 2048 | 0 | 5.52× |
| rfw-buf256k | 28,346 ±6% | 2245 ±6% | 3195 ±6% | 0 | 3.55× |
| rfw-sock4m | 39,426 | 1581 | 2277 ±22% | 0 | 4.94× |
| rfw-splice-buf256k | 40,477 ±20% | 1532 ±28% | 2769 ±52% | 0 | 5.07× |
| rfw-gro-sock4m | 41,754 | 1524 | 2130 | 0 | 5.23× |

#### UDP round-trip latency — 1 flow, 64 B

| variant | pkt/s | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 58,899 | 16.5 | 20.5 ±11% | 0.04 | 0.99× |
| socat | 59,588 | 16.2 | 19.8 | 0.04 |  |
| rfw | 61,972 | 15.6 | 19.5 ±10% | 0.04 | 1.04× |
| rfw-splice | 62,125 | 15.6 | 19.5 | 0.04 | 1.04× |
| rfw-gro | 61,791 | 15.6 | 19.3 | 0.04 | 1.04× |
| rfw-buf16k | 62,467 | 15.5 | 19.3 | 0.04 | 1.05× |
| rfw-buf256k | 62,746 | 15.4 | 19.1 ±11% | 0.04 | 1.05× |
| rfw-sock4m | 62,893 | 15.4 | 19.1 ±8% | 0.04 | 1.06× |
| rfw-splice-buf256k | 62,618 | 15.5 | 19.1 | 0.04 | 1.05× |
| rfw-gro-sock4m | 61,948 | 15.6 | 19.3 | 0.04 | 1.04× |

#### UDP open-loop flood, 64 B datagrams, 8 flows (highest rate with ≤0.5 % loss)

| variant | max lossless kpps | vs socat |
|---|---:|---:|
| docker-proxy | 182 | 1.17× |
| socat | 156 ±14% |  |
| rfw | 348 ±29% | 2.24× |
| rfw-splice | 364 | 2.34× |
| rfw-gro | 373 | 2.40× |
| rfw-buf16k | 364 ±6% | 2.34× |
| rfw-buf256k | 346 ±29% | 2.22× |
| rfw-sock4m | 356 ±6% | 2.29× |
| rfw-splice-buf256k | 368 | 2.36× |
| rfw-gro-sock4m | 328 ±9% | 2.11× |

Loss % per offered rate (udp_flood64); `–` = step skipped after >5 % loss.

| variant | 50k pps | 100k pps | 200k pps | 400k pps | 800k pps |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 0.00 | 0.00 | 0.00 | 20.67 | – |
| socat | 0.00 | 0.00 | 0.00 | 26.12 | – |
| rfw | 0.00 | 0.00 | 0.00 | 0.02 | 39.38 |
| rfw-splice | 0.00 | 0.00 | 0.00 | 0.00 | 43.09 |
| rfw-gro | 0.00 | 0.00 | 0.00 | 0.33 | 46.85 |
| rfw-buf16k | 0.00 | 0.00 | 0.00 | 0.14 | 43.86 |
| rfw-buf256k | 0.00 | 0.00 | 0.00 | 0.39 | 42.86 |
| rfw-sock4m | 0.00 | 0.00 | 0.00 | 0.00 | 40.01 |
| rfw-splice-buf256k | 0.00 | 0.00 | 0.00 | 0.00 | 42.11 |
| rfw-gro-sock4m | 0.00 | 0.00 | 0.00 | 0.00 | 42.79 |

#### UDP open-loop flood, 1200 B datagrams, 8 flows

| variant | max lossless kpps | vs socat |
|---|---:|---:|
| docker-proxy | 178 ±6% | 1.08× |
| socat | 164 ±10% |  |
| rfw | 156 | 0.95× |
| rfw-splice | 159 ±12% | 0.97× |
| rfw-gro | 176 ±12% | 1.07× |
| rfw-buf16k | 159 ±9% | 0.97× |
| rfw-buf256k | 180 ±8% | 1.10× |
| rfw-sock4m | 157 ±8% | 0.95× |
| rfw-splice-buf256k | 173 ±6% | 1.05× |
| rfw-gro-sock4m | 164 | 1.00× |

Loss % per offered rate (udp_flood1200); `–` = step skipped after >5 % loss.

| variant | 20k pps | 50k pps | 100k pps | 200k pps |
|---|---:|---:|---:|---:|
| docker-proxy | 0.00 | 0.00 | 0.00 | 0.20 |
| socat | 0.00 | 0.00 | 0.00 | 0.04 |
| rfw | 0.00 | 0.00 | 0.00 | 0.01 |
| rfw-splice | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-gro | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-buf16k | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-buf256k | 0.00 | 0.00 | 0.00 | 0.01 |
| rfw-sock4m | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-splice-buf256k | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-gro-sock4m | 0.00 | 0.00 | 0.00 | 0.00 |

#### TCP bulk, 1 stream, client → server (iperf3)

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 41.6 ±5% | 0 | 0.84 | 0.16 | 3.93× |
| socat | 10.6 | 0 | 0.95 | 0.72 |  |
| rfw | 39.9 | 0 | 0.93 | 0.19 | 3.77× |
| rfw-splice | 41.5 | 0 | 0.74 | 0.14 | 3.93× |
| rfw-gro | 40.4 | 0 | 0.93 | 0.18 | 3.82× |
| rfw-buf16k | 24.0 ±7% | 0 | 0.89 ±8% | 0.30 ±16% | 2.27× |
| rfw-buf256k | 41.5 | 0 | 0.96 | 0.18 | 3.93× |
| rfw-sock4m | 40.8 | 0 | 0.93 | 0.18 | 3.86× |
| rfw-splice-buf256k | 42.2 | 0 | 0.76 | 0.15 | 3.99× |
| rfw-gro-sock4m | 39.7 | 0 | 0.93 | 0.19 | 3.75× |

#### TCP bulk, 8 streams, client → server

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 100.2 | 3 ±267% | 1.14 | 0.09 ±6% | 4.75× |
| socat | 21.1 | 1418 ±49% | 1.95 | 0.74 |  |
| rfw | 77.9 | 4609 ±14% | 1.97 | 0.20 | 3.69× |
| rfw-splice | 94.8 ±6% | 289 ±539% | 1.67 ±13% | 0.14 ±8% | 4.49× |
| rfw-gro | 77.0 ±8% | 4613 ±22% | 1.97 | 0.20 ±9% | 3.65× |
| rfw-buf16k | 45.1 | 97 ±122% | 1.97 | 0.35 ±5% | 2.14× |
| rfw-buf256k | 100.7 | 10 ±145% | 1.97 | 0.16 | 4.77× |
| rfw-sock4m | 76.2 ±8% | 5116 ±29% | 1.96 | 0.21 ±9% | 3.61× |
| rfw-splice-buf256k | 97.2 ±10% | 0 | 1.70 | 0.14 ±12% | 4.61× |
| rfw-gro-sock4m | 76.7 | 4283 ±7% | 1.97 | 0.21 | 3.63× |

#### TCP bulk, 8 streams, server → client

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| docker-proxy | 122.5 ±9% | 743 ±30% | 0.85 | 0.06 ±13% | 2.89× |
| socat | 42.4 ±9% | 1313 ±14% | 1.94 | 0.37 ±9% |  |
| rfw | 66.6 ±7% | 1923 ±6% | 1.97 | 0.24 ±8% | 1.57× |
| rfw-splice | 98.4 ±20% | 902 ±40% | 1.25 ±16% | 0.11 ±7% | 2.32× |
| rfw-gro | 66.9 ±8% | 2141 ±25% | 1.97 | 0.23 ±9% | 1.58× |
| rfw-buf16k | 45.8 | 457 ±51% | 1.97 | 0.34 | 1.08× |
| rfw-buf256k | 97.3 ±10% | 435 ±26% | 1.97 | 0.16 ±11% | 2.30× |
| rfw-sock4m | 66.0 ±8% | 1697 ±5% | 1.97 | 0.24 ±7% | 1.56× |
| rfw-splice-buf256k | 123.3 ±9% | 2222 ±8% | 1.00 | 0.06 ±15% | 2.91× |
| rfw-gro-sock4m | 65.0 ±7% | 1352 ±9% | 1.96 | 0.24 ±8% | 1.53× |
