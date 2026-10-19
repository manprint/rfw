# rfw benchmark report

- host: Intel(R) Core(TM) 7 240H (16 threads), kernel 7.0.0-38-generic, governor `powersave`
- Docker version 29.9.1, build dfc520d; rfw rfw 1.1.0 (git 7cb54ae)
- forwarder pinned to CPUs `0,2,4,6`, backends `8,10`, load generators `12,13,14,15`
- profile `full`, 2 repetition(s), variants interleaved; cells are medians (±spread when >10 %)

## Forwarder pinned to 4 core(s)

### Scorecard

| test | primary metric | socat | docker-proxy | rfw | rfw-splice | rfw vs socat | rfw vs docker-proxy |
|---|---|---:|---:|---:|---:|---:|---:|
| rr64_64 | msg/s | 219,418 | 221,058 | 411,580 | 334,128 | 1.88× | 1.86× |
| rr1000_16k | Mbit/s | 23,153 | 46,781 | 43,055 | 45,780 | 1.86× | 0.92× ⚠ |
| cps | conn/s | 13,127 | 43,160 | 59,074 | 63,737 | 4.50× | 1.37× |
| integrity | Gbit/s (echo, 4 conn) | 9.5 | 9.5 | 9.6 | 9.5 | 1.01× | 1.02× |
| udp_rr | pkt/s | 60,935 | 60,338 | 63,954 | 63,632 | 1.05× | 1.06× |
| udp_flood64 | max lossless kpps | 172 | 336 | 457 | 470 | 2.66× | 1.36× |
| udp_flood1200 | max lossless kpps | 163 | 187 | 167 | 183 | 1.03× | 0.89× ⚠ |
| pg_s16 | tps | 85,667 | 84,890 | 86,592 | 86,349 | 1.01× | 1.02× |
| bulk_p1 | Gbit/s | 10.9 | 40.7 | 39.6 | 41.2 | 3.63× | 0.97× |
| bulk_p8 | Gbit/s | 42.1 | 84.7 | 84.0 | 93.7 | 1.99× | 0.99× |
| bulk_p8_rev | Gbit/s | 70.1 | 106.8 | 90.3 | 109.5 | 1.29× | 0.85× ⚠ |

### Details

#### TCP small-message throughput — 64 connections × 64 B (closed loop)

| variant | msg/s | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 416,883 | 148 | 314 ±6% | – | 1.90× |
| docker-proxy | 221,058 | 130 | 3228 | 3.81 | 1.01× |
| socat | 219,418 | 136 | 3219 | 3.09 |  |
| rfw | 411,580 | 148 | 233 | 3.57 | 1.88× |
| rfw-splice | 334,128 | 148 | 1876 ±66% | 3.27 | 1.52× |
| rfw-d-host | 391,518 | 151 | 253 | 3.56 | 1.78× |
| rfw-d-splice-sysres | 307,127 | 173 | 1845 ±68% | 3.47 | 1.40× |
| rfw-splice-buf256k | 374,021 | 161 | 275 ±6% | 3.54 | 1.70× |

#### TCP 1000 concurrent connections × 16 KiB messages (splice pipe-limit probe)

| variant | Mbit/s | p99 µs | errors | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 42,277 ±22% | 8077 ±34% | 0 | – | 1.83× |
| docker-proxy | 46,781 | 10977 | 0 | 3.91 | 2.02× |
| socat | 23,153 ±28% | 42598 | 0 | 2.70 ±32% |  |
| rfw | 43,055 ±8% | 11338 ±6% | 0 | 3.46 ±8% | 1.86× |
| rfw-splice | 45,780 ±7% | 10781 ±14% | 0 | 3.31 ±6% | 1.98× |
| rfw-d-host | 40,850 ±15% | 11895 ±8% | 0 | 3.30 ±15% | 1.76× |
| rfw-d-splice-sysres | 48,531 | 10420 ±14% | 0 | 3.45 | 2.10× |
| rfw-splice-buf256k | 36,815 ±29% | 11796 ±10% | 0 | 2.82 ±31% | 1.59× |

#### TCP connection rate — connect + 1 B round trip + close, 64 concurrent

| variant | conn/s | txn p50 µs | txn p99 µs | errors | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 86,780 | 535 ±17% | 3990 ±14% | 0 | 6.61× |
| docker-proxy | 43,160 | 932 | 6914 ±6% | 0 | 3.29× |
| socat | 13,127 | 4866 | 8487 | 0 |  |
| rfw | 59,074 | 1016 | 2408 ±14% | 0 | 4.50× |
| rfw-splice | 63,737 | 960 | 2101 ±11% | 0 | 4.86× |
| rfw-d-host | 57,260 ±6% | 1055 | 2265 ±26% | 0 | 4.36× |
| rfw-d-splice-sysres | 59,854 | 1016 | 2036 ±18% | 0 | 4.56× |
| rfw-splice-buf256k | 59,890 ±5% | 1018 | 1982 ±24% | 0 | 4.56× |

#### TCP data integrity + echo throughput — 4 × random stream, half-close, byte-for-byte check

| variant | Gbit/s (echo, 4 conn) | ok | vs socat |
|---|---:|---:|---:|
| direct | 9.5 | 1 | 0.99× |
| docker-proxy | 9.5 | 1 | 0.99× |
| socat | 9.5 | 1 |  |
| rfw | 9.6 | 1 | 1.01× |
| rfw-splice | 9.5 | 1 | 1.00× |
| rfw-d-host | 9.6 | 1 | 1.01× |
| rfw-d-splice-sysres | 9.5 | 1 | 1.00× |
| rfw-splice-buf256k | 9.5 | 1 | 0.99× |

#### UDP round-trip latency — 1 flow, 64 B

| variant | pkt/s | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 114,882 | 8.4 | 10.8 | – | 1.89× |
| docker-proxy | 60,338 | 16.0 | 20.1 | 0.60 | 0.99× |
| socat | 60,935 | 15.8 | 19.8 | 0.54 |  |
| rfw | 63,954 | 15.1 | 18.8 | 0.53 | 1.05× |
| rfw-splice | 63,632 | 15.2 | 19.0 | 0.53 | 1.04× |
| rfw-d-host | 62,859 | 15.4 | 19.2 | 0.55 | 1.03× |
| rfw-d-splice-sysres | 62,596 | 15.5 | 19.5 | 0.55 | 1.03× |
| rfw-splice-buf256k | 64,178 | 15.1 | 18.8 | 0.53 | 1.05× |

#### UDP open-loop flood, 64 B datagrams, 8 flows (highest rate with ≤0.5 % loss)

| variant | max lossless kpps | vs socat |
|---|---:|---:|
| direct | 624 | 3.64× |
| docker-proxy | 336 | 1.96× |
| socat | 172 ±15% |  |
| rfw | 457 ±28% | 2.66× |
| rfw-splice | 470 ±23% | 2.74× |
| rfw-d-host | 339 | 1.97× |
| rfw-d-splice-sysres | 342 | 1.99× |
| rfw-splice-buf256k | 340 | 1.98× |

Loss % per offered rate (udp_flood64); `–` = step skipped after >5 % loss.

| variant | 50k pps | 100k pps | 200k pps | 400k pps | 800k pps |
|---|---:|---:|---:|---:|---:|
| direct | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| docker-proxy | 0.00 | 0.00 | 0.00 | 0.15 | 21.68 |
| socat | 0.00 | 0.00 | 0.00 | 0.79 | 23.22 |
| rfw | 0.00 | 0.00 | 0.00 | 0.01 | 1.03 |
| rfw-splice | 0.00 | 0.00 | 0.00 | 0.07 | 4.58 |
| rfw-d-host | 0.00 | 0.00 | 0.00 | 0.03 | 27.44 |
| rfw-d-splice-sysres | 0.00 | 0.00 | 0.00 | 0.01 | 19.61 |
| rfw-splice-buf256k | 0.00 | 0.00 | 0.00 | 0.01 | 11.33 |

#### UDP open-loop flood, 1200 B datagrams, 8 flows

| variant | max lossless kpps | vs socat |
|---|---:|---:|
| direct | 170 ±14% | 1.05× |
| docker-proxy | 187 | 1.15× |
| socat | 163 |  |
| rfw | 167 | 1.03× |
| rfw-splice | 183 ±8% | 1.13× |
| rfw-d-host | 170 ±11% | 1.05× |
| rfw-d-splice-sysres | 171 ±12% | 1.05× |
| rfw-splice-buf256k | 168 | 1.03× |

Loss % per offered rate (udp_flood1200); `–` = step skipped after >5 % loss.

| variant | 20k pps | 50k pps | 100k pps | 200k pps |
|---|---:|---:|---:|---:|
| direct | 0.00 | 0.00 | 0.00 | 0.00 |
| docker-proxy | 0.00 | 0.00 | 0.00 | 0.05 |
| socat | 0.00 | 0.00 | 0.00 | 0.04 |
| rfw | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-splice | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-d-host | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-d-splice-sysres | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-splice-buf256k | 0.00 | 0.00 | 0.00 | 0.00 |

#### PostgreSQL pgbench select-only, 16 clients

| variant | tps | avg ms | fwd cores | vs socat |
|---|---:|---:|---:|---:|
| direct | 85,657 | 0.186 | – | 1.00× |
| docker-proxy | 84,890 | 0.189 | 0.99 | 0.99× |
| socat | 85,667 | 0.186 | 1.01 |  |
| rfw | 86,592 | 0.185 | 0.87 | 1.01× |
| rfw-splice | 86,349 | 0.185 | 0.95 | 1.01× |
| rfw-d-host | 86,441 | 0.185 | 0.92 | 1.01× |
| rfw-d-splice-sysres | 87,600 | 0.182 | 1.11 | 1.02× |
| rfw-splice-buf256k | 87,313 | 0.183 | 0.97 | 1.02× |

#### TCP bulk, 1 stream, client → server (iperf3)

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 42.4 | 0 | – | – | 3.89× |
| docker-proxy | 40.7 | 0 | 0.78 | 0.15 | 3.73× |
| socat | 10.9 ±6% | 2 ±100% | 0.96 | 0.70 ±5% |  |
| rfw | 39.6 | 0 | 0.93 | 0.19 | 3.63× |
| rfw-splice | 41.2 | 0 | 0.74 | 0.14 | 3.77× |
| rfw-d-host | 40.3 | 36 ±100% | 0.94 | 0.19 | 3.69× |
| rfw-d-splice-sysres | 41.3 | 2 ±100% | 0.75 | 0.15 | 3.78× |
| rfw-splice-buf256k | 41.0 | 0 | 0.73 | 0.14 | 3.76× |

#### TCP bulk, 8 streams, client → server

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 100.3 | 816 ±14% | – | – | 2.38× |
| docker-proxy | 84.7 | 0 | 2.12 | 0.20 | 2.01× |
| socat | 42.1 | 1210 ±17% | 3.87 | 0.74 |  |
| rfw | 84.0 | 17 ±29% | 2.96 | 0.28 | 1.99× |
| rfw-splice | 93.7 | 27 ±33% | 2.01 | 0.17 | 2.22× |
| rfw-d-host | 87.2 | 18 ±8% | 3.06 | 0.28 | 2.07× |
| rfw-d-splice-sysres | 86.1 ±9% | 26 ±54% | 2.03 | 0.19 ±9% | 2.04× |
| rfw-splice-buf256k | 92.0 | 0 | 1.92 | 0.17 | 2.18× |

#### TCP bulk, 8 streams, server → client

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 146.6 | 176 ±16% | – | – | 2.09× |
| docker-proxy | 106.8 | 598 ±8% | 0.88 | 0.07 | 1.52× |
| socat | 70.1 | 4951 ±12% | 3.88 | 0.44 |  |
| rfw | 90.3 | 1018 ±6% | 3.64 ±6% | 0.32 | 1.29× |
| rfw-splice | 109.5 | 2073 ±29% | 1.43 | 0.10 | 1.56× |
| rfw-d-host | 86.1 | 1058 ±13% | 3.40 ±5% | 0.32 | 1.23× |
| rfw-d-splice-sysres | 107.7 | 1612 ±11% | 1.45 | 0.11 | 1.54× |
| rfw-splice-buf256k | 107.9 | 1680 ±26% | 1.05 | 0.08 | 1.54× |
