# rfw benchmark report

- host: Intel(R) Core(TM) 7 240H (16 threads), kernel 7.0.0-38-generic, governor `powersave`
- Docker version 29.9.1, build dfc520d; rfw rfw 1.1.0 (git 7cb54ae)
- forwarder pinned to CPUs `0,2`, backends `8,10`, load generators `12,13,14,15`
- profile `full`, 2 repetition(s), variants interleaved; cells are medians (±spread when >10 %)

## Forwarder pinned to 2 core(s)

### Scorecard

| test | primary metric | socat | docker-proxy | rfw | rfw-splice | rfw vs socat | rfw vs docker-proxy |
|---|---|---:|---:|---:|---:|---:|---:|
| rr1_64 | msg/s | 51,990 | 52,959 | 54,827 | 54,503 | 1.05× | 1.04× |
| rr1_1k | msg/s | 49,906 | 52,189 | 52,832 | 53,914 | 1.06× | 1.01× |
| rr64_64 | msg/s | 154,324 | 165,228 | 262,979 | 251,356 | 1.70× | 1.59× |
| rr8_16k | Mbit/s | 51 | 34,167 | 38,122 | 46,946 | 748.23× | 1.12× |
| open_20k | p50 µs | 74 | 74 | 72 | 72 | 1.03× | 1.02× |
| open_60k | p50 µs | 74 | 76 | 73 | 72 | 1.02× | 1.05× |
| cps | conn/s | 7,442 | 27,228 | 33,436 | 39,674 | 4.49× | 1.23× |
| cps_c256 | conn/s | 5,782 | 34,252 | 39,170 | 42,420 | 6.77× | 1.14× |
| integrity | Gbit/s (echo, 4 conn) | 9.6 | 9.5 | 9.6 | 9.4 | 1.00× | 1.01× |
| udp_rr | pkt/s | 59,201 | 58,507 | 62,184 | 59,270 | 1.05× | 1.06× |
| dns1 | qps | 42,243 | 41,977 | 43,388 | 43,504 | 1.03× | 1.03× |
| dns32 | qps | 157,498 | 188,093 | 221,776 | 240,888 | 1.41× | 1.18× |
| udp_flood64 | max lossless kpps | 141 | 171 | 262 | 259 | 1.86× | 1.53× |
| udp_flood1200 | max lossless kpps | 171 | 120 | 177 | 162 | 1.03× | 1.47× |
| redis | SET c50 rps | 104,223 | 107,847 | 106,866 | 108,202 | 1.03× | 0.99× |
| pg_s1 | tps | 25,759 | 26,050 | 25,482 | 26,005 | 0.99× | 0.98× |
| pg_s16 | tps | 81,671 | 86,065 | 81,487 | 82,967 | 1.00× | 0.95× ⚠ |
| pg_w8 | tps | 12,883 | 13,448 | 12,992 | 12,814 | 1.01× | 0.97× |
| udp_iperf | loss % @500M | 0.01 | 0.01 | 0.02 | 0.03 | 0.58× ⚠ | 0.30× ⚠ |
| bulk_p1 | Gbit/s | 10.2 | 40.3 | 38.7 | 39.9 | 3.78× | 0.96× |
| bulk_p1_rev | Gbit/s | 24.4 | 53.3 | 39.2 | 58.4 | 1.61× | 0.74× ⚠ |
| bulk_p8 | Gbit/s | 20.6 | 88.3 | 66.7 | 77.3 | 3.24× | 0.76× ⚠ |
| bulk_p8_rev | Gbit/s | 40.0 | 106.5 | 57.2 | 95.5 | 1.43× | 0.54× ⚠ |
| bulk_bidir | Gbit/s | 28.6 | 94.8 | 60.5 | 73.6 | 2.11× | 0.64× ⚠ |

### Details

#### TCP latency — 1 connection, 64 B ping-pong (closed loop)

| variant | msg/s | p50 µs | p99 µs | p99.9 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|---:|
| direct | 93,538 | 10.4 | 13.4 | 24.5 | – | 1.80× |
| docker-nat | 73,192 ±17% | 11.8 | 44.1 ±68% | 249.8 ±93% | – | 1.41× |
| docker-proxy | 52,959 | 18.2 | 24.6 ±7% | 94.2 | 0.55 | 1.02× |
| socat | 51,990 | 18.6 | 23.6 | 101.4 | 0.51 |  |
| rfw | 54,827 | 17.6 | 24.8 ±11% | 104.4 ±7% | 0.44 | 1.05× |
| rfw-splice | 54,503 | 17.7 | 23.0 ±5% | 107.5 | 0.47 | 1.05× |
| rfw-d-host | 54,977 | 17.8 | 21.6 | 97.5 | 0.45 | 1.06× |
| rfw-d-host-splice | 51,714 | 18.9 | 22.7 | 91.3 | 0.51 | 0.99× |
| rfw-d-splice-sysres | 51,381 | 19.0 | 22.9 | 98.8 | 0.50 | 0.99× |
| rfw-d-bridge | 51,610 | 18.9 | 22.7 | 96.0 | 0.44 | 0.99× |
| rfw-d-v1.1.0 | 54,857 | 17.8 | 21.6 | 97.0 | 0.44 | 1.06× |
| rfw-d-v1.1.0-splice | 51,685 | 18.9 | 22.9 | 97.5 | 0.51 | 0.99× |
| rfw-splice-buf256k | 55,066 | 17.6 | 22.1 | 100.1 ±8% | 0.47 | 1.06× |

#### TCP latency — 1 connection, 1 KiB ping-pong

| variant | msg/s | p50 µs | p99 µs | vs socat |
|---|---:|---:|---:|---:|
| direct | 91,084 | 10.6 | 13.9 | 1.83× |
| docker-nat | 46,485 ±78% | 23.0 ±49% | 609.5 ±98% | 0.93× |
| docker-proxy | 52,189 | 18.5 | 24.6 | 1.05× |
| socat | 49,906 | 19.4 | 24.5 |  |
| rfw | 52,832 | 18.1 | 25.2 ±11% | 1.06× |
| rfw-splice | 53,914 | 17.9 | 23.5 | 1.08× |
| rfw-d-host | 53,009 | 18.3 | 22.4 | 1.06× |
| rfw-d-host-splice | 50,960 | 19.1 | 23.1 | 1.02× |
| rfw-d-splice-sysres | 50,718 | 19.1 | 23.2 | 1.02× |
| rfw-d-bridge | 50,090 | 19.4 | 23.5 | 1.00× |
| rfw-d-v1.1.0 | 52,816 | 18.4 | 22.4 | 1.06× |
| rfw-d-v1.1.0-splice | 50,942 | 19.1 | 23.4 | 1.02× |
| rfw-splice-buf256k | 53,484 | 17.9 | 25.4 ±15% | 1.07× |

#### TCP small-message throughput — 64 connections × 64 B (closed loop)

| variant | msg/s | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 417,990 | 148 | 309 ±6% | – | 2.71× |
| docker-nat | 352,153 ±9% | 175 | 377 | – | 2.28× |
| docker-proxy | 165,228 | 152 | 1929 | 1.97 | 1.07× |
| socat | 154,324 | 406 | 647 | 2.00 |  |
| rfw | 262,979 | 124 | 3129 | 1.91 | 1.70× |
| rfw-splice | 251,356 | 191 ±12% | 3129 | 1.94 | 1.63× |
| rfw-d-host | 256,401 | 111 ±7% | 3178 | 1.92 | 1.66× |
| rfw-d-host-splice | 229,508 | 278 | 377 | 1.96 | 1.49× |
| rfw-d-splice-sysres | 228,010 | 281 | 387 | 1.96 | 1.48× |
| rfw-d-bridge | 262,944 | 243 | 349 | 1.96 | 1.70× |
| rfw-d-v1.1.0 | 266,698 | 158 ±33% | 2638 ±20% | 1.94 | 1.73× |
| rfw-d-v1.1.0-splice | 222,640 | 288 | 402 | 1.96 | 1.44× |
| rfw-splice-buf256k | 232,300 ±5% | 175 ±38% | 3154 | 1.94 | 1.51× |

#### TCP medium messages — 8 connections × 16 KiB

| variant | Mbit/s | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|
| direct | 62,188 | 68 | – | 1220.56× |
| docker-nat | 55,280 | 86 ±8% | – | 1084.98× |
| docker-proxy | 34,167 | 393 | 2.00 | 670.60× |
| socat | 51 | 41943 | 0.01 ±5% |  |
| rfw | 38,122 ±10% | 91 ±21% | 1.95 | 748.23× |
| rfw-splice | 46,946 | 66 | 1.96 | 921.40× |
| rfw-d-host | 40,871 | 72 | 1.98 | 802.19× |
| rfw-d-host-splice | 42,544 | 70 | 1.96 | 835.02× |
| rfw-d-splice-sysres | 44,467 | 67 | 1.97 | 872.75× |
| rfw-d-bridge | 36,751 | 81 | 1.99 | 721.32× |
| rfw-d-v1.1.0 | 40,759 | 72 | 1.98 | 799.98× |
| rfw-d-v1.1.0-splice | 42,461 | 73 ±7% | 1.97 | 833.39× |
| rfw-splice-buf256k | 43,508 ±11% | 80 ±23% | 1.86 ±6% | 853.94× |

#### TCP latency under load, open loop — 20 000 req/s over 64 connections (no coordinated omission)

| variant | p50 µs | p99 µs | p99.9 µs | msg/s | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|---:|
| direct | 64 | 108 | 296 ±48% | 20,008 | – | 1.15× |
| docker-nat | 65 | 108 | 160 ±10% | 20,008 | – | 1.14× |
| docker-proxy | 74 | 120 | 155 ±7% | 20,008 | 0.32 | 1.00× |
| socat | 74 | 122 | 363 ±29% | 20,008 | 0.26 |  |
| rfw | 72 | 133 ±7% | 552 ±60% | 20,008 | 0.19 | 1.03× |
| rfw-splice | 72 | 124 | 354 | 20,008 | 0.21 | 1.02× |
| rfw-d-host | 72 | 123 | 353 ±59% | 20,008 | 0.19 | 1.02× |
| rfw-d-host-splice | 73 | 124 | 169 | 20,008 | 0.22 | 1.01× |
| rfw-d-splice-sysres | 73 | 125 | 376 ±35% | 20,008 | 0.23 | 1.01× |
| rfw-d-bridge | 73 | 120 | 190 ±11% | 20,008 | 0.19 | 1.01× |
| rfw-d-v1.1.0 | 72 | 123 | 165 ±7% | 20,008 | 0.19 | 1.03× |
| rfw-d-v1.1.0-splice | 73 | 125 | 177 ±14% | 20,008 | 0.22 | 1.01× |
| rfw-splice-buf256k | 72 | 125 | 366 ±57% | 20,008 | 0.20 | 1.02× |

#### TCP latency under heavy load, open loop — 60 000 req/s over 64 connections

| variant | p50 µs | p99 µs | p99.9 µs | msg/s | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|---:|
| direct | 64 | 73 | 717 ±25% | 60,008 | – | 1.15× |
| docker-nat | 66 | 173 ±60% | 1159 ±89% | 60,008 | – | 1.13× |
| docker-proxy | 76 | 148 ±16% | 6820 ±79% | 60,008 | 1.09 | 0.97× |
| socat | 74 | 102 ±12% | 690 ±60% | 60,008 | 0.76 |  |
| rfw | 73 | 204 ±56% | 905 ±68% | 60,008 | 0.58 | 1.02× |
| rfw-splice | 72 | 95 ±6% | 668 ±13% | 60,008 | 0.60 | 1.02× |
| rfw-d-host | 73 | 94 | 150 ±21% | 60,008 | 0.58 | 1.02× |
| rfw-d-host-splice | 74 | 88 | 175 ±31% | 60,008 | 0.66 | 1.00× |
| rfw-d-splice-sysres | 74 | 89 | 428 ±24% | 60,008 | 0.68 | 1.00× |
| rfw-d-bridge | 74 | 97 | 168 ±28% | 60,008 | 0.57 | 1.00× |
| rfw-d-v1.1.0 | 72 | 92 | 120 | 60,008 | 0.55 | 1.02× |
| rfw-d-v1.1.0-splice | 74 | 89 | 497 ±55% | 60,008 | 0.67 | 1.00× |
| rfw-splice-buf256k | 72 | 101 ±15% | 793 ±78% | 60,008 | 0.59 | 1.02× |

#### TCP connection rate — connect + 1 B round trip + close, 64 concurrent

| variant | conn/s | txn p50 µs | txn p99 µs | errors | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 80,863 ±5% | 629 | 3064 | 0 | 10.87× |
| docker-nat | 70,579 | 592 ±7% | 5079 ±8% | 0 | 9.48× |
| docker-proxy | 27,228 | 1831 | 9011 | 0 | 3.66× |
| socat | 7,442 | 8618 | 14352 | 0 |  |
| rfw | 33,436 | 1790 | 4235 ±30% | 0 | 4.49× |
| rfw-splice | 39,674 | 1569 | 2540 | 0 | 5.33× |
| rfw-d-host | 35,604 | 1737 | 2654 | 0 | 4.78× |
| rfw-d-host-splice | 35,714 | 1745 | 2843 ±8% | 0 | 4.80× |
| rfw-d-splice-sysres | 36,084 | 1708 | 2695 ±9% | 0 | 4.85× |
| rfw-d-bridge | 35,604 | 1737 | 2597 | 0 | 4.78× |
| rfw-d-v1.1.0 | 36,148 | 1728 | 2523 | 0 | 4.86× |
| rfw-d-v1.1.0-splice | 37,506 | 1634 | 2728 ±7% | 0 | 5.04× |
| rfw-splice-buf256k | 36,190 ±8% | 1737 ±8% | 2761 ±15% | 0 | 4.86× |

#### TCP connection rate, 256 concurrent

| variant | conn/s | txn p99 µs | errors | vs socat |
|---|---:|---:|---:|---:|
| direct | 74,654 | 13042 ±20% | 0 | 12.91× |
| docker-nat | 64,625 | 18678 ±7% | 0 | 11.18× |
| docker-proxy | 34,252 | 44827 ±13% | 0 | 5.92× |
| socat | 5,782 | 1033896 | 0 |  |
| rfw | 39,170 | 19268 | 0 | 6.77× |
| rfw-splice | 42,420 | 19988 ±25% | 0 | 7.34× |
| rfw-d-host | 37,550 | 20709 ±8% | 0 | 6.49× |
| rfw-d-host-splice | 39,298 | 18612 ±11% | 0 | 6.80× |
| rfw-d-splice-sysres | 41,346 | 18154 ±7% | 0 | 7.15× |
| rfw-d-bridge | 32,762 | 18907 ±13% | 0 | 5.67× |
| rfw-d-v1.1.0 | 38,022 | 19923 ±11% | 0 | 6.58× |
| rfw-d-v1.1.0-splice | 40,673 | 20709 | 0 | 7.03× |
| rfw-splice-buf256k | 30,605 | 22151 | 0 | 5.29× |

#### TCP data integrity + echo throughput — 4 × random stream, half-close, byte-for-byte check

| variant | Gbit/s (echo, 4 conn) | ok | vs socat |
|---|---:|---:|---:|
| direct | 9.5 | 1 | 0.99× |
| docker-nat | 9.4 | 1 | 0.99× |
| docker-proxy | 9.5 | 1 | 0.99× |
| socat | 9.6 | 1 |  |
| rfw | 9.6 | 1 | 1.00× |
| rfw-splice | 9.4 | 1 | 0.99× |
| rfw-d-host | 9.6 | 1 | 1.01× |
| rfw-d-host-splice | 9.5 | 1 | 1.00× |
| rfw-d-splice-sysres | 9.5 | 1 | 0.99× |
| rfw-d-bridge | 9.5 | 1 | 0.99× |
| rfw-d-v1.1.0 | 9.6 | 1 | 1.00× |
| rfw-d-v1.1.0-splice | 9.5 | 1 | 0.99× |
| rfw-splice-buf256k | 9.4 | 1 | 0.98× |

#### UDP round-trip latency — 1 flow, 64 B

| variant | pkt/s | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 111,951 | 8.6 | 11.1 | – | 1.89× |
| docker-nat | 99,240 | 9.8 | 12.6 | – | 1.68× |
| docker-proxy | 58,507 | 16.5 | 20.8 | 0.63 | 0.99× |
| socat | 59,201 | 16.3 | 20.5 | 0.54 |  |
| rfw | 62,184 | 15.6 | 19.5 | 0.54 | 1.05× |
| rfw-splice | 59,270 ±6% | 15.8 | 27.2 ±31% | 0.53 | 1.00× |
| rfw-d-host | 61,920 | 15.7 | 19.4 | 0.55 | 1.05× |
| rfw-d-host-splice | 61,856 | 15.7 | 19.4 | 0.55 | 1.04× |
| rfw-d-splice-sysres | 61,952 | 15.7 | 19.4 | 0.55 | 1.05× |
| rfw-d-bridge | 58,488 | 16.5 | 20.6 | 0.54 | 0.99× |
| rfw-d-v1.1.0 | 62,234 | 15.6 | 19.2 | 0.55 | 1.05× |
| rfw-d-v1.1.0-splice | 60,872 | 15.8 | 21.0 ±7% | 0.56 | 1.03× |
| rfw-splice-buf256k | 60,803 | 15.6 | 22.6 ±15% | 0.54 | 1.03× |

#### DNS (CoreDNS) — 1 client, closed loop

| variant | qps | p50 µs | p99 µs | vs socat |
|---|---:|---:|---:|---:|
| direct | 65,337 | 14 | 19 | 1.55× |
| docker-nat | 58,488 | 16 | 26 ±25% | 1.38× |
| docker-proxy | 41,977 | 22 | 30 ±5% | 0.99× |
| socat | 42,243 | 22 | 31 ±5% |  |
| rfw | 43,388 | 22 | 32 | 1.03× |
| rfw-splice | 43,504 | 22 | 31 ±14% | 1.03× |
| rfw-d-host | 43,918 | 22 | 26 | 1.04× |
| rfw-d-host-splice | 43,602 | 22 | 28 | 1.03× |
| rfw-d-splice-sysres | 43,890 | 22 | 27 | 1.04× |
| rfw-d-bridge | 42,444 | 22 | 29 | 1.00× |
| rfw-d-v1.1.0 | 43,744 | 22 | 26 | 1.04× |
| rfw-d-v1.1.0-splice | 42,190 | 22 | 35 ±20% | 1.00× |
| rfw-splice-buf256k | 42,794 | 22 | 38 ±30% | 1.01× |

#### DNS (CoreDNS) — 32 clients, closed loop

| variant | qps | p50 µs | p99 µs | fwd cores | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 257,041 | 108 | 345 | – | 1.63× |
| docker-nat | 238,432 | 113 | 371 | – | 1.51× |
| docker-proxy | 188,093 | 162 | 353 | 1.26 | 1.19× |
| socat | 157,498 | 107 ±7% | 298 | 1.70 |  |
| rfw | 221,776 | 121 | 359 | 1.60 | 1.41× |
| rfw-splice | 240,888 | 112 | 350 | 1.67 | 1.53× |
| rfw-d-host | 206,358 | 138 | 364 | 1.57 | 1.31× |
| rfw-d-host-splice | 208,644 | 136 | 360 | 1.58 | 1.32× |
| rfw-d-splice-sysres | 204,866 | 140 | 368 | 1.57 | 1.30× |
| rfw-d-bridge | 215,742 | 133 | 334 | 1.66 | 1.37× |
| rfw-d-v1.1.0 | 203,932 | 142 | 368 | 1.57 | 1.29× |
| rfw-d-v1.1.0-splice | 208,314 ±6% | 134 ±9% | 368 | 1.58 | 1.32× |
| rfw-splice-buf256k | 230,832 ±9% | 118 ±7% | 377 ±9% | 1.64 | 1.47× |

#### UDP open-loop flood, 64 B datagrams, 8 flows (highest rate with ≤0.5 % loss)

| variant | max lossless kpps | vs socat |
|---|---:|---:|
| direct | 344 | 2.44× |
| docker-nat | 345 ±12% | 2.45× |
| docker-proxy | 171 | 1.21× |
| socat | 141 ±13% |  |
| rfw | 262 ±37% | 1.86× |
| rfw-splice | 259 ±36% | 1.84× |
| rfw-d-host | 153 ±5% | 1.09× |
| rfw-d-host-splice | 377 | 2.68× |
| rfw-d-splice-sysres | 351 | 2.49× |
| rfw-d-bridge | 189 | 1.34× |
| rfw-d-v1.1.0 | 347 | 2.47× |
| rfw-d-v1.1.0-splice | 167 | 1.19× |
| rfw-splice-buf256k | 365 | 2.59× |

Loss % per offered rate (udp_flood64); `–` = step skipped after >5 % loss.

| variant | 50k pps | 100k pps | 200k pps | 400k pps | 800k pps |
|---|---:|---:|---:|---:|---:|
| direct | 0.00 | 0.00 | 0.00 | 0.00 | 6.82 |
| docker-nat | 0.00 | 0.00 | 0.00 | 0.00 | 38.13 |
| docker-proxy | 0.00 | 0.00 | 0.03 | 14.58 | – |
| socat | 0.00 | 0.00 | 0.01 | 27.94 | – |
| rfw | 0.00 | 0.00 | 0.00 | 0.46 | 42.32 |
| rfw-splice | 0.00 | 0.00 | 0.00 | 0.62 | 40.53 |
| rfw-d-host | 0.00 | 0.00 | 0.02 | 5.26 | 45.50 |
| rfw-d-host-splice | 0.00 | 0.00 | 0.00 | 0.09 | 47.66 |
| rfw-d-splice-sysres | 0.00 | 0.00 | 0.00 | 0.25 | 50.66 |
| rfw-d-bridge | 0.00 | 0.00 | 0.20 | 3.41 | 50.46 |
| rfw-d-v1.1.0 | 0.00 | 0.00 | 0.00 | 0.24 | 49.79 |
| rfw-d-v1.1.0-splice | 0.00 | 0.00 | 0.13 | 1.18 | 49.54 |
| rfw-splice-buf256k | 0.00 | 0.00 | 0.00 | 0.00 | 43.66 |

#### UDP open-loop flood, 1200 B datagrams, 8 flows

| variant | max lossless kpps | vs socat |
|---|---:|---:|
| direct | 167 ±9% | 0.98× |
| docker-nat | 167 ±7% | 0.98× |
| docker-proxy | 120 ±32% | 0.70× |
| socat | 171 ±10% |  |
| rfw | 177 | 1.03× |
| rfw-splice | 162 | 0.95× |
| rfw-d-host | 132 ±44% | 0.77× ⚠ |
| rfw-d-host-splice | 180 ±11% | 1.05× |
| rfw-d-splice-sysres | 152 ±12% | 0.89× ⚠ |
| rfw-d-bridge | 179 | 1.04× |
| rfw-d-v1.1.0 | 150 | 0.88× ⚠ |
| rfw-d-v1.1.0-splice | 153 ±17% | 0.89× ⚠ |
| rfw-splice-buf256k | 169 | 0.99× |

Loss % per offered rate (udp_flood1200); `–` = step skipped after >5 % loss.

| variant | 20k pps | 50k pps | 100k pps | 200k pps |
|---|---:|---:|---:|---:|
| direct | 0.00 | 0.00 | 0.00 | 0.00 |
| docker-nat | 0.00 | 0.00 | 0.00 | 0.00 |
| docker-proxy | 0.00 | 0.00 | 0.00 | 0.70 |
| socat | 0.00 | 0.00 | 0.00 | 0.11 |
| rfw | 0.00 | 0.00 | 0.00 | 0.02 |
| rfw-splice | 0.00 | 0.00 | 0.00 | 0.15 |
| rfw-d-host | 0.00 | 0.00 | 0.00 | 1.24 |
| rfw-d-host-splice | 0.00 | 0.00 | 0.00 | 0.05 |
| rfw-d-splice-sysres | 0.00 | 0.00 | 0.00 | 0.06 |
| rfw-d-bridge | 0.00 | 0.00 | 0.00 | 0.29 |
| rfw-d-v1.1.0 | 0.00 | 0.00 | 0.00 | 0.00 |
| rfw-d-v1.1.0-splice | 0.00 | 0.00 | 0.00 | 0.03 |
| rfw-splice-buf256k | 0.00 | 0.00 | 0.00 | 0.00 |

#### Redis (redis-benchmark through the forwarder)

| variant | SET c50 rps | GET c50 rps | PING c1 rps | PING c1 p50 ms | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 105,383 | 106,472 | 61,885 | 0.015 | 1.01× |
| docker-nat | 96,930 | 98,709 | 56,691 | 0.015 | 0.93× |
| docker-proxy | 107,847 | 108,609 | 40,719 | 0.023 | 1.03× |
| socat | 104,223 | 45,866 | 39,749 | 0.023 |  |
| rfw | 106,866 | 106,642 | 41,290 | 0.023 | 1.03× |
| rfw-splice | 108,202 | 107,470 | 41,295 | 0.023 | 1.04× |
| rfw-d-host | 105,473 | 106,002 | 41,220 | 0.023 | 1.01× |
| rfw-d-host-splice | 108,696 | 108,610 | 39,247 | 0.023 | 1.04× |
| rfw-d-splice-sysres | 108,229 | 108,493 | 39,526 | 0.023 | 1.04× |
| rfw-d-bridge | 97,661 | 98,306 | 39,279 | 0.023 | 0.94× ⚠ |
| rfw-d-v1.1.0 | 106,952 | 107,643 | 41,459 | 0.023 | 1.03× |
| rfw-d-v1.1.0-splice | 107,130 | 107,538 | 39,589 | 0.023 | 1.03× |
| rfw-splice-buf256k | 107,558 | 108,373 | 41,032 | 0.023 | 1.03× |

#### PostgreSQL pgbench select-only, 1 client (the RDS-forwarder case)

| variant | tps | avg ms | vs socat |
|---|---:|---:|---:|
| direct | 33,066 | 0.030 | 1.28× |
| docker-nat | 32,032 | 0.032 | 1.24× |
| docker-proxy | 26,050 | 0.038 | 1.01× |
| socat | 25,759 | 0.039 |  |
| rfw | 25,482 | 0.040 | 0.99× |
| rfw-splice | 26,005 | 0.038 | 1.01× |
| rfw-d-host | 26,448 | 0.038 | 1.03× |
| rfw-d-host-splice | 25,632 | 0.039 | 1.00× |
| rfw-d-splice-sysres | 25,635 | 0.039 | 1.00× |
| rfw-d-bridge | 25,408 | 0.039 | 0.99× |
| rfw-d-v1.1.0 | 26,400 | 0.038 | 1.02× |
| rfw-d-v1.1.0-splice | 25,542 | 0.039 | 0.99× |
| rfw-splice-buf256k | 25,828 | 0.039 | 1.00× |

#### PostgreSQL pgbench select-only, 16 clients

| variant | tps | avg ms | fwd cores | vs socat |
|---|---:|---:|---:|---:|
| direct | 86,174 | 0.185 | – | 1.06× |
| docker-nat | 81,267 ±7% | 0.198 ±8% | – | 1.00× |
| docker-proxy | 86,065 | 0.186 | 0.89 | 1.05× |
| socat | 81,671 | 0.196 | 1.01 |  |
| rfw | 81,487 | 0.197 | 0.77 | 1.00× |
| rfw-splice | 82,967 | 0.193 | 0.83 | 1.02× |
| rfw-d-host | 84,115 | 0.190 | 0.77 | 1.03× |
| rfw-d-host-splice | 86,183 | 0.185 | 0.96 | 1.06× |
| rfw-d-splice-sysres | 85,362 | 0.188 | 0.95 | 1.05× |
| rfw-d-bridge | 86,922 | 0.184 | 0.86 | 1.06× |
| rfw-d-v1.1.0 | 85,289 | 0.188 | 0.81 | 1.04× |
| rfw-d-v1.1.0-splice | 85,039 | 0.188 | 0.96 | 1.04× |
| rfw-splice-buf256k | 81,739 | 0.196 | 0.83 | 1.00× |

#### PostgreSQL pgbench TPC-B-like (writes), 8 clients

| variant | tps | avg ms | vs socat |
|---|---:|---:|---:|
| direct | 13,936 ±7% | 0.577 ±7% | 1.08× |
| docker-nat | 13,500 ±9% | 0.598 ±10% | 1.05× |
| docker-proxy | 13,448 | 0.595 | 1.04× |
| socat | 12,883 | 0.621 |  |
| rfw | 12,992 | 0.616 | 1.01× |
| rfw-splice | 12,814 ±5% | 0.625 ±5% | 0.99× |
| rfw-d-host | 12,634 | 0.634 | 0.98× |
| rfw-d-host-splice | 12,396 | 0.645 | 0.96× |
| rfw-d-splice-sysres | 12,092 | 0.661 | 0.94× ⚠ |
| rfw-d-bridge | 12,431 | 0.643 | 0.96× |
| rfw-d-v1.1.0 | 12,277 | 0.651 | 0.95× |
| rfw-d-v1.1.0-splice | 12,044 | 0.665 | 0.93× ⚠ |
| rfw-splice-buf256k | 12,280 ±12% | 0.660 ±12% | 0.95× |

#### UDP iperf3, 1400 B datagrams

| variant | loss % @500M | loss % @2G | Mbit/s @2G | vs socat |
|---|---:|---:|---:|---:|
| direct | 0.00 | 0.01 ±80% | 2,000 |  |
| docker-nat | 0.56 ±100% | 0.08 ±45% | 2,000 | 0.02× |
| docker-proxy | 0.01 ±100% | 0.01 ±7% | 2,000 | 1.90× |
| socat | 0.01 ±100% | 0.03 ±10% | 2,000 |  |
| rfw | 0.02 ±100% | 0.04 ±38% | 2,000 | 0.58× ⚠ |
| rfw-splice | 0.03 ±32% | 0.04 ±98% | 2,000 | 0.54× ⚠ |
| rfw-d-host | 0.00 | 0.02 ±71% | 2,000 |  |
| rfw-d-host-splice | 0.00 | 0.01 ±12% | 2,000 |  |
| rfw-d-splice-sysres | 0.00 | 0.06 | 2,000 |  |
| rfw-d-bridge | 0.01 ±11% | 0.01 ±79% | 2,000 | 1.08× |
| rfw-d-v1.1.0 | 0.00 | 0.04 ±72% | 2,000 |  |
| rfw-d-v1.1.0-splice | 0.02 ±100% | 0.08 ±44% | 2,000 | 0.82× ⚠ |
| rfw-splice-buf256k | 0.00 | 0.02 ±73% | 2,000 |  |

#### TCP bulk, 1 stream, client → server (iperf3)

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 42.0 | 0 | – | – | 4.10× |
| docker-nat | 35.5 ±9% | 68 ±100% | – | – | 3.47× |
| docker-proxy | 40.3 | 0 | 0.83 | 0.16 | 3.94× |
| socat | 10.2 | 0 | 0.95 | 0.75 |  |
| rfw | 38.7 | 2 ±100% | 0.94 | 0.19 | 3.78× |
| rfw-splice | 39.9 | 0 | 0.74 | 0.15 | 3.90× |
| rfw-d-host | 41.4 | 0 | 0.93 | 0.18 | 4.05× |
| rfw-d-host-splice | 42.4 | 0 | 0.77 | 0.15 | 4.15× |
| rfw-d-splice-sysres | 42.4 | 0 ±100% | 0.75 | 0.14 | 4.15× |
| rfw-d-bridge | 38.9 | 14 ±100% | 0.89 | 0.18 | 3.80× |
| rfw-d-v1.1.0 | 41.2 | 0 | 0.93 | 0.18 | 4.03× |
| rfw-d-v1.1.0-splice | 42.1 | 0 ±100% | 0.78 | 0.15 | 4.11× |
| rfw-splice-buf256k | 42.0 | 0 | 0.76 | 0.14 | 4.11× |

#### TCP bulk, 1 stream, server → client (iperf3 -R)

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 80.3 | 27 ±70% | – | – | 3.29× |
| docker-nat | 74.1 | 68 ±100% | – | – | 3.03× |
| docker-proxy | 53.3 | 270 | 0.36 | 0.05 | 2.18× |
| socat | 24.4 | 83 ±18% | 0.97 | 0.32 |  |
| rfw | 39.2 | 400 ±55% | 1.10 | 0.22 | 1.61× |
| rfw-splice | 58.4 ±7% | 1699 ±57% | 0.54 | 0.07 | 2.39× |
| rfw-d-host | 40.1 | 141 ±32% | 1.10 | 0.22 | 1.64× |
| rfw-d-host-splice | 65.7 | 430 ±12% | 0.59 | 0.07 | 2.69× |
| rfw-d-splice-sysres | 65.1 | 490 ±6% | 0.59 | 0.07 | 2.67× |
| rfw-d-bridge | 39.0 | 198 ±20% | 1.09 | 0.22 | 1.60× |
| rfw-d-v1.1.0 | 39.5 | 128 ±16% | 1.10 | 0.22 | 1.62× |
| rfw-d-v1.1.0-splice | 65.4 | 496 ±22% | 0.59 | 0.07 | 2.68× |
| rfw-splice-buf256k | 57.4 ±10% | 225 ±60% | 0.40 | 0.06 ±6% | 2.35× |

#### TCP bulk, 8 streams, client → server

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 99.2 | 342 ±34% | – | – | 4.81× |
| docker-nat | 92.5 | 90 | – | – | 4.49× |
| docker-proxy | 88.3 | 4 ±75% | 1.16 | 0.10 | 4.29× |
| socat | 20.6 ±7% | 2267 ±33% | 1.95 | 0.76 ±6% |  |
| rfw | 66.7 | 2559 ±24% | 1.96 | 0.23 | 3.24× |
| rfw-splice | 77.3 | 2718 ±68% | 1.40 ±14% | 0.14 ±12% | 3.75× |
| rfw-d-host | 76.6 | 5321 ±18% | 1.96 | 0.21 | 3.71× |
| rfw-d-host-splice | 85.6 | 4774 | 1.19 | 0.11 | 4.15× |
| rfw-d-splice-sysres | 89.0 | 2212 ±100% | 1.57 ±15% | 0.14 ±12% | 4.32× |
| rfw-d-bridge | 75.5 | 16993 ±42% | 1.96 | 0.21 | 3.66× |
| rfw-d-v1.1.0 | 71.6 | 3574 ±58% | 1.96 | 0.22 | 3.47× |
| rfw-d-v1.1.0-splice | 82.5 | 4711 ±8% | 1.18 | 0.11 | 4.00× |
| rfw-splice-buf256k | 90.2 ±9% | 0 | 1.70 | 0.15 ±11% | 4.37× |

#### TCP bulk, 8 streams, server → client

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 141.5 | 236 ±14% | – | – | 3.54× |
| docker-nat | 135.4 | 8965 ±23% | – | – | 3.39× |
| docker-proxy | 106.5 | 316 | 0.87 | 0.07 | 2.66× |
| socat | 40.0 ±9% | 2068 ±39% | 1.94 | 0.39 ±9% |  |
| rfw | 57.2 | 1788 | 1.96 | 0.27 | 1.43× |
| rfw-splice | 95.5 ±15% | 1318 ±30% | 1.40 ±23% | 0.12 ±8% | 2.39× |
| rfw-d-host | 60.5 | 1722 ±5% | 1.96 | 0.26 | 1.51× |
| rfw-d-host-splice | 81.9 ±10% | 852 ±15% | 1.07 ±8% | 0.11 | 2.05× |
| rfw-d-splice-sysres | 78.5 | 508 ±17% | 1.08 ±8% | 0.11 | 1.96× |
| rfw-d-bridge | 57.2 | 1719 | 1.96 | 0.27 | 1.43× |
| rfw-d-v1.1.0 | 60.2 | 1734 ±6% | 1.96 | 0.26 | 1.51× |
| rfw-d-v1.1.0-splice | 96.9 ±24% | 1000 ±44% | 1.26 ±22% | 0.10 | 2.42× |
| rfw-splice-buf256k | 109.2 | 1359 ±32% | 1.00 | 0.07 | 2.73× |

#### TCP bulk, 4 streams per direction (iperf3 --bidir; Gbit/s = both directions summed)

| variant | Gbit/s | retrans | fwd cores | core·s/GB | vs socat |
|---|---:|---:|---:|---:|---:|
| direct | 122.8 | 659 ±50% | – | – | 4.29× |
| docker-nat | 114.3 ±6% | 384 | – | – | 3.99× |
| docker-proxy | 94.8 | 2 ±100% | 1.27 | 0.11 | 3.31× |
| socat | 28.6 ±7% | 1193 ±33% | 1.94 | 0.54 ±7% |  |
| rfw | 60.5 | 858 ±72% | 1.96 | 0.26 | 2.11× |
| rfw-splice | 73.6 | 102 ±23% | 1.07 | 0.12 | 2.57× |
| rfw-d-host | 62.9 | 1712 | 1.96 | 0.25 | 2.19× |
| rfw-d-host-splice | 86.2 ±12% | 228 ±7% | 1.20 ±13% | 0.11 | 3.01× |
| rfw-d-splice-sysres | 76.9 | 502 ±87% | 1.04 | 0.11 | 2.69× |
| rfw-d-bridge | 60.3 | 950 | 1.96 | 0.26 | 2.11× |
| rfw-d-v1.1.0 | 63.5 | 1034 ±29% | 1.96 | 0.25 | 2.22× |
| rfw-d-v1.1.0-splice | 79.7 ±10% | 196 ±12% | 1.05 ±6% | 0.11 | 2.78× |
| rfw-splice-buf256k | 95.7 | 2 ±100% | 1.36 | 0.11 | 3.34× |
