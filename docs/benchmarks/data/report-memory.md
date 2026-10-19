## Memory and scaling

#### Idle memory with N TCP + N UDP forwarded ports (MiB of anonymous memory, all processes)

socat needs one process per port and protocol, docker `-p` one docker-proxy per published port and protocol; rfw is one process.

| variant | N=10 | N=100 | N=1,000 |
|---|---:|---:|---:|
| rfw | 1.0 · up in 0.1s | 2.7 · up in 0.1s | 22.4 · up in 0.1s |
| rfw-splice | 0.8 · up in 0.1s | 2.5 · up in 0.1s | 22.3 · up in 0.1s |
| rfw-d-host | 1.0 · 3 procs · up in 0.14s | 3.2 · 3 procs · up in 0.25s | 27.3 · 3 procs · up in 0.22s |
| rfw-d-compose | 1.1 · 3 procs · up in 0.1s | 2.9 · 3 procs · up in 0.2s | 28.3 · 3 procs · up in 0.21s |
| socat | 14.4 · 21 procs · up in 0.2s | 143.4 · 201 procs · up in 0.2s | 1,431.4 · 2001 procs · up in 0.92s |
| docker-proxy | 56.7 · 40 procs · up in 0.41s | 563.3 · 400 procs · up in 3.79s | 5,651.4 · 4000 procs · up in 89.18s |

#### N concurrent idle TCP connections (marginal anonymous memory per connection)

Each connection already did one verified round trip; copy buffers are allocated but barely touched.

| variant | N=100 | N=1,000 | N=10,000 |
|---|---:|---:|---:|
| rfw | 19.4 KiB/conn (3.2 MiB total) | 19.1 KiB/conn (20.0 MiB total) | 19.3 KiB/conn (190.1 MiB total) |
| rfw-splice | 1.7 KiB/conn (1.4 MiB total) | 11.9 KiB/conn (12.9 MiB total) | 18.6 KiB/conn (183.0 MiB total) |
| rfw-d-host | 32.2 KiB/conn (4.5 MiB total) | 37.9 KiB/conn (38.4 MiB total) | 38.7 KiB/conn (379.6 MiB total) |
| rfw-d-compose | 34.0 KiB/conn (4.6 MiB total) | 37.3 KiB/conn (20.0 MiB total) **488 failed** | 38.0 KiB/conn (20.3 MiB total) **9488 failed** |
| socat | 54.9 KiB/conn (52.7 MiB total) | 55.1 KiB/conn (101.2 MiB total) | 54.2 KiB/conn (577.1 MiB total) |
| docker-proxy | 109.8 KiB/conn (198.6 MiB total) | 27.1 KiB/conn (213.7 MiB total) | 20.7 KiB/conn (390.3 MiB total) |

#### N concurrent TCP connections that each moved 1 MiB (copy buffers touched)

| variant | N=100 | N=1,000 | N=5,000 |
|---|---:|---:|---:|
| rfw | 42.8 KiB/conn (5.5 MiB total) | 43.1 KiB/conn (43.4 MiB total) | 43.2 KiB/conn (212.5 MiB total) |
| rfw-splice | 1.8 KiB/conn (1.4 MiB total) | 25.6 KiB/conn (26.3 MiB total) | 39.7 KiB/conn (195.3 MiB total) |
| rfw-d-host | 53.4 KiB/conn (6.6 MiB total) | 58.3 KiB/conn (58.3 MiB total) | 60.0 KiB/conn (294.5 MiB total) |
| socat | 59.0 KiB/conn (53.1 MiB total) | 58.5 KiB/conn (104.5 MiB total) | 59.0 KiB/conn (335.4 MiB total) |
| docker-proxy | 111.7 KiB/conn (198.5 MiB total) | 27.1 KiB/conn (213.8 MiB total) | 20.4 KiB/conn (287.2 MiB total) |

#### N concurrent UDP flows (sessions)

`failed` = flows whose first datagram never came back.

| variant | N=100 | N=1,000 | N=10,000 |
|---|---:|---:|---:|
| rfw | 0.3 KiB/conn (1.4 MiB total) | 1.6 KiB/conn (2.9 MiB total) | 1.8 KiB/conn (18.5 MiB total) |
| rfw-splice | 0.8 KiB/conn (1.3 MiB total) | 1.7 KiB/conn (2.9 MiB total) | 1.8 KiB/conn (18.5 MiB total) |
| rfw-d-host | 1.3 KiB/conn (1.4 MiB total) | 1.7 KiB/conn (3.0 MiB total) | 1.8 KiB/conn (18.5 MiB total) |
| rfw-d-compose | 1.0 KiB/conn (1.5 MiB total) | 1.7 KiB/conn (3.0 MiB total) | 1.8 KiB/conn (18.5 MiB total) |
| socat | 54.8 KiB/conn (52.6 MiB total) **1 failed** **-1 lost** | 54.9 KiB/conn (74.4 MiB total) **496 failed** **-319 lost** | 345.9 KiB/conn (373.0 MiB total) **9036 failed** **-8430 lost** |
| docker-proxy | 151.4 KiB/conn (202.4 MiB total) | 79.2 KiB/conn (265.9 MiB total) | 71.4 KiB/conn (884.5 MiB total) |

#### Soak: connection churn + bulk integrity transfers, memory / fds / threads over time

| variant | duration | iterations | errors | anon MiB first → max → last | fds first → last | threads first → last | active after |
|---|---:|---:|---:|---|---|---|---:|
| rfw | 240s | 3 | 0 | 3.8 → 4.9 → 4.9 | 76 → 76 | 3 → 3 | 0 |
| rfw-splice | 240s | 4 | 0 | 1.4 → 1.5 → 1.5 | 77 → 76 | 3 → 3 | 0 |
| rfw-d-host | 240s | 4 | 0 | 5.3 → 5.3 → 5.3 | – → – | – → – | 0 |
| rfw-d-compose | 240s | 4 | 0 | 4.7 → 4.7 → 4.7 | – → – | – → – | 0 |
| socat | 240s | 4 | 0 | 47.4 → 47.4 → 47.4 | – → – | – → – | None |
