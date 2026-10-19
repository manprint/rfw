#!/usr/bin/env python3
"""Turn bench results (results.jsonl) into Markdown tables, a machine-readable
summary.json, and an optional regression check against a committed baseline.

    bench/report.py bench/results/<run> [more runs...] [--md out.md]
    bench/report.py <run> --baseline bench/baseline/quick-2c.json --check

The regression check compares *ratios to the `direct` baseline measured in the
same run* (never absolute numbers), so a slow or noisy CI runner does not
trip it. It hard-fails only on correctness problems.
"""

import argparse
import json
import os
import statistics
import sys

ORDER = ["direct", "docker-nat", "docker-proxy", "socat", "socat-nodelay", "rfw", "rfw-w1", "rfw-w4", "rfw-splice", "rfw-gro", "rfw-d-host",
         "rfw-d-host-splice", "rfw-d-unconfined", "rfw-d-privileged", "rfw-d-splice-sysres", "rfw-d-bridge", "rfw-d-default-ulimit", "rfw-d-compose", "rfw-d-v1.1.0", "rfw-d-v1.1.0-splice",
         "rfw-buf16k", "rfw-buf256k", "rfw-sock4m", "rfw-splice-buf256k", "rfw-gro-sock4m"]
RFW = [v for v in ORDER if v.startswith("rfw")]


def g(d, *path):
    for p in path:
        if not isinstance(d, dict) or p not in d:
            return None
        d = d[p]
    return d


def cores_busy(rec):
    c, w = rec.get("fwd_cpu_s"), rec.get("wall_s")
    return None if c is None or not w else c / w


def cpu_per_gb(rec):
    cb = cores_busy(rec)
    gb = g(rec, "result", "gbit_per_s")
    return None if cb is None or not gb else cb / (gb / 8)


def best_flood(steps_key):
    return lambda r: g(r, "result", steps_key)


# test -> [(column, extractor(rec), fmt, higher_is_better)]; first column is the primary metric
M = {
    "rr1_64": [("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
               ("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.1f}", False),
               ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.1f}", False),
               ("p99.9 µs", lambda r: g(r, "result", "lat_us", "p999"), "{:.1f}", False),
               ("fwd cores", cores_busy, "{:.2f}", False)],
    "rr1_1k": [("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
               ("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.1f}", False),
               ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.1f}", False)],
    "rr64_64": [("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
                ("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
                ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                ("fwd cores", cores_busy, "{:.2f}", False)],
    "rr8_16k": [("Mbit/s", lambda r: g(r, "result", "mbit_per_s"), "{:,.0f}", True),
                ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                ("fwd cores", cores_busy, "{:.2f}", False)],
    "rr256_16k": [("Mbit/s", lambda r: g(r, "result", "mbit_per_s"), "{:,.0f}", True),
                  ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                  ("errors", lambda r: g(r, "result", "errors"), "{:.0f}", False),
                  ("fwd cores", cores_busy, "{:.2f}", False)],
    "rr1000_16k": [("Mbit/s", lambda r: g(r, "result", "mbit_per_s"), "{:,.0f}", True),
                   ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                   ("errors", lambda r: g(r, "result", "errors"), "{:.0f}", False),
                   ("fwd cores", cores_busy, "{:.2f}", False)],
    "open_20k": [("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
                 ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                 ("p99.9 µs", lambda r: g(r, "result", "lat_us", "p999"), "{:.0f}", False),
                 ("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
                 ("fwd cores", cores_busy, "{:.2f}", False)],
    "open_60k": [("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
                 ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                 ("p99.9 µs", lambda r: g(r, "result", "lat_us", "p999"), "{:.0f}", False),
                 ("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
                 ("fwd cores", cores_busy, "{:.2f}", False)],
    "open_120k": [("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
                 ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                 ("p99.9 µs", lambda r: g(r, "result", "lat_us", "p999"), "{:.0f}", False),
                 ("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
                 ("fwd cores", cores_busy, "{:.2f}", False)],
    "open_180k": [("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
                 ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
                 ("p99.9 µs", lambda r: g(r, "result", "lat_us", "p999"), "{:.0f}", False),
                 ("msg/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
                 ("fwd cores", cores_busy, "{:.2f}", False)],
    "cps": [("conn/s", lambda r: g(r, "result", "cps"), "{:,.0f}", True),
            ("txn p50 µs", lambda r: g(r, "result", "txn_lat_us", "p50"), "{:.0f}", False),
            ("txn p99 µs", lambda r: g(r, "result", "txn_lat_us", "p99"), "{:.0f}", False),
            ("errors", lambda r: g(r, "result", "errors"), "{:.0f}", False)],
    "cps_c256": [("conn/s", lambda r: g(r, "result", "cps"), "{:,.0f}", True),
                 ("txn p99 µs", lambda r: g(r, "result", "txn_lat_us", "p99"), "{:.0f}", False),
                 ("errors", lambda r: g(r, "result", "errors"), "{:.0f}", False)],
    "integrity": [("Gbit/s (echo, 4 conn)", lambda r: (g(r, "result", "mbit_per_s_each_way") or 0) / 1000 or None, "{:.1f}", True),
                  ("ok", lambda r: None if g(r, "result", "ok") is None else float(g(r, "result", "ok")), "{:.0f}", True)],
    "udp_rr": [("pkt/s", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
               ("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.1f}", False),
               ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.1f}", False),
               ("fwd cores", cores_busy, "{:.2f}", False)],
    "dns1": [("qps", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
             ("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
             ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False)],
    "dns32": [("qps", lambda r: g(r, "result", "msgs_per_s"), "{:,.0f}", True),
              ("p50 µs", lambda r: g(r, "result", "lat_us", "p50"), "{:.0f}", False),
              ("p99 µs", lambda r: g(r, "result", "lat_us", "p99"), "{:.0f}", False),
              ("fwd cores", cores_busy, "{:.2f}", False)],
    "udp_flood64": [("max lossless kpps", lambda r: (g(r, "result", "max_lossless_recv_pps") or 0) / 1000 or None, "{:,.0f}", True)],
    "udp_flood1200": [("max lossless kpps", lambda r: (g(r, "result", "max_lossless_recv_pps") or 0) / 1000 or None, "{:,.0f}", True)],
    "redis": [("SET c50 rps", lambda r: g(r, "result", "c50_set", "rps"), "{:,.0f}", True),
              ("GET c50 rps", lambda r: g(r, "result", "c50_get", "rps"), "{:,.0f}", True),
              ("PING c1 rps", lambda r: g(r, "result", "c1_ping_mbulk", "rps") or g(r, "result", "c1_ping", "rps"), "{:,.0f}", True),
              ("PING c1 p50 ms", lambda r: g(r, "result", "c1_ping_mbulk", "p50_ms") or g(r, "result", "c1_ping", "p50_ms"), "{:.3f}", False)],
    "pg_s1": [("tps", lambda r: g(r, "result", "tps"), "{:,.0f}", True),
              ("avg ms", lambda r: g(r, "result", "lat_avg_ms"), "{:.3f}", False)],
    "pg_s16": [("tps", lambda r: g(r, "result", "tps"), "{:,.0f}", True),
               ("avg ms", lambda r: g(r, "result", "lat_avg_ms"), "{:.3f}", False),
               ("fwd cores", cores_busy, "{:.2f}", False)],
    "pg_w8": [("tps", lambda r: g(r, "result", "tps"), "{:,.0f}", True),
              ("avg ms", lambda r: g(r, "result", "lat_avg_ms"), "{:.3f}", False)],
    "udp_iperf": [("loss % @500M", lambda r: _step(r, 0, "loss_pct"), "{:.2f}", False),
                  ("loss % @2G", lambda r: _step(r, 1, "loss_pct"), "{:.2f}", False),
                  ("Mbit/s @2G", lambda r: _step(r, 1, "mbit_per_s"), "{:,.0f}", True)],
}
for t, par, rev in (("bulk_p1", 1, False), ("bulk_p1_rev", 1, True), ("bulk_p8", 8, False), ("bulk_p8_rev", 8, True),
                    ("bulk_bidir", 4, False)):
    M[t] = [("Gbit/s", lambda r: g(r, "result", "gbit_per_s"), "{:.1f}", True),
            ("retrans", lambda r: g(r, "result", "retransmits"), "{:.0f}", False),
            ("fwd cores", cores_busy, "{:.2f}", False),
            ("core·s/GB", cpu_per_gb, "{:.2f}", False)]

TITLES = {
    "rr1_64": "TCP latency — 1 connection, 64 B ping-pong (closed loop)",
    "rr1_1k": "TCP latency — 1 connection, 1 KiB ping-pong",
    "rr64_64": "TCP small-message throughput — 64 connections × 64 B (closed loop)",
    "rr256_16k": "TCP 256 concurrent connections × 16 KiB messages",
    "rr1000_16k": "TCP 1000 concurrent connections × 16 KiB messages (splice pipe-limit probe)",
    "rr8_16k": "TCP medium messages — 8 connections × 16 KiB",
    "open_20k": "TCP latency under load, open loop — 20 000 req/s over 64 connections (no coordinated omission)",
    "open_60k": "TCP latency under heavy load, open loop — 60 000 req/s over 64 connections",
    "open_120k": "TCP latency near saturation, open loop — 120 000 req/s over 64 connections",
    "open_180k": "TCP latency past saturation for slow forwarders, open loop — 180 000 req/s over 64 connections",
    "bulk_p1": "TCP bulk, 1 stream, client → server (iperf3)",
    "bulk_p1_rev": "TCP bulk, 1 stream, server → client (iperf3 -R)",
    "bulk_p8": "TCP bulk, 8 streams, client → server",
    "bulk_p8_rev": "TCP bulk, 8 streams, server → client",
    "bulk_bidir": "TCP bulk, 4 streams per direction (iperf3 --bidir; Gbit/s = both directions summed)",
    "cps": "TCP connection rate — connect + 1 B round trip + close, 64 concurrent",
    "cps_c256": "TCP connection rate, 256 concurrent",
    "integrity": "TCP data integrity + echo throughput — 4 × random stream, half-close, byte-for-byte check",
    "redis": "Redis (redis-benchmark through the forwarder)",
    "pg_s1": "PostgreSQL pgbench select-only, 1 client (the RDS-forwarder case)",
    "pg_s16": "PostgreSQL pgbench select-only, 16 clients",
    "pg_w8": "PostgreSQL pgbench TPC-B-like (writes), 8 clients",
    "udp_rr": "UDP round-trip latency — 1 flow, 64 B",
    "dns1": "DNS (CoreDNS) — 1 client, closed loop",
    "dns32": "DNS (CoreDNS) — 32 clients, closed loop",
    "udp_flood64": "UDP open-loop flood, 64 B datagrams, 8 flows (highest rate with ≤0.5 % loss)",
    "udp_flood1200": "UDP open-loop flood, 1200 B datagrams, 8 flows",
    "udp_iperf": "UDP iperf3, 1400 B datagrams",
}


def _step(r, i, key):
    s = g(r, "result", "steps")
    if isinstance(s, list) and len(s) > i and isinstance(s[i], dict):
        return s[i].get(key)
    return None


def load(paths):
    recs = []
    for p in paths:
        f = os.path.join(p, "results.jsonl") if os.path.isdir(p) else p
        for line in open(f):
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    return recs


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def agg(recs, cores):
    """{test: {variant: [rec,...]}} for one forwarder-core budget."""
    out = {}
    for r in recs:
        if r.get("fwd_cores", cores) != cores or "result" not in r:
            continue
        out.setdefault(r["test"], {}).setdefault(r["variant"], []).append(r)
    return out


def cell(recs, ex, fmt):
    vals = [ex(r) for r in recs]
    vals = [v for v in vals if v is not None]
    if not vals:
        return "–", None
    m = statistics.median(vals)
    s = fmt.format(m)
    if len(vals) > 1:
        lo, hi = min(vals), max(vals)
        if m and (hi - lo) / abs(m) > 0.10:
            s += f" ±{(hi - lo) / 2 / abs(m) * 100:.0f}%"
    return s, m


def scores(data):
    """{variant|test|column: ratio vs direct, oriented so higher = better}."""
    out = {}
    for test, cols in M.items():
        if test not in data or "direct" not in data[test]:
            continue
        # primary + latency-ish columns that exist
        for name, ex, fmt, hib in cols:
            if name in ("fwd cores", "core·s/GB", "ok", "errors", "retrans"):
                continue
            # Tail percentiles are reported but never gated: on a shared 4-vCPU CI
            # runner a single scheduling hiccup moves p99/p99.9 by 2x between runs.
            if "p99" in name:
                continue
            d = med([ex(r) for r in data[test]["direct"]])
            if not d:
                continue
            for v, recs in data[test].items():
                if v == "direct":
                    continue
                x = med([ex(r) for r in recs])
                if x:
                    out[f"{v}|{test}|{name}"] = (x / d) if hib else (d / x)
    return out


def table(test, data, cores):
    cols = M[test]
    present = [v for v in ORDER if v in data.get(test, {})]
    if not present:
        return ""
    head = ["variant"] + [c[0] for c in cols] + ["vs socat"]
    rows = []
    prim = {}
    for v in present:
        cells = []
        for i, (name, ex, fmt, hib) in enumerate(cols):
            s, m = cell(data[test][v], ex, fmt)
            cells.append(s)
            if i == 0:
                prim[v] = m
        rows.append([v] + cells)
    soc = prim.get("socat")
    hib0 = cols[0][3]
    for row in rows:
        v = row[0]
        p = prim.get(v)
        if v == "socat" or not soc or not p:
            row.append("")
        else:
            ratio = p / soc if hib0 else soc / p
            row.append(f"{ratio:.2f}×" + (" ⚠" if ratio < 0.95 and v.startswith("rfw") else ""))
    lines = [f"#### {TITLES.get(test, test)}", "", "| " + " | ".join(head) + " |", "|" + "|".join(["---"] + ["---:"] * (len(head) - 1)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return "\n".join(lines)


def flood_detail(data, test):
    present = [v for v in ORDER if v in data.get(test, {})]
    if not present:
        return ""
    rates = sorted({s["rate_target"] for v in present for r in data[test][v] for s in (g(r, "result", "steps") or [])})
    if not rates:
        return ""
    lines = [f"Loss % per offered rate ({test}); `–` = step skipped after >5 % loss.", "",
             "| variant | " + " | ".join(f"{int(r / 1000)}k pps" for r in rates) + " |", "|---|" + "---:|" * len(rates)]
    for v in present:
        cells = []
        for rate in rates:
            vals = [s["loss_pct"] for r in data[test][v] for s in (g(r, "result", "steps") or []) if s["rate_target"] == rate]
            cells.append(f"{statistics.median(vals):.2f}" if vals else "–")
        lines.append(f"| {v} | " + " | ".join(cells) + " |")
    lines.append("")
    return "\n".join(lines)


def scorecard(data):
    """rfw (best of rfw / rfw-splice) vs socat vs docker-proxy on each primary metric."""
    lines = ["| test | primary metric | socat | docker-proxy | rfw | rfw-splice | rfw vs socat | rfw vs docker-proxy |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for test, cols in M.items():
        if test not in data:
            continue
        name, ex, fmt, hib = cols[0]
        vals = {}
        for v in ("socat", "docker-proxy", "rfw", "rfw-splice"):
            vals[v] = med([ex(r) for r in data[test].get(v, [])])
        if vals["rfw"] is None:
            continue

        def rel(a, b):
            if not a or not b:
                return "–"
            r = a / b if hib else b / a
            return f"{r:.2f}×" + (" ⚠" if r < 0.95 else "")

        f = lambda x: "–" if x is None else fmt.format(x)  # noqa: E731
        lines.append(f"| {test} | {name} | {f(vals['socat'])} | {f(vals['docker-proxy'])} | {f(vals['rfw'])} | {f(vals['rfw-splice'])} | "
                     f"{rel(vals['rfw'], vals['socat'])} | {rel(vals['rfw'], vals['docker-proxy'])} |")
    return "\n".join(lines) + "\n"


def health_problems(recs, cores):
    probs = []
    for r in recs:
        if r.get("fwd_cores", cores) != cores:
            continue
        if r["test"] == "_start" and "error" in r:
            probs.append(f"{r['variant']}: failed to start: {r['error']}")
        elif r["test"] == "_health":
            h = r["result"]
            if h.get("active_tcp"):
                probs.append(f"{r['variant']}: {h['active_tcp']} TCP connection(s) still active after the run (leak)")
            if h.get("rejected"):
                probs.append(f"{r['variant']}: {h['rejected']} connection(s) rejected")
        elif "error" in r and r["variant"] in RFW:
            probs.append(f"{r['variant']}/{r['test']}: {r['error'][:160]}")
        elif r["test"] == "integrity" and r["variant"] in RFW and not g(r, "result", "ok"):
            probs.append(f"{r['variant']}: INTEGRITY FAILURE")
    return probs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--md")
    ap.add_argument("--json", help="write summary.json here")
    ap.add_argument("--baseline")
    ap.add_argument("--check", action="store_true", help="exit 1 on correctness problems or ratio regressions")
    ap.add_argument("--tolerance", type=float, default=0.30, help="allowed relative drop of a ratio-to-direct score vs baseline")
    ap.add_argument("--update-baseline", help="write the scores of this run as a baseline file")
    a = ap.parse_args()

    recs = load(a.runs)
    cores_set = sorted({r.get("fwd_cores") for r in recs if r.get("fwd_cores")})
    meta = {}
    for p in a.runs:
        mp = os.path.join(p, "meta.json")
        if os.path.exists(mp):
            meta = json.load(open(mp))
            break

    md = ["# rfw benchmark report", ""]
    if meta:
        md += [f"- host: {meta.get('cpu_model')} ({meta.get('nproc')} threads), kernel {meta.get('kernel')}, governor `{meta.get('governor')}`",
               f"- {meta.get('docker')}; rfw {meta.get('rfw_version')} (git {meta.get('git_rev')})",
               f"- forwarder pinned to CPUs `{meta.get('fwd_cpus')}`, backends `{meta.get('backend_cpus')}`, load generators `{meta.get('client_cpus')}`",
               f"- profile `{meta.get('profile')}`, {meta.get('reps')} repetition(s), variants interleaved; cells are medians (±spread when >10 %)", ""]
    summary = {}
    problems = []
    for cores in cores_set:
        data = agg(recs, cores)
        md += [f"## Forwarder pinned to {cores} core(s)", "", "### Scorecard", "", scorecard(data), "### Details", ""]
        for t in M:
            if t in data:
                md.append(table(t, data, cores))
                if t.startswith("udp_flood"):
                    md.append(flood_detail(data, t))
        summary[str(cores)] = scores(data)
        problems += [f"[{cores}c] {p}" for p in health_problems(recs, cores)]
    if problems:
        md += ["## Problems detected", ""] + [f"- {p}" for p in problems] + [""]

    text = "\n".join(md)
    if a.md:
        open(a.md, "w").write(text)
    else:
        print(text)
    if a.json:
        json.dump({"scores": summary, "problems": problems}, open(a.json, "w"), indent=1, sort_keys=True)
    if a.update_baseline:
        json.dump({"scores": summary, "meta": {k: meta.get(k) for k in ("cpu_model", "nproc", "kernel", "profile", "rfw_version")}},
                  open(a.update_baseline, "w"), indent=1, sort_keys=True)

    rc = 0
    if a.baseline and os.path.exists(a.baseline):
        base = json.load(open(a.baseline))["scores"]
        regress = []
        for cores, sc in summary.items():
            for k, now in sc.items():
                if not k.startswith("rfw"):
                    continue
                was = (base.get(cores) or {}).get(k)
                if was and now < was * (1 - a.tolerance):
                    regress.append(f"[{cores}c] {k}: {now:.2f} vs baseline {was:.2f} ({(now / was - 1) * 100:+.0f}%)")
        if regress:
            print("\n### Possible regressions vs baseline (ratio to `direct`)\n", file=sys.stderr)
            for r in regress:
                print("- " + r, file=sys.stderr)
            if a.check:
                rc = 1
    if a.check and problems:
        print("\nCORRECTNESS PROBLEMS:\n" + "\n".join(problems), file=sys.stderr)
        rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
