#!/usr/bin/env python3
"""Markdown tables for memtest.py output (mem.jsonl).

    bench/memreport.py bench/results/<mem-run> [--md out.md]
"""

import argparse
import json
import os

ORDER = ["rfw", "rfw-splice", "rfw-d-host", "rfw-d-compose", "socat", "docker-proxy"]


def load(p):
    f = os.path.join(p, "mem.jsonl") if os.path.isdir(p) else p
    return [json.loads(l) for l in open(f) if l.strip()]


def mb(kb):
    return "–" if kb is None else (f"{kb / 1024:,.1f}" if kb < 10 * 1024 * 1024 else f"{kb / 1048576:,.2f} GiB")


def grid(recs, test, title, cell, note=""):
    rs = [r for r in recs if r.get("test") == test]
    if not rs:
        return ""
    ns = sorted({r["n"] for r in rs})
    vs = [v for v in ORDER if any(r["variant"] == v for r in rs)] + sorted({r["variant"] for r in rs} - set(ORDER))
    out = [f"#### {title}", ""]
    if note:
        out += [note, ""]
    out += ["| variant | " + " | ".join(f"N={n:,}" for n in ns) + " |", "|---|" + "---:|" * len(ns)]
    for v in vs:
        cells = []
        for n in ns:
            r = next((x for x in rs if x["variant"] == v and x["n"] == n), None)
            cells.append(cell(r) if r else "–")
        out.append(f"| {v} | " + " | ".join(cells) + " |")
    return "\n".join(out) + "\n"


def conns_cell(r):
    if r.get("error"):
        return "error"
    base = (r.get("idle") or {}).get("anon_kb")
    hold = (r.get("hold") or {}).get("anon_kb")
    s = f"{r['kb_per_conn']:.1f} KiB/conn" if r.get("kb_per_conn") is not None else "–"
    if hold is not None:
        s += f" ({mb(hold)} MiB total)" if hold < 10 * 1024 * 1024 else f" ({mb(hold)})"
    if r.get("failed"):
        s += f" **{r['failed']} failed**"
    if r.get("alive_after_hold") not in (None, r.get("established")):
        s += f" **{r.get('established', 0) - (r.get('alive_after_hold') or 0)} lost**"
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("--md")
    a = ap.parse_args()
    recs = load(a.run)
    md = ["## Memory and scaling", ""]
    md.append(grid(recs, "mem_ports", "Idle memory with N TCP + N UDP forwarded ports (MiB of anonymous memory, all processes)",
                   lambda r: "error" if r.get("error") else f"{mb(r.get('anon_kb'))}" + (f" · {r['procs']} procs" if r.get("procs") else "")
                   + f" · up in {r.get('startup_s')}s",
                   "socat needs one process per port and protocol, docker `-p` one docker-proxy per published port and protocol; rfw is one process."))
    md.append(grid(recs, "mem_tcp", "N concurrent idle TCP connections (marginal anonymous memory per connection)", conns_cell,
                   "Each connection already did one verified round trip; copy buffers are allocated but barely touched."))
    md.append(grid(recs, "mem_tcp_active", "N concurrent TCP connections that each moved 1 MiB (copy buffers touched)", conns_cell))
    md.append(grid(recs, "mem_udp", "N concurrent UDP flows (sessions)", conns_cell,
                   "`failed` = flows whose first datagram never came back."))
    soak = [r for r in recs if r.get("test") == "soak"]
    if soak:
        md += ["#### Soak: connection churn + bulk integrity transfers, memory / fds / threads over time", "",
               "| variant | duration | iterations | errors | anon MiB first → max → last | fds first → last | threads first → last | active after |",
               "|---|---:|---:|---:|---|---|---|---:|"]
        for r in soak:
            if r.get("error"):
                md.append(f"| {r['variant']} | error: {r['error'][:80]} |||||||")
                continue
            s = [x for x in r["series"] if x.get("anon_kb") is not None]
            f = [x.get("fds") for x in r["series"] if x.get("fds") is not None]
            t = [x.get("threads") for x in r["series"] if x.get("threads") is not None]
            md.append(f"| {r['variant']} | {r['seconds']}s | {r['iterations']} | {r['errors']} | "
                      f"{mb(s[0]['anon_kb'])} → {mb(max(x['anon_kb'] for x in s))} → {mb(s[-1]['anon_kb'])} | "
                      f"{f[0] if f else '–'} → {f[-1] if f else '–'} | {t[0] if t else '–'} → {t[-1] if t else '–'} | {r.get('active_after')} |")
        md.append("")
    text = "\n".join(md)
    if a.md:
        open(a.md, "w").write(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
