#!/usr/bin/env python3
"""rfw benchmark orchestrator.

    bench/run.py --profile quick            # ~10 min, what CI runs
    bench/run.py --profile full --reps 3    # the complete comparison

Every forwarder variant is started in turn (interleaved across repetitions so
CPU-frequency / thermal drift hits all variants alike), driven through the same
scenarios, and its CPU time and memory are metered. Results are appended to
<out>/results.jsonl after every test; `bench/report.py` turns them into tables.
"""

import argparse
import atexit
import json
import os
import signal
import subprocess
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *  # noqa: F401,F403,E402
import harness as H  # noqa: E402

PROFILES = {
    # quick: CI tier, must finish in ~10 minutes on a 4 vCPU runner
    "quick": dict(dur=3, warm=1, bulk=4, cps_total=15000, flood_dur=3, pg_dur=4, redis_n=60000, integ_mb=128,
                  flood64=[100_000, 300_000], flood1200=[50_000], ladder=False),
    "full": dict(dur=8, warm=1, bulk=8, cps_total=40000, flood_dur=5, pg_dur=8, redis_n=200000, integ_mb=512,
                 flood64=[50_000, 100_000, 200_000, 400_000, 800_000], flood1200=[20_000, 50_000, 100_000, 200_000],
                 ladder=True),
}

QUICK_TESTS = ["smoke", "rr1_64", "rr64_64", "open_20k", "bulk_p1", "bulk_p8_rev", "cps", "integrity", "udp_rr",
               "udp_flood64", "udp_flood1200"]
FULL_TESTS = ["smoke", "rr1_64", "rr1_1k", "rr64_64", "rr8_16k", "open_20k", "open_60k", "bulk_p1", "bulk_p1_rev",
              "bulk_p8", "bulk_p8_rev", "bulk_bidir", "cps", "cps_c256", "integrity", "redis", "pg_s1", "pg_s16", "pg_w8",
              "udp_rr", "dns1", "dns32", "udp_flood64", "udp_flood1200", "udp_iperf"]
QUICK_VARIANTS = ["direct", "docker-nat", "docker-proxy", "socat", "rfw", "rfw-splice", "rfw-d-host"]
FULL_VARIANTS = ["direct", "docker-nat", "docker-proxy", "socat", "socat-nodelay", "rfw", "rfw-splice", "rfw-gro",
                 "rfw-d-host", "rfw-d-host-splice", "rfw-d-unconfined", "rfw-d-privileged", "rfw-d-bridge",
                 "rfw-d-v1.1.0"]


class Ctx:
    pass


# ----------------------------------------------------------------- runners

def bench(ctx, argv, timeout=300):
    cmd = ["taskset", "-c", ctx.cpus_client, ctx.bench_bin, *[str(a) for a in argv]]
    r = sh(cmd, check=False, timeout=timeout)
    j = last_json(r.stdout)
    if j is None:
        raise RuntimeError(f"rfwbench {argv[0]} produced no JSON (rc={r.returncode}): {r.stderr.strip()[-300:]}")
    if r.returncode != 0:
        j["rc"] = r.returncode
    return j


def dclient(ctx, image, args, entrypoint=None, env=None, timeout=300):
    cmd = ["docker", "run", "--rm", "--network", "host", "--label", LABEL, "--cpuset-cpus", ctx.cpus_client]
    for k, v in (env or {}).items():
        cmd += ["-e", f"{k}={v}"]
    if entrypoint:
        cmd += ["--entrypoint", entrypoint]
    cmd += [image, *[str(a) for a in args]]
    return sh(cmd, check=False, timeout=timeout)


def ep(v, bport, proto="tcp"):
    return v.ep(bport, proto)


# --------------------------------------------------------------- scenarios

def sc_smoke(ctx, v, P):
    h, p = ep(v, ECHO_BASE)
    r = bench(ctx, ["rr", "--host", h, "--base", p, "--ports", 1, "--conns", 1, "--duration", 1, "--warmup", 0.2])
    u = bench(ctx, ["udprr", "--host", *[ep(v, ECHO_BASE, "udp")[0]], "--base", ep(v, ECHO_BASE, "udp")[1],
                    "--ports", 1, "--flows", 1, "--duration", 1, "--warmup", 0.2])
    if r["msgs"] < 100 or u["msgs"] < 100:
        raise RuntimeError(f"smoke failed: tcp msgs={r['msgs']} udp msgs={u['msgs']}")
    return {"tcp_msgs": r["msgs"], "udp_msgs": u["msgs"]}


def make_rr(conns, size, rate=0, ports=None):
    def f(ctx, v, P):
        h, p = ep(v, ECHO_BASE)
        a = ["rr", "--host", h, "--base", p, "--ports", ports or min(conns, ECHO_PORTS), "--conns", conns, "--size", size,
             "--duration", P["dur"], "--warmup", P["warm"]]
        if rate:
            a += ["--rate", rate]
        return bench(ctx, a)
    return f


def iperf_tcp(ctx, v, P, par, rev=False, bidir=False):
    h, p = ep(v, 5201)
    a = ["-c", h, "-p", p, "-t", P["bulk"], "-P", par, "-J", "-O", 1]
    if rev:
        a.append("-R")
    if bidir:
        a.append("--bidir")
    for attempt in range(3):
        r = dclient(ctx, IMG_IPERF, a)
        try:
            j = json.loads(r.stdout)
            e = j["end"]
            if bidir:
                snd, rcv = e["sum_sent"], e["sum_received"]
                return {"gbit_per_s": (rcv["bits_per_second"] + e["sum_received_bidir_reverse"]["bits_per_second"]) / 1e9,
                        "retransmits": snd.get("retransmits", 0)}
            return {"gbit_per_s": e["sum_received"]["bits_per_second"] / 1e9, "retransmits": e["sum_sent"].get("retransmits", 0),
                    "parallel": par, "reverse": rev}
        except (ValueError, KeyError):
            time.sleep(2)  # iperf3 server may still be finishing the previous test
    raise RuntimeError("iperf3 failed: " + (r.stdout[-200:] or r.stderr[-200:]))


def sc_cps(conns):
    def f(ctx, v, P):
        h, p = ep(v, ECHO_BASE)
        return bench(ctx, ["cps", "--host", h, "--base", p, "--ports", ECHO_PORTS, "--conns", conns,
                           "--total", P["cps_total"]])
    return f


def sc_integrity(ctx, v, P):
    h, p = ep(v, ECHO_BASE)
    r = bench(ctx, ["integrity", "--host", h, "--port", p, "--bytes", P["integ_mb"] << 20, "--conns", 4], timeout=600)
    if not r.get("ok"):
        raise RuntimeError(f"INTEGRITY FAILURE: {r}")
    return r


def sc_udp_rr(ctx, v, P):
    h, p = ep(v, ECHO_BASE, "udp")
    return bench(ctx, ["udprr", "--host", h, "--base", p, "--ports", 1, "--flows", 1, "--duration", P["dur"],
                       "--warmup", P["warm"]])


def make_dns(flows):
    def f(ctx, v, P):
        h, p = ep(v, 53, "udp")
        return bench(ctx, ["udprr", "--host", h, "--base", p, "--ports", 1, "--flows", flows, "--duration", P["dur"],
                           "--warmup", P["warm"], "--dns"])
    return f


def make_flood(size, key):
    def f(ctx, v, P):
        h, p = ep(v, ECHO_BASE, "udp")
        out = []
        for rate in P[key]:
            r = bench(ctx, ["udpflood", "--host", h, "--base", p, "--ports", 8, "--flows", 8, "--size", size,
                            "--rate", rate, "--duration", P["flood_dur"]], timeout=120)
            out.append(r)
            time.sleep(1)
            if P["ladder"] and r["loss_pct"] > 5.0:
                break
        ok = [r for r in out if r["loss_pct"] <= 0.5]
        return {"steps": out, "max_lossless_rate": max((r["rate_target"] for r in ok), default=0),
                "max_lossless_recv_pps": max((r["recv_pps"] for r in ok), default=0)}
    return f


def sc_udp_iperf(ctx, v, P):
    h, p = ep(v, 5201, "udp")
    res = []
    for rate in ("500M", "2G"):
        r = dclient(ctx, IMG_IPERF, ["-c", h, "-p", p, "-u", "-b", rate, "-l", 1400, "-t", P["bulk"], "-J", "-O", 1])
        try:
            s = json.loads(r.stdout)["end"]["sum"]
            res.append({"rate": rate, "mbit_per_s": s["bits_per_second"] / 1e6, "loss_pct": s["lost_percent"],
                        "jitter_ms": s["jitter_ms"]})
        except (ValueError, KeyError):
            res.append({"rate": rate, "error": (r.stdout[-150:] or r.stderr[-150:])})
        time.sleep(1)
    return {"steps": res}


def sc_redis(ctx, v, P):
    h, p = ep(v, 6379)
    out = {}
    for name, c, tests in (("c50", 50, "set,get"), ("c1", 1, "ping_mbulk")):
        n = P["redis_n"] if c > 1 else max(5000, P["redis_n"] // 8)
        r = dclient(ctx, IMG_REDIS, ["-h", h, "-p", p, "-c", c, "-n", n, "-t", tests, "--csv"], entrypoint="redis-benchmark")
        for line in r.stdout.splitlines():
            f = [x.strip('"') for x in line.split(",")]
            if len(f) >= 8 and f[1].replace(".", "").isdigit():
                out[f"{name}_{f[0].lower().split()[0]}"] = {"rps": float(f[1]), "avg_ms": float(f[2]), "p50_ms": float(f[4]),
                                                            "p99_ms": float(f[6])}
    if not out:
        raise RuntimeError("redis-benchmark produced no rows")
    return out


def make_pg(clients, select_only):
    def f(ctx, v, P):
        h, p = ep(v, 5432)
        a = ["-h", h, "-p", p, "-U", "postgres", "-n", "-c", clients, "-j", min(clients, 2), "-T", P["pg_dur"], "-r"]
        if select_only:
            a.append("-S")
        r = dclient(ctx, IMG_PG, [*a, "postgres"], entrypoint="pgbench", env={"PGPASSWORD": "bench"})
        out = r.stdout
        res = {}
        for line in out.splitlines():
            if line.startswith("tps ="):
                res["tps"] = float(line.split("=")[1].split()[0])
            elif line.startswith("latency average"):
                res["lat_avg_ms"] = float(line.split("=")[1].split()[0])
            elif "failed transactions" in line:
                res["failed"] = int(line.split(":")[1].split()[0])
        if "tps" not in res:
            raise RuntimeError("pgbench failed: " + (out[-200:] + r.stderr[-200:]))
        return res
    return f


TESTS = {
    "smoke": sc_smoke,
    "rr1_64": make_rr(1, 64),
    "rr1_1k": make_rr(1, 1024),
    "rr64_64": make_rr(64, 64),
    "rr8_16k": make_rr(8, 16384),
    "rr256_16k": make_rr(256, 16384),
    "rr1000_16k": make_rr(1000, 16384),
    "open_20k": make_rr(64, 64, rate=20000),
    "open_60k": make_rr(64, 64, rate=60000),
    "open_120k": make_rr(64, 64, rate=120000),
    "open_180k": make_rr(64, 64, rate=180000),
    "bulk_p1": lambda c, v, P: iperf_tcp(c, v, P, 1),
    "bulk_p1_rev": lambda c, v, P: iperf_tcp(c, v, P, 1, rev=True),
    "bulk_p8": lambda c, v, P: iperf_tcp(c, v, P, 8),
    "bulk_p8_rev": lambda c, v, P: iperf_tcp(c, v, P, 8, rev=True),
    "bulk_bidir": lambda c, v, P: iperf_tcp(c, v, P, 4, bidir=True),
    "cps": sc_cps(64),
    "cps_c256": sc_cps(256),
    "integrity": sc_integrity,
    "redis": sc_redis,
    "pg_s1": make_pg(1, True),
    "pg_s16": make_pg(16, True),
    "pg_w8": make_pg(8, False),
    "udp_rr": sc_udp_rr,
    "dns1": make_dns(1),
    "dns32": make_dns(32),
    "udp_flood64": make_flood(64, "flood64"),
    "udp_flood1200": make_flood(1200, "flood1200"),
    "udp_iperf": sc_udp_iperf,
}
NEEDS_DB = {"redis", "pg_s1", "pg_s16", "pg_w8"}
NEEDS_DNS = {"dns1", "dns32"}


# ---------------------------------------------------------------- main loop

def run_test(ctx, v, name, P, rep, out_f):
    cpu0 = v.cpu()
    sys0 = system_cpu_busy()
    t0 = time.time()
    rec = {"variant": v.name, "rep": rep, "test": name, "fwd_cores": ctx.fwd_cores}
    try:
        rec["result"] = TESTS[name](ctx, v, P)
    except Exception as e:  # noqa: BLE001
        rec["error"] = str(e)[:500]
        log(f"  {v.name}/{name}: ERROR {e}")
    rec["wall_s"] = round(time.time() - t0, 2)
    rec["sys_cpu_s"] = round(system_cpu_busy() - sys0, 2)
    c1 = v.cpu()
    rec["fwd_cpu_s"] = None if cpu0 is None or c1 is None else round(c1 - cpu0, 3)
    out_f.write(json.dumps(rec) + "\n")
    out_f.flush()
    return rec


def summarize(rec):
    r = rec.get("result") or {}
    for k in ("msgs_per_s", "gbit_per_s", "cps", "tps", "max_lossless_rate"):
        if k in r:
            return f"{k}={r[k]:.4g}"
    return "ok" if "error" not in rec else "ERR"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=PROFILES, default="quick")
    ap.add_argument("--variants", help="comma list (default: profile set)")
    ap.add_argument("--tests", help="comma list (default: profile set)")
    ap.add_argument("--reps", type=int, default=None)
    ap.add_argument("--fwd-cores", type=int, default=2, help="CPUs the forwarder is pinned to (2 or 4 mirror the target hosts)")
    ap.add_argument("--cpus-fwd")
    ap.add_argument("--cpus-backend")
    ap.add_argument("--cpus-client")
    ap.add_argument("--out", default=None)
    ap.add_argument("--rfw-bin", default=os.path.join(REPO, "target", "release", "rfw"))
    ap.add_argument("--no-image-build", action="store_true", help="reuse an existing rfw-bench:local image")
    ap.add_argument("--no-pin-proxy", action="store_true")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    if args.list:
        print("variants:", ", ".join(VARIANTS))
        print("tests:", ", ".join(TESTS))
        return

    P = PROFILES[args.profile]
    variants = (args.variants.split(",") if args.variants else (QUICK_VARIANTS if args.profile == "quick" else FULL_VARIANTS))
    tests = (args.tests.split(",") if args.tests else (QUICK_TESTS if args.profile == "quick" else FULL_TESTS))
    reps = args.reps or (2 if args.profile == "quick" else 3)
    for n in variants:
        if n not in VARIANTS:
            sys.exit(f"unknown variant {n}")

    ctx = Ctx()
    f_default, b_default, c_default = default_cpusets(os.cpu_count())
    fwd_list = f_default.split(",")
    ctx.fwd_cores = args.fwd_cores
    ctx.cpus_fwd = args.cpus_fwd or ",".join(fwd_list[:args.fwd_cores])
    ctx.cpus_backend = args.cpus_backend or b_default
    ctx.cpus_client = args.cpus_client or c_default
    ctx.out = os.path.abspath(args.out or os.path.join(ROOT, "results", time.strftime("%Y%m%d-%H%M%S")))
    os.makedirs(ctx.out, exist_ok=True)
    ctx.rfw_bin = os.path.abspath(args.rfw_bin)
    ctx.pin_proxy = not args.no_pin_proxy
    ctx.spec_override = None
    ctx.host_ip = primary_ip()
    ctx.bench_bin = os.path.join(ROOT, "rfwbench", "target", "x86_64-unknown-linux-musl", "release", "rfwbench")
    if not os.path.exists(ctx.bench_bin):
        log("building rfwbench (musl, static)")
        sh(["cargo", "build", "--release", "--target", "x86_64-unknown-linux-musl",
            "--manifest-path", os.path.join(ROOT, "rfwbench", "Cargo.toml")], timeout=600)
    need_local_image = any(VARIANTS[n].__mro__ and issubclass(VARIANTS[n], RfwDocker) and VARIANTS[n].image is None for n in variants)
    need_db = any(t in NEEDS_DB for t in tests)
    need_dns = any(t in NEEDS_DNS for t in tests)

    meta = host_meta()
    meta.update(profile=args.profile, variants=variants, tests=tests, reps=reps, fwd_cpus=ctx.cpus_fwd,
                backend_cpus=ctx.cpus_backend, client_cpus=ctx.cpus_client, host_ip=ctx.host_ip,
                rfw_version=sh([ctx.rfw_bin, "--version"], check=False).stdout.strip())
    json.dump(meta, open(os.path.join(ctx.out, "meta.json"), "w"), indent=2)
    log("meta:", json.dumps({k: meta[k] for k in ("cpu_model", "governor", "fwd_cpus", "backend_cpus", "client_cpus")}))

    wanted = {LISTEN_BASE + b for b in [*range(ECHO_BASE, ECHO_BASE + ECHO_PORTS), 5201, 6379, 5432, 53]}
    wanted |= {PUBLISH_BASE + (p - LISTEN_BASE) for p in wanted} | {METRICS_PORT}
    busy = sorted(wanted & listening_ports())
    if busy:
        sys.exit(f"ports in the benchmark range are already in use: {busy[:10]}")

    atexit.register(cleanup_all)
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(143))
    cleanup_all()
    if need_local_image:
        if args.no_image_build and docker("image", "inspect", IMG_RFW_LOCAL, check=False).returncode == 0:
            pass
        else:
            ensure_local_image(REPO)
    elif any(VARIANTS[n].image == IMG_RFW_RELEASED for n in variants if hasattr(VARIANTS[n], "image")):
        if docker("image", "inspect", IMG_RFW_RELEASED, check=False).returncode != 0:
            docker("pull", "-q", IMG_RFW_RELEASED)

    ctx.fleet = Fleet(ctx.bench_bin, ctx.out, ctx.cpus_backend, with_db=need_db, with_dns=need_dns)
    ctx.fleet.start()
    if need_db:
        ctx.fleet.init_pgbench()
    log("backends up:", ctx.fleet.ip)

    out_f = open(os.path.join(ctx.out, "results.jsonl"), "a")
    for rep in range(reps):
        order = shuffled_rotation(variants, rep)
        log(f"=== rep {rep + 1}/{reps}, order: {order}")
        for name in order:
            v = VARIANTS[name](ctx)
            log(f"-- {name}: {v.desc}")
            try:
                v.start()
                wait_variant_ready(v)
                time.sleep(1.5)
            except Exception as e:  # noqa: BLE001
                log(f"  start failed: {e}")
                out_f.write(json.dumps({"variant": name, "rep": rep, "test": "_start", "error": str(e)[:500]}) + "\n")
                out_f.flush()
                try:
                    v.stop()
                except Exception:  # noqa: BLE001
                    pass
                continue
            try:
                for t in tests:
                    if t in NEEDS_DNS and not need_dns:
                        continue
                    rec = run_test(ctx, v, t, P, rep, out_f)
                    log(f"  {t}: {summarize(rec)}")
                st = v.stats()
                if st is not None:
                    time.sleep(2)
                    st = v.stats()
                    out_f.write(json.dumps({"variant": name, "rep": rep, "test": "_health", "fwd_cores": ctx.fwd_cores,
                                            "result": {"active_tcp": stats_sum(st, "active_connections", "tcp"),
                                                       "rejected": stats_sum(st, "rejected_connections"),
                                                       "dgrams_dropped": stats_sum(st, "datagrams_dropped"),
                                                       "udp_sessions": stats_sum(st, "active_connections", "udp")}}) + "\n")
                    out_f.flush()
            finally:
                v.stop()
                time.sleep(1)
    log("done ->", ctx.out)


if __name__ == "__main__":
    main()
