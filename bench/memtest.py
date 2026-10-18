#!/usr/bin/env python3
"""Memory / scaling / leak tests for the forwarders.

    bench/memtest.py --tests ports,conns,udp,soak --out bench/results/mem1

  ports  idle memory with N forwarded ports (TCP+UDP each), N = 10/100/1000
  conns  memory with N concurrent idle TCP connections, N = 100/1000/10000
  udp    memory with N concurrent UDP flows (sessions),  N = 100/1000/10000
  soak   connection churn for a while; memory / fd / thread series (leak check)

Memory is *anonymous* memory: Pss_Anon of the process (native) or cgroup
memory.stat anon (containers), summed over every process of the forwarder, so
socat's forked children and docker-proxy's one-process-per-port are counted
honestly. Kernel + socket memory is reported separately where it is visible.
"""

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import *  # noqa: F401,F403,E402
import harness as H  # noqa: E402
import run as R  # noqa: E402

MEM_BASE = 30000  # listen ports for the N-port tests (below the ephemeral range)
PUB_BASE = 14000  # docker -p range for the N-port test


class PublishN(Variant):
    """`docker run -p` of N TCP and N UDP ports: what Docker itself costs."""

    name = "docker-proxy"
    desc = "docker -p with N ports (one docker-proxy process per port and protocol)"

    def __init__(self, ctx, n):
        super().__init__(ctx)
        self.n = n

    def start(self):
        a = ["run", "-d", "--name", "rfwb-pub", "--label", LABEL, "--network", NET]
        step = 250  # keep each -p range modest
        for lo in range(0, self.n, step):
            hi = min(self.n, lo + step) - 1
            for proto in ("tcp", "udp"):
                a += ["-p", f"{PUB_BASE + lo}-{PUB_BASE + hi}:{PUB_BASE + lo}-{PUB_BASE + hi}/{proto}"]
        docker(*a, IMG_ALPINE, "sleep", "100000", timeout=900)
        self.ports = list(range(PUB_BASE, PUB_BASE + self.n))
        deadline = time.time() + 120
        while time.time() < deadline and len(proxy_pids(self.ports)) < 2 * self.n:
            time.sleep(0.5)
        self.pids = proxy_pids(self.ports)

    def stop(self):
        docker("rm", "-f", "rfwb-pub", check=False)

    def mem(self):
        return {"anon_kb": root_pss_anon(self.pids), "procs": len(self.pids)}

    def ep(self, bport, proto="tcp"):
        return "127.0.0.1", PUB_BASE


def proc_info(v):
    """threads / fds / processes where they can be read."""
    out = {}
    pid = getattr(getattr(v, "proc", None), "pid", None)
    if pid:
        try:
            out["threads"] = int([l for l in open(f"/proc/{pid}/status") if l.startswith("Threads:")][0].split()[1])
            out["fds"] = len(os.listdir(f"/proc/{pid}/fd"))
        except OSError:
            pass
    cid = getattr(v, "cid", None)
    if cid:
        d = cgroup_dir(cid)
        if d:
            try:
                out["procs"] = int(open(f"{d}/pids.current").read())
            except OSError:
                pass
    return out


def n_specs(ctx, n):
    echo = ctx.fleet.ip["echo"]
    return [(proto, MEM_BASE + i, echo, ECHO_BASE + (i % ECHO_PORTS)) for i in range(n) for proto in ("tcp", "udp")]


def wait_port(v_host, port, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if tcp_port_open(v_host, port, 0.3):
            return time.time() - t0
        time.sleep(0.1)
    return None


def t_ports(ctx, vcls, n):
    ctx.spec_override = n_specs(ctx, n)
    t0 = time.time()
    v = PublishN(ctx, n) if vcls is None else vcls(ctx)
    try:
        v.start()
        if vcls is None:
            startup = time.time() - t0
        else:
            w = wait_port("127.0.0.1", MEM_BASE + n - 1, 180)
            if w is None:
                raise RuntimeError("last port never opened")
            startup = time.time() - t0
        time.sleep(4)
        base = v.mem()
        # touch every 10th TCP port once so lazily allocated per-port state exists
        if vcls is not None:
            for i in range(0, n, max(1, n // 100)):
                tcp_port_open("127.0.0.1", MEM_BASE + i)
            time.sleep(2)
        rec = {"test": "mem_ports", "n": n, "startup_s": round(startup, 2), **v.mem(), **proc_info(v)}
        rec["anon_kb_untouched"] = base.get("anon_kb")
        return rec
    finally:
        v.stop()
        ctx.spec_override = None
        time.sleep(1.5)


def hold(ctx, v, kind, n, hold_s, burst=0):
    h, p = v.ep(ECHO_BASE, "udp" if kind == "udp" else "tcp")
    cmd = ["taskset", "-c", ctx.cpus_client, ctx.bench_bin, "udphold" if kind == "udp" else "hold", "--host", h, "--base", str(p),
           "--ports", str(ECHO_PORTS), "--flows" if kind == "udp" else "--conns", str(n), "--hold", str(hold_s)]
    if burst:
        cmd += ["--burst", str(burst)]
    pr = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, stderr=subprocess.DEVNULL)
    ready = None
    t0 = time.time()
    while time.time() - t0 < 300:
        line = pr.stdout.readline()
        if not line:
            break
        if '"ready"' in line:
            ready = json.loads(line)
            break
    return pr, ready


def t_hold(ctx, vcls, kind, n, burst=0):
    v = vcls(ctx)
    ctx.spec_override = None
    try:
        v.start()
        wait_variant_ready(v)
        time.sleep(2)
        idle = v.mem()
        idle_info = proc_info(v)
        pr, ready = hold(ctx, v, kind, n, 14, burst)
        if not ready:
            pr.kill()
            raise RuntimeError("client never became ready")
        time.sleep(4)
        m1 = v.mem()
        info = proc_info(v)
        out, _ = pr.communicate(timeout=120)
        done = {}
        for line in out.splitlines():
            if '"done"' in line:
                done = json.loads(line)
        per = None
        if m1.get("anon_kb") is not None and idle.get("anon_kb") is not None and ready["established"]:
            per = (m1["anon_kb"] - idle["anon_kb"]) / ready["established"]
        stats = v.stats()
        time.sleep(3)
        st2 = v.stats()
        return {"test": f"mem_{kind}" + ("_active" if burst else ""), "n": n, "burst": burst, "established": ready["established"], "failed": ready["failed"],
                "setup_s": ready["setup_s"], "alive_after_hold": done.get("alive_after_hold"), "idle": idle, "hold": m1,
                "kb_per_conn": None if per is None else round(per, 2), "proc": info, "proc_idle": idle_info,
                "active_after_close": stats_sum(st2, "active_connections", "tcp" if kind == "tcp" else "udp")}
    finally:
        v.stop()
        time.sleep(1.5)


def t_soak(ctx, vcls, seconds):
    v = vcls(ctx)
    ctx.spec_override = None
    series = []
    try:
        v.start()
        wait_variant_ready(v)
        time.sleep(2)
        t0 = time.time()
        h, p = v.ep(ECHO_BASE)
        errors = 0
        i = 0
        while time.time() - t0 < seconds:
            r1 = R.bench(ctx, ["cps", "--host", h, "--base", p, "--ports", ECHO_PORTS, "--conns", 32, "--total", 8000])
            r3 = R.bench(ctx, ["integrity", "--host", h, "--port", p, "--bytes", 32 << 20, "--conns", 2], timeout=120)
            errors += r1.get("errors", 0) + (0 if r3.get("ok") else 1)
            time.sleep(1.0)  # TIME_WAIT bound: 8000 conns per ~2s spread over 32 ports
            m = v.mem()
            series.append({"t": round(time.time() - t0, 1), "anon_kb": m.get("anon_kb"), **proc_info(v)})
            i += 1
        # settle, then final sample
        time.sleep(5)
        m = v.mem()
        series.append({"t": round(time.time() - t0, 1), "anon_kb": m.get("anon_kb"), **proc_info(v), "final": True})
        st = v.stats()
        return {"test": "soak", "seconds": seconds, "iterations": i, "errors": errors, "series": series,
                "active_after": stats_sum(st, "active_connections", "tcp")}
    finally:
        v.stop()
        time.sleep(1.5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tests", default="ports,conns,udp,soak")
    ap.add_argument("--variants", default="rfw,rfw-splice,rfw-d-host,socat,docker-proxy")
    ap.add_argument("--ports-n", default="10,100,1000")
    ap.add_argument("--conns-n", default="100,1000,10000")
    ap.add_argument("--udp-n", default="100,1000,10000")
    ap.add_argument("--soak-seconds", type=int, default=90)
    ap.add_argument("--fwd-cores", type=int, default=2)
    ap.add_argument("--out", default=None)
    ap.add_argument("--rfw-bin", default=os.path.join(REPO, "target", "release", "rfw"))
    ap.add_argument("--no-image-build", action="store_true")
    a = ap.parse_args()

    ctx = R.Ctx()
    f_default, b_default, c_default = default_cpusets(os.cpu_count())
    ctx.fwd_cores = a.fwd_cores
    ctx.cpus_fwd = ",".join(f_default.split(",")[:a.fwd_cores])
    ctx.cpus_backend, ctx.cpus_client = b_default, c_default
    ctx.out = os.path.abspath(a.out or os.path.join(ROOT, "results", "mem-" + time.strftime("%Y%m%d-%H%M%S")))
    os.makedirs(ctx.out, exist_ok=True)
    ctx.rfw_bin = os.path.abspath(a.rfw_bin)
    ctx.pin_proxy = True
    ctx.spec_override = None
    ctx.host_ip = primary_ip()
    ctx.bench_bin = os.path.join(ROOT, "rfwbench", "target", "x86_64-unknown-linux-musl", "release", "rfwbench")
    tests = a.tests.split(",")
    vnames = a.variants.split(",")

    busy = sorted(({MEM_BASE + i for i in range(1000)} | {PUB_BASE + i for i in range(1000)}) & listening_ports())
    if busy:
        sys.exit(f"ports in the memory-test range are in use: {busy[:10]}")

    atexit_register = __import__("atexit").register
    atexit_register(cleanup_all)
    cleanup_all()
    if any(VARIANTS[n].__mro__ and issubclass(VARIANTS[n], RfwDocker) and VARIANTS[n].image is None for n in vnames):
        if not (a.no_image_build and docker("image", "inspect", IMG_RFW_LOCAL, check=False).returncode == 0):
            ensure_local_image(REPO)
    ctx.fleet = Fleet(ctx.bench_bin, ctx.out, ctx.cpus_backend, with_db=False, with_dns=False)
    ctx.fleet.start()
    json.dump(dict(host_meta(), fwd_cpus=ctx.cpus_fwd, tests=tests, variants=vnames,
                   rfw_version=sh([ctx.rfw_bin, "--version"], check=False).stdout.strip()),
              open(os.path.join(ctx.out, "meta.json"), "w"), indent=2)
    out_f = open(os.path.join(ctx.out, "mem.jsonl"), "a")

    def emit(variant, rec):
        rec = dict(rec, variant=variant, fwd_cores=ctx.fwd_cores)
        out_f.write(json.dumps(rec) + "\n")
        out_f.flush()
        brief = {k: rec[k] for k in ("n", "anon_kb", "kb_per_conn", "startup_s", "errors", "established", "failed", "error") if k in rec}
        log(f"  {variant} {rec.get('test')}: {brief}")

    def attempt(variant, fn):
        try:
            emit(variant, fn())
        except Exception as e:  # noqa: BLE001
            emit(variant, {"test": "?", "error": str(e)[:300]})

    if "ports" in tests:
        for n in map(int, a.ports_n.split(",")):
            log(f"== idle memory, {n} TCP + {n} UDP ports")
            for vn in vnames:
                if vn in ("direct", "docker-nat"):
                    continue
                cls = None if vn == "docker-proxy" else VARIANTS[vn]
                attempt(vn, lambda: t_ports(ctx, cls, n))
    if "conns" in tests:
        for n in map(int, a.conns_n.split(",")):
            log(f"== {n} concurrent idle TCP connections")
            for vn in vnames:
                if vn in ("direct", "docker-nat"):
                    continue
                attempt(vn, lambda: t_hold(ctx, VARIANTS[vn], "tcp", n))
    if "conns_active" in tests:
        for n in map(int, a.conns_n.split(",")):
            log(f"== {n} concurrent TCP connections that each moved 1 MiB")
            for vn in vnames:
                if vn in ("direct", "docker-nat"):
                    continue
                attempt(vn, lambda: t_hold(ctx, VARIANTS[vn], "tcp", n, 1 << 20))
    if "udp" in tests:
        for n in map(int, a.udp_n.split(",")):
            log(f"== {n} concurrent UDP flows")
            for vn in vnames:
                if vn in ("direct", "docker-nat"):
                    continue
                attempt(vn, lambda: t_hold(ctx, VARIANTS[vn], "udp", n))
    if "soak" in tests:
        for vn in vnames:
            if vn in ("direct", "docker-nat", "docker-proxy"):
                continue
            log(f"== soak {a.soak_seconds}s: {vn}")
            attempt(vn, lambda: t_soak(ctx, VARIANTS[vn], a.soak_seconds))
    log("done ->", ctx.out)


if __name__ == "__main__":
    main()
