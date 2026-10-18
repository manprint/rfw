"""Shared plumbing for the rfw benchmark harness: shell/docker helpers, the
backend fleet, CPU + memory metering, and the forwarder variants.

Everything the harness starts is labelled `rfwbench=1` and cleaned up by label
only; nothing here touches containers, images, networks or sysctls it did not
create.
"""

import json
import os
import random
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)
LABEL = "rfwbench=1"
NET = "rfwb-net"

# Port plan. Listen ports stay below the kernel ephemeral range (32768+) so a
# source port can never collide with one of them.
LISTEN_BASE = 20000  # forwarder listen port = LISTEN_BASE + backend port
PUBLISH_BASE = 10000  # docker -p host port   = PUBLISH_BASE + backend port
ECHO_BASE = 7000
ECHO_PORTS = 32
METRICS_PORT = 19090

IMG_ALPINE = "alpine:3.22"
IMG_SOCAT = "alpine/socat:latest"
IMG_IPERF = "networkstatic/iperf3:latest"
IMG_REDIS = "redis:alpine"
IMG_PG = "postgres:17-alpine"
IMG_DNS = "coredns/coredns:latest"
IMG_RFW_RELEASED = "ghcr.io/manprint/rfw:v1.1.0"
IMG_RFW_LOCAL = "rfw-bench:local"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


# ------------------------------------------------------------ conntrack guard
# Loopback and bridge connections are tracked by the host's netfilter. Every
# closed connection lingers in TIME_WAIT for 120 s, so connection-churn tests can
# fill the (host-wide) table, at which point the kernel drops *all* new flows,
# including the workstation's own internet traffic. The harness therefore never
# lets the table go above a small fraction of its size.
CT_COUNT = "/proc/sys/net/netfilter/nf_conntrack_count"
CT_MAX = "/proc/sys/net/netfilter/nf_conntrack_max"
CT_IDLE_FRAC = 0.08   # start a scenario only when the table is below this fraction
CT_BUDGET_FRAC = 0.12  # one scenario may add at most this fraction (entries = 2 per connection)


def ct_max():
    try:
        return int(open(CT_MAX).read())
    except (OSError, ValueError):
        return None


def ct_count():
    try:
        return int(open(CT_COUNT).read())
    except (OSError, ValueError):
        return None


def ct_connections_budget():
    """Most connections one churn scenario may open (two tracked legs each)."""
    m = ct_max()
    return None if m is None else int(m * CT_BUDGET_FRAC / 2)


def ct_clamp(total):
    b = ct_connections_budget()
    return total if b is None else min(int(total), b)


def ct_wait(frac=CT_IDLE_FRAC, timeout=240):
    """Block until the conntrack table drained below `frac` of its size."""
    m, t0, said = ct_max(), time.time(), False
    if m is None:
        return
    while True:
        c = ct_count()
        if c is None or c <= m * frac:
            return
        if time.time() - t0 > timeout:
            raise RuntimeError(f"conntrack table not draining ({c}/{m}); refusing to continue")
        if not said:
            print(f"   conntrack {c}/{m}: waiting for TIME_WAIT entries to expire", flush=True)
            said = True
        time.sleep(3)


def sh(cmd, check=True, timeout=None, input=None, env=None):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, input=input, env=env)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed ({r.returncode}): {' '.join(cmd)}\n{r.stderr.strip()[-800:]}")
    return r


def docker(*args, check=True, timeout=None):
    return sh(["docker", *args], check=check, timeout=timeout)


def last_json(text):
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    return None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


# ---------------------------------------------------------------- host info

def primary_ip():
    try:
        out = sh("ip -4 route get 1.1.1.1", check=False).stdout
        m = re.search(r"src (\d+\.\d+\.\d+\.\d+)", out)
        if m:
            return m.group(1)
    except Exception:
        pass
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    finally:
        s.close()


def host_meta():
    def rd(p):
        try:
            return open(p).read().strip()
        except OSError:
            return None

    cpu = ""
    for line in open("/proc/cpuinfo"):
        if line.startswith("model name"):
            cpu = line.split(":", 1)[1].strip()
            break
    return {
        "cpu_model": cpu,
        "nproc": os.cpu_count(),
        "kernel": os.uname().release,
        "governor": rd("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        "mem_total_kb": int(re.search(r"MemTotal:\s+(\d+)", rd("/proc/meminfo")).group(1)),
        "docker": sh("docker --version", check=False).stdout.strip(),
        "pipe_user_pages_soft": rd("/proc/sys/fs/pipe-user-pages-soft"),
        "pipe_max_size": rd("/proc/sys/fs/pipe-max-size"),
        "rmem_max": rd("/proc/sys/net/core/rmem_max"),
        "wmem_max": rd("/proc/sys/net/core/wmem_max"),
        "ip_local_port_range": rd("/proc/sys/net/ipv4/ip_local_port_range"),
        "git_rev": sh("git rev-parse --short HEAD", check=False).stdout.strip(),
    }


def default_cpusets(nproc):
    """(forwarder, backend, client) cpu lists, avoiding SMT siblings where the
    layout allows. Overridable from the command line."""
    if nproc >= 16:
        return "0,2,4,6", "8,10", "12,13,14,15"
    if nproc >= 8:
        return "0,1,2,3", "4,5", "6,7"
    if nproc >= 4:
        return "0,1", "2", "3"
    return "0", "0", "0"


# ----------------------------------------------------------------- metering

def read_stat_cpu(pid):
    """utime+stime in seconds for a pid (0 if gone)."""
    try:
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, IndexError, ValueError):
        return 0.0


def system_cpu_busy():
    """Whole-machine non-idle CPU seconds."""
    f = open("/proc/stat").readline().split()[1:]
    v = [int(x) for x in f]
    idle = v[3] + (v[4] if len(v) > 4 else 0)
    return (sum(v[:8]) - idle) / os.sysconf("SC_CLK_TCK")


def cgroup_dir(cid):
    for p in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope", f"/sys/fs/cgroup/docker/{cid}"):
        if os.path.isdir(p):
            return p
    return None


def cgroup_cpu(cid):
    d = cgroup_dir(cid)
    if not d:
        return 0.0
    for line in open(f"{d}/cpu.stat"):
        if line.startswith("usage_usec"):
            return int(line.split()[1]) / 1e6
    return 0.0


def cgroup_mem(cid):
    """Container memory in KiB: anon (comparable to a native process' Pss_Anon),
    plus kernel+socket memory and memory.current for reference."""
    d = cgroup_dir(cid)
    if not d:
        return {}
    st = {}
    for line in open(f"{d}/memory.stat"):
        k, v = line.split()
        st[k] = int(v)
    cur = int(open(f"{d}/memory.current").read())
    return {
        "anon_kb": st.get("anon", 0) // 1024,
        "kernel_kb": (st.get("kernel", st.get("slab", 0) + st.get("kernel_stack", 0) + st.get("pagetables", 0))) // 1024,
        "sock_kb": st.get("sock", 0) // 1024,
        "current_kb": cur // 1024,
    }


def smaps_pss_anon(pid):
    try:
        for line in open(f"/proc/{pid}/smaps_rollup"):
            if line.startswith("Pss_Anon:"):
                return int(line.split()[1])
    except OSError:
        pass
    return 0


def root_pss_anon(pids):
    """Pss_Anon of root-owned processes (docker-proxy) via a privileged
    --pid=host helper container. Returns KiB summed over pids."""
    if not pids:
        return 0
    script = "t=0; for p in %s; do v=$(awk '/^Pss_Anon:/{print $2}' /proc/$p/smaps_rollup 2>/dev/null); t=$((t+${v:-0})); done; echo $t" % " ".join(
        str(p) for p in pids
    )
    r = docker("run", "--rm", "--pid=host", "--privileged", "--label", LABEL, IMG_ALPINE, "sh", "-c", script, check=False)
    try:
        return int(r.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return 0


def proxy_pids(host_ports):
    """PIDs of docker-proxy processes serving any of `host_ports`."""
    want = {str(p) for p in host_ports}
    out = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            cmd = open(f"/proc/{d}/cmdline", "rb").read().split(b"\0")
        except OSError:
            continue
        if cmd and os.path.basename(cmd[0].decode(errors="ignore")) == "docker-proxy":
            try:
                hp = cmd[cmd.index(b"-host-port") + 1].decode()
            except (ValueError, IndexError):
                continue
            if hp in want:
                out.append(int(d))
    return out


def tcp_port_open(host, port, timeout=0.5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def listening_ports():
    out = sh("ss -H -lntu", check=False).stdout
    ports = set()
    for line in out.splitlines():
        m = re.search(r":(\d+)\s", line.split()[4] + " ")
        if m:
            ports.add(int(m.group(1)))
    return ports


# ------------------------------------------------------------ backend fleet

class Fleet:
    """Backend containers on a private bridge.

    The main fleet is *not* published: forwarders reach it by container IP. The
    `docker -p` variants build their own published fleet (`published=True`) when
    they start and remove it when they stop, because docker-proxy is a root
    process that keeps pooled pipes after carrying traffic and so exhausts the
    uid-0 pipe budget (`fs.pipe-user-pages-soft`) for every container that runs
    afterwards; see docs/benchmarks. Removing the containers ends the proxies."""

    def __init__(self, bench_bin, outdir, cpus_backend, with_db=True, with_dns=True, published=False, prefix="rfwb-"):
        self.bin = bench_bin
        self.out = outdir
        self.cpus = cpus_backend
        self.with_db = with_db
        self.with_dns = with_dns
        self.published = published
        self.prefix = prefix
        self.ip = {}
        self.ids = {}

    def _run(self, name, image, *args, publish=(), extra=()):
        a = ["run", "-d", "--name", f"{self.prefix}{name}", "--label", LABEL, "--network", NET, "--cpuset-cpus", self.cpus,
             "--ulimit", "nofile=1048576:1048576"]
        if self.published:
            for p in publish:
                a += ["-p", p]
        a += list(extra) + [image, *args]
        cid = docker(*a).stdout.strip()
        self.ids[name] = cid
        self.ip[name] = docker("inspect", "-f", "{{(index .NetworkSettings.Networks \"%s\").IPAddress}}" % NET, cid).stdout.strip()
        return cid

    def _addr(self, svc, bport):
        return ("127.0.0.1", PUBLISH_BASE + bport) if self.published else (self.ip[svc], bport)

    def start(self):
        for img in (IMG_ALPINE, IMG_SOCAT, IMG_IPERF, IMG_REDIS, IMG_PG, IMG_DNS):
            if docker("image", "inspect", img, check=False).returncode != 0:
                log("pulling", img)
                docker("pull", "-q", img)
        if docker("network", "inspect", NET, check=False).returncode != 0:
            docker("network", "create", "--label", LABEL, NET)
        e = ECHO_BASE
        n = ECHO_PORTS
        pub = [f"{PUBLISH_BASE + e}-{PUBLISH_BASE + e + n - 1}:{e}-{e + n - 1}/tcp",
               f"{PUBLISH_BASE + e}-{PUBLISH_BASE + e + n - 1}:{e}-{e + n - 1}/udp"]
        nthreads = max(1, len(self.cpus.split(",")))
        self._run("echo", IMG_ALPINE, "/rfwbench", "echo", "--base", str(e), "--ports", str(n), "--threads", str(nthreads),
                  publish=pub, extra=["-v", f"{self.bin}:/rfwbench:ro"])
        self._run("iperf", IMG_IPERF, "-s", publish=[f"{PUBLISH_BASE + 5201}:5201/tcp", f"{PUBLISH_BASE + 5201}:5201/udp"])
        if self.with_db:
            self._run("redis", IMG_REDIS, "redis-server", "--save", "", "--appendonly", "no",
                      publish=[f"{PUBLISH_BASE + 6379}:6379/tcp"])
            self._run("pg", IMG_PG, "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "max_connections=200",
                      publish=[f"{PUBLISH_BASE + 5432}:5432/tcp"],
                      extra=["-e", "POSTGRES_PASSWORD=bench", "--tmpfs", "/var/lib/postgresql/data"])
        if self.with_dns:
            corefile = os.path.join(self.out, "Corefile")
            open(corefile, "w").write(".:53 {\n  whoami\n}\n")
            self._run("dns", IMG_DNS, "-conf", "/Corefile", publish=[f"{PUBLISH_BASE + 53}:53/udp"],
                      extra=["-v", f"{corefile}:/Corefile:ro"])
        # wait for readiness
        probes = [self._addr("echo", e), self._addr("iperf", 5201)]
        if self.with_db:
            probes += [self._addr("pg", 5432), self._addr("redis", 6379)]
        deadline = time.time() + 90
        while time.time() < deadline:
            if all(tcp_port_open(h, p) for h, p in probes):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("backends did not come up")
        if self.with_db:
            self._wait_pg()
            if self.published:
                self.init_pgbench()

    def stop(self):
        ids = list(self.ids.values())
        if ids:
            docker("rm", "-f", *ids, check=False)
        self.ids = {}

    def _wait_pg(self):
        for _ in range(60):
            r = docker("exec", self.ids["pg"], "pg_isready", "-U", "postgres", check=False)
            if r.returncode == 0:
                time.sleep(1)
                return
            time.sleep(1)
        raise RuntimeError("postgres not ready")

    def init_pgbench(self, scale=5):
        docker("exec", "-e", "PGPASSWORD=bench", self.ids["pg"], "pgbench", "-i", "-q", "-s", str(scale), "-U", "postgres", "postgres",
               timeout=300)

    def service_map(self):
        """[(proto, backend_ip, backend_port)] for every forwarded service."""
        m = []
        for p in range(ECHO_BASE, ECHO_BASE + ECHO_PORTS):
            m += [("tcp", self.ip["echo"], p), ("udp", self.ip["echo"], p)]
        m += [("tcp", self.ip["iperf"], 5201), ("udp", self.ip["iperf"], 5201)]
        if self.with_db:
            m += [("tcp", self.ip["redis"], 6379), ("tcp", self.ip["pg"], 5432)]
        if self.with_dns:
            m += [("udp", self.ip["dns"], 53)]
        return m

    def published_ports(self):
        return sorted({PUBLISH_BASE + p for _, _, p in self.service_map()})


def cleanup_all():
    ids = docker("ps", "-aq", "--filter", f"label={LABEL}", check=False).stdout.split()
    if ids:
        docker("rm", "-f", *ids, check=False)
    docker("network", "rm", NET, check=False)


# ----------------------------------------------------------------- variants

def specs(ctx):
    """Forwarder list [(proto, listen_port, backend_ip, backend_port)];
    `ctx.spec_override` lets the memory tests ask for N ports instead."""
    if getattr(ctx, "spec_override", None) is not None:
        return ctx.spec_override
    return [(proto, LISTEN_BASE + bp, ip, bp) for proto, ip, bp in ctx.fleet.service_map()]


class Variant:
    """A forwarder under test. `ep(port)` is where clients connect for the
    backend service `port`; `cpu()` is cumulative forwarder CPU seconds (None
    when it cannot be attributed, e.g. kernel NAT); `mem()` is anon memory."""

    name = "?"
    desc = ""
    kind = "forwarder"  # forwarder | baseline | ceiling

    def __init__(self, ctx):
        self.ctx = ctx

    def start(self):
        pass

    def stop(self):
        pass

    def ep(self, bport, proto="tcp"):
        raise NotImplementedError

    def cpu(self):
        return None

    def mem(self):
        return {}

    def stats(self):
        return None


class Direct(Variant):
    name = "direct"
    desc = "client -> backend container IP (no forwarder; baseline)"
    kind = "baseline"

    def ep(self, bport, proto="tcp"):
        f = self.ctx.fleet
        for p, ip, bp in f.service_map():
            if bp == bport:
                return ip, bport
        raise KeyError(bport)


def published_fleet(ctx):
    f = ctx.fleet
    pf = Fleet(f.bin, f.out, f.cpus, with_db=f.with_db, with_dns=f.with_dns, published=True, prefix="rfwb-pub-")
    pf.start()
    return pf


class DockerNat(Variant):
    name = "docker-nat"
    desc = "docker -p, kernel DNAT/conntrack path (client -> host IP)"
    kind = "ceiling"

    def start(self):
        self.pub = published_fleet(self.ctx)

    def stop(self):
        self.pub.stop()

    def ep(self, bport, proto="tcp"):
        return self.ctx.host_ip, PUBLISH_BASE + bport


class DockerProxy(Variant):
    name = "docker-proxy"
    desc = "docker -p, userland docker-proxy (client -> 127.0.0.1)"

    def start(self):
        self.pub = published_fleet(self.ctx)
        self.pids = proxy_pids(self.pub.published_ports())
        if not self.pids:
            raise RuntimeError("no docker-proxy processes found (userland-proxy disabled?)")
        if self.ctx.pin_proxy:
            # Docker starts docker-proxy unpinned; pin it to the forwarder CPUs so
            # it gets the same CPU budget as every other userland forwarder.
            r = sh(["docker", "run", "--rm", "--privileged", "--pid=host", "--label", LABEL, "-v",
                    f"{self.ctx.bench_bin}:/rfwbench:ro", IMG_ALPINE, "/rfwbench", "pin", "--cpus", self.ctx.cpus_fwd,
                    "--pids", ",".join(map(str, self.pids))], check=False)
            j = last_json(r.stdout) or {}
            if not j.get("pinned_threads") or j.get("failed"):
                log("  WARNING: docker-proxy pinning incomplete:", r.stdout.strip(), r.stderr.strip()[-200:])
        self.base = {p: read_stat_cpu(p) for p in self.pids}

    def stop(self):
        self.pub.stop()

    def ep(self, bport, proto="tcp"):
        return "127.0.0.1", PUBLISH_BASE + bport

    def cpu(self):
        return sum(read_stat_cpu(p) for p in self.pids)

    def mem(self):
        return {"anon_kb": root_pss_anon(self.pids), "procs": len(self.pids)}


class Socat(Variant):
    name = "socat"
    desc = "socat in alpine/socat container, --network host, as deployed (fork,reuseaddr)"
    opts = ""

    def start(self):
        lines = []
        for proto, lp, ip, bp in specs(self.ctx):
            if proto == "tcp":
                lines.append(f"socat tcp-listen:{lp},fork,reuseaddr{self.opts} tcp-connect:{ip}:{bp}{self.opts} &")
            else:
                lines.append(f"socat udp-listen:{lp},fork,reuseaddr udp:{ip}:{bp} &")
        script = "\n".join(lines) + "\nwait\n"
        self.cid = docker("run", "-d", "--name", "rfwb-fwd", "--label", LABEL, "--network", "host",
                          "--cpuset-cpus", self.ctx.cpus_fwd, "--ulimit", "nofile=1048576:1048576",
                          "--entrypoint", "sh", IMG_SOCAT, "-c", script).stdout.strip()

    def stop(self):
        docker("rm", "-f", "rfwb-fwd", check=False)

    def ep(self, bport, proto="tcp"):
        return "127.0.0.1", LISTEN_BASE + bport

    def cpu(self):
        return cgroup_cpu(self.cid)

    def mem(self):
        return cgroup_mem(self.cid)


class SocatTuned(Socat):
    name = "socat-nodelay"
    desc = "socat with nodelay on both legs (tuned variant, not the deployed command)"
    opts = ",nodelay"


class RfwNative(Variant):
    name = "rfw"
    desc = "rfw release binary on the host (default settings)"
    extra = []
    env = {}

    def start(self):
        args = [self.ctx.rfw_bin, "--log-dir", "", "--metrics-addr", f"127.0.0.1:{METRICS_PORT}", *self.extra]
        for proto, lp, ip, bp in specs(self.ctx):
            args.append(f"0.0.0.0:{lp}:{ip}:{bp}/{proto}")
        env = dict(os.environ, RUST_LOG="warn", **self.env)

        def lim():
            import resource
            resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))

        self.proc = subprocess.Popen(["taskset", "-c", self.ctx.cpus_fwd, *args], env=env, preexec_fn=lim,
                                     stdout=subprocess.DEVNULL, stderr=open(os.path.join(self.ctx.out, f"{self.name}.log"), "w"))

    def stop(self):
        if getattr(self, "proc", None):
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def ep(self, bport, proto="tcp"):
        return "127.0.0.1", LISTEN_BASE + bport

    def cpu(self):
        return read_stat_cpu(self.proc.pid)

    def mem(self):
        return {"anon_kb": smaps_pss_anon(self.proc.pid)}

    def stats(self):
        return fetch_stats(f"127.0.0.1:{METRICS_PORT}")


class RfwSplice(RfwNative):
    name = "rfw-splice"
    desc = "rfw --splice (zero-copy TCP path)"
    extra = ["--splice"]


class RfwWorkers1(RfwNative):
    name = "rfw-w1"
    desc = "rfw with TOKIO_WORKER_THREADS=1 (experiment)"
    env = {"TOKIO_WORKER_THREADS": "1"}


class RfwWorkers4(RfwNative):
    name = "rfw-w4"
    desc = "rfw with TOKIO_WORKER_THREADS=4 on 2 CPUs (experiment)"
    env = {"TOKIO_WORKER_THREADS": "4"}


class RfwGro(RfwNative):
    name = "rfw-gro"
    desc = "rfw --udp-gro-gso"
    extra = ["--udp-gro-gso"]


class RfwDocker(Variant):
    name = "rfw-d-host"
    desc = "rfw image, docker run --network host (production recipe)"
    docker_flags = []
    extra = []
    bridge = False
    image = None  # default: local build
    ulimit = "nofile=1048576:1048576"  # None = leave Docker's default (soft 1024)

    def start(self):
        image = self.image or IMG_RFW_LOCAL
        args = ["run", "-d", "--name", "rfwb-fwd", "--label", LABEL, "--cpuset-cpus", self.ctx.cpus_fwd,
                "-e", "RUST_LOG=warn", *self.docker_flags]
        if self.ulimit:
            args += ["--ulimit", self.ulimit]
        if self.bridge:
            args += ["--network", NET]
            for proto, lp, ip, bp in specs(self.ctx):
                args += ["-p", f"{lp}:{lp}/{proto}"]
            args += ["-p", f"{METRICS_PORT}:{METRICS_PORT}/tcp"]
            mhost = "0.0.0.0"
        else:
            args += ["--network", "host"]
            mhost = "127.0.0.1"
        args += [image, "--log-dir", "", "--metrics-addr", f"{mhost}:{METRICS_PORT}", *self.extra]
        for proto, lp, ip, bp in specs(self.ctx):
            args.append(f"0.0.0.0:{lp}:{ip}:{bp}/{proto}")
        self.cid = docker(*args).stdout.strip()

    def stop(self):
        docker("rm", "-f", "rfwb-fwd", check=False)

    def ep(self, bport, proto="tcp"):
        return (self.ctx.host_ip if self.bridge else "127.0.0.1"), LISTEN_BASE + bport

    def cpu(self):
        return cgroup_cpu(self.cid)

    def mem(self):
        return cgroup_mem(self.cid)

    def stats(self):
        return fetch_stats(f"127.0.0.1:{METRICS_PORT}")


class RfwDockerSplice(RfwDocker):
    name = "rfw-d-host-splice"
    desc = "rfw image, host network, --splice"
    extra = ["--splice"]


class RfwDockerUnconfined(RfwDocker):
    name = "rfw-d-unconfined"
    desc = "rfw image, host network, seccomp=unconfined"
    docker_flags = ["--security-opt", "seccomp=unconfined"]


class RfwDockerPrivileged(RfwDocker):
    name = "rfw-d-privileged"
    desc = "rfw image, host network, --privileged"
    docker_flags = ["--privileged"]


class RfwDockerSysRes(RfwDocker):
    name = "rfw-d-splice-sysres"
    desc = "rfw image, host network, --splice, --cap-add SYS_RESOURCE"
    docker_flags = ["--cap-add", "SYS_RESOURCE"]
    extra = ["--splice"]


class RfwDockerBridge(RfwDocker):
    name = "rfw-d-bridge"
    desc = "rfw image on a bridge with -p (compose layout: DNAT + rfw; double hop)"
    bridge = True


class RfwDockerDefaultUlimit(RfwDocker):
    name = "rfw-d-default-ulimit"
    desc = "rfw image, host network, Docker's default nofile limit (no --ulimit)"
    ulimit = None


class RfwDockerCompose(RfwDocker):
    name = "rfw-d-compose"
    desc = "rfw image with the docker-compose.yml budget: 128 MiB memory limit, --max-connections 512"
    docker_flags = ["--memory", "128m", "--memory-swap", "128m"]
    extra = ["--max-connections", "512"]


class RfwDockerReleased(RfwDocker):
    name = "rfw-d-v1.1.0"
    desc = "published ghcr.io/manprint/rfw:v1.1.0 image, host network"
    image = IMG_RFW_RELEASED


class RfwDockerReleasedSplice(RfwDockerReleased):
    name = "rfw-d-v1.1.0-splice"
    desc = "published v1.1.0 image with --splice (before the pipe-capacity fallback)"
    extra = ["--splice"]


ALL_VARIANTS = [Direct, DockerNat, DockerProxy, Socat, SocatTuned, RfwNative, RfwSplice, RfwWorkers1, RfwWorkers4, RfwGro, RfwDocker,
                RfwDockerSplice, RfwDockerUnconfined, RfwDockerPrivileged, RfwDockerSysRes, RfwDockerBridge,
                RfwDockerDefaultUlimit, RfwDockerCompose, RfwDockerReleased, RfwDockerReleasedSplice]
VARIANTS = {v.name: v for v in ALL_VARIANTS}


def fetch_stats(addr):
    try:
        with urllib.request.urlopen(f"http://{addr}/stats", timeout=3) as r:
            return json.load(r)
    except Exception:
        return None


def wait_variant_ready(v, timeout=60):
    """TCP connect on the first echo port, then one UDP echo round trip."""
    host, port = v.ep(ECHO_BASE)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if tcp_port_open(host, port):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"{v.name}: TCP port {host}:{port} never opened")
    # every echo port and UDP path must answer
    last = host, ECHO_BASE + ECHO_PORTS - 1
    while time.time() < deadline:
        if tcp_port_open(*v.ep(ECHO_BASE + ECHO_PORTS - 1)):
            break
        time.sleep(0.2)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.5)
    uh, up = v.ep(ECHO_BASE, "udp")
    while time.time() < deadline:
        try:
            s.sendto(b"ping-rfwbench", (uh, up))
            if s.recv(64) == b"ping-rfwbench":
                s.close()
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"{v.name}: UDP echo never answered")


def stats_sum(stats, key, proto=None):
    """Sum a numeric field over the /stats array (optionally one protocol)."""
    if not isinstance(stats, list):
        return None
    try:
        return sum(int(f.get(key, 0)) for f in stats if proto is None or f.get("protocol") == proto)
    except (TypeError, ValueError):
        return None


def shuffled_rotation(items, rep):
    k = rep % len(items)
    out = items[k:] + items[:k]
    if rep % 2 == 1:
        out = list(reversed(out))
    return out


def ensure_local_image(src_dir):
    """Build the production Dockerfile (what people deploy) as rfw-bench:local."""
    log("building", IMG_RFW_LOCAL, "from the repo Dockerfile")
    docker("build", "-q", "-t", IMG_RFW_LOCAL, "--label", LABEL, src_dir, timeout=1800)
