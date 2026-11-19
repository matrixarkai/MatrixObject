#!/usr/bin/env python3
"""Rig driver: bring up / take down OUR cluster, and run one measured step with resource attribution.

  qps_rig.py up                      bring up a raft3 cluster (ports BASE..BASE+23), gateway on BASE+10
  qps_rig.py down                    stop only what `up` started (pids file)
  qps_rig.py step LABEL LOADGEN-ARGS...   snapshot, run the load generator, snapshot, print one JSON line and
                                  append it to $OUT/results.jsonl
  qps_rig.py snap                    print the process/device snapshot as JSON

The load generator is `examples/qps_http.rs` (build it with `cargo build --release --example qps_http`).
Environment: BIN (default $CARGO_TARGET_DIR/release, or target/release), RUN (default <tmp>/qps_run),
BASE_PORT (22000), META (raft3), THREADS (gateway threads, 64), MAX_PAGES, CHECKPOINT_BYTES, GATEWAY_ENV,
OUT (default ./qps_results), LOADGEN (default $BIN/examples/qps_http), DISK (block device name for the
device counters, e.g. sda; unset = no device counters), HB_NOTE (a note recorded with every result, e.g. the
heartbeat setting under test).
Box class: quiet load1 < 4, moderate 4-10, contended > 10 (worst of before/after).
"""
import json, os, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("BIN", os.path.join(os.environ.get("CARGO_TARGET_DIR", "target"), "release"))
RUN = os.environ.get("RUN", os.path.join(tempfile.gettempdir(), "qps_run"))
BASE = int(os.environ.get("BASE_PORT", "22000"))
OUT = os.environ.get("OUT", "qps_results")
LOADGEN = os.environ.get("LOADGEN", os.path.join(BIN, "examples", "qps_http"))
DISK = os.environ.get("DISK", "")
BRINGUP = os.environ.get("BRINGUP", os.path.join(HERE, "..", "scripts", "cluster_gateway_bringup.sh"))
os.makedirs(OUT, exist_ok=True)
TICK = os.sysconf("SC_CLK_TCK")


def pids():
    out = {}
    try:
        for line in open(os.path.join(RUN, "pids")):
            n, p = line.split()
            out[n] = int(p)
    except OSError:
        pass
    return out


def cpu_s(pid):
    try:
        f = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / TICK, int(f[19])
    except OSError:
        return 0.0, 0


def rss_mb(pid):
    try:
        for l in open("/proc/%d/status" % pid):
            if l.startswith("VmRSS"):
                return int(l.split()[1]) / 1024
    except OSError:
        pass
    return 0.0


def io(pid):
    d = {}
    try:
        for l in open("/proc/%d/io" % pid):
            k, v = l.split(":")
            d[k] = int(v)
    except OSError:
        pass
    return d


def ctx(pid):
    v = n = 0
    try:
        for l in open("/proc/%d/status" % pid):
            if l.startswith("voluntary_ctxt_switches"):
                v = int(l.split()[1])
            if l.startswith("nonvoluntary_ctxt_switches"):
                n = int(l.split()[1])
    except OSError:
        pass
    return v, n


def diskstats():
    for l in open("/proc/diskstats"):
        f = l.split()
        if f[2] == DISK:
            return {"writes": int(f[7]), "wsectors": int(f[9]), "flushes": int(f[19]) if len(f) > 19 else -1}
    return {}


def lo_bytes():
    for l in open("/proc/net/dev"):
        if l.strip().startswith("lo:"):
            f = l.split(":")[1].split()
            return int(f[0]), int(f[8])
    return 0, 0


def du(path):
    t = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                t += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return t


def psi(kind="cpu"):
    try:
        l = open("/proc/pressure/" + kind).readline()
        return float(l.split("avg10=")[1].split()[0])
    except Exception:
        return -1.0


def load1():
    return float(open("/proc/loadavg").read().split()[0])


def snap():
    p = pids()
    s = {"t": time.time(), "load1": load1(), "psi_cpu": psi("cpu"), "psi_io": psi("io"), "disk": diskstats(), "lo": lo_bytes(), "proc": {}}
    for n, pid in p.items():
        c, _ = cpu_s(pid)
        s["proc"][n] = {"cpu": c, "io": io(pid), "rss_mb": rss_mb(pid), "ctx": ctx(pid)}
    s["meta_bytes"] = sum(du(os.path.join(RUN, "meta%d" % i)) for i in (1, 2, 3))
    s["chunk_bytes"] = sum(du(os.path.join(RUN, "chunk%d" % i)) for i in (1, 2, 3))
    return s


def klass(l):
    return "quiet" if l < 4 else ("moderate" if l <= 10 else "contended")


def up():
    if not pids() and os.path.isdir(RUN) and os.path.basename(RUN).startswith("qps_"):
        import shutil
        shutil.rmtree(RUN, ignore_errors=True)  # only a run dir named qps_*, only when nothing is running
    env = dict(os.environ, BIN=BIN, RUN=RUN, BASE_PORT=str(BASE), META=os.environ.get("META", "raft3"),
               THREADS=os.environ.get("THREADS", "64"), MAX_PAGES=os.environ.get("MAX_PAGES", "2000000"))
    r = subprocess.run(["bash", BRINGUP, "up"], env=env, capture_output=True, text=True)
    print(r.stdout + r.stderr)
    sys.exit(r.returncode)


def down():
    env = dict(os.environ, BIN=BIN, RUN=RUN, BASE_PORT=str(BASE))
    r = subprocess.run(["bash", BRINGUP, "down"], env=env, capture_output=True, text=True)
    print(r.stdout + r.stderr)


def step(label, args):
    a = snap()
    cmd = [LOADGEN, "run", "--addr", "127.0.0.1:%d" % (BASE + 10), "--label", label] + args
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True)
    b = snap()
    dt = b["t"] - a["t"]
    try:
        res = json.loads(r.stdout.strip().splitlines()[-1])
    except Exception:
        res = {"error": "loadgen failed", "stdout": r.stdout[-500:], "stderr": r.stderr[-800:]}
    res["load_before"], res["load_after"] = a["load1"], b["load1"]
    res["psi_cpu_after"] = b["psi_cpu"]
    res["class"] = klass(max(a["load1"], b["load1"]))
    # resource attribution over the step (includes warm-up and drain; wall = dt)
    attrib = {}
    for n, x in b["proc"].items():
        y = a["proc"].get(n)
        if not y:
            continue
        di = {k: x["io"].get(k, 0) - y["io"].get(k, 0) for k in ("wchar", "write_bytes", "syscw", "rchar", "read_bytes")}
        attrib[n] = {"cpu_pct": round(100 * (x["cpu"] - y["cpu"]) / dt, 1), "wMB_s": round(di["wchar"] / 1048576 / dt, 2),
                     "disk_wMB_s": round(di["write_bytes"] / 1048576 / dt, 2), "syscw_s": round(di["syscw"] / dt),
                     "vol_cs_s": round((x["ctx"][0] - y["ctx"][0]) / dt), "rss_mb": round(x["rss_mb"])}
    res["proc"] = attrib
    dd = {k: b["disk"].get(k, 0) - a["disk"].get(k, 0) for k in b["disk"]}
    res["disk"] = {"write_ios_s": round(dd.get("writes", 0) / dt), "flushes_s": round(dd.get("flushes", 0) / dt),
                   "MB_s": round(dd.get("wsectors", 0) * 512 / 1048576 / dt, 2)}
    res["lo_MB_s"] = round((b["lo"][1] - a["lo"][1]) / 1048576 / dt, 1)
    res["meta_growth_MB"] = round((b["meta_bytes"] - a["meta_bytes"]) / 1048576, 1)
    res["chunk_growth_MB"] = round((b["chunk_bytes"] - a["chunk_bytes"]) / 1048576, 1)
    # write amplification: bytes written (wchar) per user byte appended
    ops = res.get("completed", 0) * (1 - res.get("read_frac", 0)) if "completed" in res else 0
    ub = ops * res.get("value", 0)
    if ub > 0:
        mw = sum(attrib[n]["wMB_s"] for n in attrib if n.startswith("meta")) * dt * 1048576
        cw = sum(attrib[n]["wMB_s"] for n in attrib if n.startswith("chunk")) * dt * 1048576
        res["amp"] = {"meta_wchar_per_user_B": round(mw / ub, 2), "chunk_wchar_per_user_B": round(cw / ub, 2),
                      "meta_disk_growth_per_user_B": round(res["meta_growth_MB"] * 1048576 / ub, 2),
                      "chunk_disk_growth_per_user_B": round(res["chunk_growth_MB"] * 1048576 / ub, 2)}
    res["step_s"] = round(dt, 1)
    res["note"] = os.environ.get("HB_NOTE", "")
    line = json.dumps(res)
    print(line, flush=True)
    with open(os.path.join(OUT, "results.jsonl"), "a") as f:
        f.write(line + "\n")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "up":
        up()
    elif cmd == "down":
        down()
    elif cmd == "step":
        step(sys.argv[2], sys.argv[3:])
    elif cmd == "snap":
        print(json.dumps(snap()))
    else:
        sys.exit("usage")
