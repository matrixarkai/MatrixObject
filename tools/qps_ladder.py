#!/usr/bin/env python3
"""Knee sweeps on one cluster.  usage: qps_ladder.py TAG MODE [conc,...]   (env as qps_rig.py: BIN RUN BASE_PORT THREADS ...)

MODE write : appends, 1 KiB, rate doubling until the knee (goodput < 90% of offered, or p99 > 1 s, or errors)
MODE read  : ranged reads (4 KiB) of populated objects, same ladder
MODE mix   : 90/10 read/write ladder
MODE conn  : fixed light read rate at each conc (connection-model before/after) + idle CPU
Each step is 15 s measured after 3 s warm-up.  Results append to results.jsonl via qps_rig.py (label TAG-MODE-cC-rR).
Environment: OBJECTS, VALUE, SECS, LADDER (comma list of offered rates), DIST (e.g. hot:10:1.0), P50MAX / P99MAX (ms: the knee rule), BUCKET.
"""
import json, os, subprocess, sys, time

TAG, MODE = sys.argv[1], sys.argv[2]
CONCS = [int(x) for x in (sys.argv[3] if len(sys.argv) > 3 else "1,8,64,256").split(",")]
HERE = os.path.dirname(os.path.abspath(__file__))
OBJECTS = int(os.environ.get("OBJECTS", "1000"))
VALUE = int(os.environ.get("VALUE", "1024"))
SECS = os.environ.get("SECS", "15")
LADDER = [int(x) for x in os.environ.get("LADDER", "25,50,100,200,400,800,1600,3200,6400").split(",")]
BASE = int(os.environ.get("BASE_PORT", "22000"))
LOADGEN = os.environ.get("LOADGEN", os.path.join(os.environ.get("BIN", os.path.join(os.environ.get("CARGO_TARGET_DIR", "target"), "release")), "examples", "qps_http"))
BUCKET = os.environ.get("BUCKET", "temporalstore")


def step(label, args):
    r = subprocess.run(["python3", HERE + "/qps_rig.py", "step", label] + args, capture_output=True, text=True)
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except Exception:
        return {"error": r.stdout[-300:] + r.stderr[-300:]}


def populate(per_object=4):
    subprocess.run([LOADGEN, "populate", "--addr", "127.0.0.1:%d" % (BASE + 10), "--bucket", BUCKET, "--objects", str(OBJECTS),
                    "--per-object", str(per_object), "--value", str(VALUE), "--conc", "16", "--label", TAG + "-populate"],
                   check=False)


def good(res, rate):
    if "error" in res:
        return False
    done = res.get("goodput_ops", res.get("completed", 0) / max(1, float(SECS)))
    return done >= 0.9 * rate and res.get("p50_ms", 0) < float(os.environ.get("P50MAX", "1000")) and res.get("p99_ms", 0) < float(os.environ.get("P99MAX", "1000")) and res.get("err_total", 0) <= 0.02 * max(1, res.get("completed", 0))


def ladder(mode, conc):
    frac = {"write": "0", "read": "1", "mix": "0.9"}[mode]
    extra = ["--rsize", "4096"] if mode != "write" else []
    last_ok = None
    for rate in LADDER:
        label = "%s-%s-c%d-r%d" % (TAG, mode, conc, rate)
        res = step(label, ["--objects", str(OBJECTS), "--rate", str(rate), "--secs", SECS, "--conc", str(conc),
                           "--value", str(VALUE), "--read-frac", frac, "--warm", "3"] + extra + (["--dist", os.environ["DIST"]] if os.environ.get("DIST") else []))
        print(label, json.dumps({k: res.get(k) for k in ("p50_ms", "p99_ms", "p999_ms", "goodput_ops", "err_total", "class")}), flush=True)
        if not good(res, rate):
            break
        last_ok = rate
    print("KNEE %s %s conc=%d last_ok_rate=%s" % (TAG, mode, conc, last_ok), flush=True)


if MODE == "conn":
    populate()
    for conc in CONCS:
        label = "%s-conn-c%d" % (TAG, conc)
        res = step(label, ["--objects", str(OBJECTS), "--rate", "200", "--secs", SECS, "--conc", str(conc), "--value", str(VALUE),
                           "--read-frac", "1", "--rsize", "4096", "--warm", "3"])
        print(label, json.dumps({k: res.get(k) for k in ("p50_ms", "p99_ms", "p999_ms", "goodput_ops", "err_total", "class")}), flush=True)
else:
    populate()
    for conc in CONCS:
        ladder(MODE, conc)
