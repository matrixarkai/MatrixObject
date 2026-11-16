#!/usr/bin/env python3
"""Verb-level latency probe for an S3 door (in-process or cluster gateway).

usage: gateway_probe.py HOST:PORT BUCKET MODE [N] [SIZE]

MODES
  verbs   N objects of SIZE bytes: PUT, whole GET, ranged GET, HEAD, then a LIST of the prefix,
          then N appends of SIZE bytes to one key, then DELETE of every object.
  extents append 1 byte at a time to one key until the door refuses, and report the count at
          which it did and what it said.

Prints one line per measurement: name n p50_us p99_us (verbs) or a summary (extents).
Keep-alive: one connection per mode, as a log client holds one.
"""
import http.client
import statistics
import sys
import time


def pct(samples, p):
    s = sorted(samples)
    return s[min(len(s) - 1, max(0, (len(s) * p + 99) // 100 - 1))]


def main():
    host, bucket, mode = sys.argv[1], sys.argv[2], sys.argv[3]
    n = int(sys.argv[4]) if len(sys.argv) > 4 else 200
    size = int(sys.argv[5]) if len(sys.argv) > 5 else 4096
    run = "probe-%d" % int(time.time())
    conn = http.client.HTTPConnection(host, timeout=60)

    def call(method, path, body=None, headers=None):
        t0 = time.perf_counter()
        conn.request(method, path, body=body, headers=headers or {})
        r = conn.getresponse()
        data = r.read()
        return (time.perf_counter() - t0) * 1e6, r.status, data, r

    if mode == "extents":
        key = "/%s/%s/wal" % (bucket, run)
        offset = 0
        while True:
            _, status, data, _ = call("PUT", key, b"x", {"x-amz-write-offset-bytes": str(offset)})
            if status != 200:
                print("extents: refused append number %d (offset %d) with status %d" % (offset + 1, offset, status))
                print("extents: body: %s" % data.decode("utf-8", "replace")[:400])
                _, st, _, r = call("HEAD", key)
                print("extents: HEAD afterwards: status %d length %s" % (st, r.getheader("x-amz-object-size")))
                call("DELETE", key)
                return
            offset += 1
            if offset % 500 == 0:
                print("extents: %d appends so far" % offset, flush=True)
            if offset > 20000:
                print("extents: no refusal by 20000 appends")
                return

    payload = bytes((i * 31) & 0xFF for i in range(size))
    keys = ["/%s/%s/obj-%06d" % (bucket, run, i) for i in range(n)]
    lat = {k: [] for k in ("put", "get_whole", "get_range", "head", "append", "delete")}
    for k in keys:
        us, st, d, _ = call("PUT", k, payload)
        assert st == 200, (st, d[:200])
        lat["put"].append(us)
    for k in keys:
        us, st, d, _ = call("GET", k)
        assert st == 200 and d == payload, (st, len(d))
        lat["get_whole"].append(us)
    r0 = min(100, size // 4)
    for k in keys:
        us, st, d, _ = call("GET", k, headers={"Range": "bytes=%d-%d" % (r0, r0 + 255)})
        assert st == 206 and d == payload[r0:r0 + 256], (st, len(d))
        lat["get_range"].append(us)
    for k in keys:
        us, st, _, r = call("HEAD", k)
        assert st == 200 and r.getheader("x-amz-object-size") == str(size), st
        lat["head"].append(us)
    t0 = time.perf_counter()
    us, st, d, _ = call("GET", "/%s?list-type=2&prefix=%s/" % (bucket, run))
    assert st == 200 and d.count(b"<Key>") == n, (st, d.count(b"<Key>"))
    list_us = us
    akey = "/%s/%s/appendlog" % (bucket, run)
    off = 0
    for _ in range(n):
        us, st, d, _ = call("PUT", akey, payload, {"x-amz-write-offset-bytes": str(off)})
        assert st == 200, (st, d[:200])
        off += size
        lat["append"].append(us)
    us, st, d, _ = call("GET", akey)
    assert st == 200 and len(d) == n * size, (st, len(d))
    whole_log_us = us
    for k in keys + [akey]:
        us, st, _, _ = call("DELETE", k)
        assert st == 204, st
        lat["delete"].append(us)
    print("# host=%s mode=verbs n=%d size=%d" % (host, n, size))
    for name, samples in lat.items():
        print("%-10s n=%-5d p50_us=%-8d p99_us=%-8d" % (name, len(samples), pct(samples, 50), pct(samples, 99)))
    print("%-10s %d keys p50_us=%d" % ("list", n, list_us))
    print("%-10s %d bytes in %d appends p50_us=%d" % ("whole_log", n * size, n, whole_log_us))


if __name__ == "__main__":
    main()
