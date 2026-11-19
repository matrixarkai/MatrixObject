# Measuring throughput and latency of a cluster

`examples/qps_http.rs` is an open-loop load generator for the S3 door, `examples/qps_direct.rs` is the same
for the SDK (no gateway), and `tools/qps_rig.py` and `tools/qps_ladder.py` bring a cluster up, run a
measured step with per-process resource attribution, and search for the offered rate at which a cluster stops
keeping up.

## Method

- **Open loop, latency from the intended send time.** A scheduler emits requests at a fixed offered rate and a
  pool of workers (the maximum in flight) executes them. A request's latency is measured from when it was due,
  not from when a worker picked it up, so a stall shows up as latency on every request behind it instead of
  being hidden by a client that slowed down (coordinated omission). `queue_max` is the backlog at its worst.
- **Per-second series.** Every result carries `series`, `second:completed:worst-latency-ms` per intended second.
  A stall shows as a second with a huge latency followed by a backlog draining at about one second per second;
  a slow request path shows as latencies that are high in every second. Read it before trusting a p99.
- **Resource attribution.** `qps_rig.py step` snapshots every process the rig started (CPU, `/proc/<pid>/io`
  `write_bytes` and `wchar`, syscalls, RSS) before and after, plus the device counters if `DISK` is set, and adds the
  load class of the box: quiet below load 4, moderate 4 - 10, contended above 10. Compare only points of the same
  class; a busy host makes multi-second stalls that look like cluster behaviour.
- **Heartbeat and TTL.** A cluster is only measured at the product defaults if the bringup uses them
  (`scripts/cluster_gateway_bringup.sh` does: chunk heartbeat 3000 ms, metadata TTL 15000 ms). A TTL under about
  three heartbeat intervals expires a node on one late heartbeat and placement refuses writes for seconds.

## Running it

```
cargo build --release --features metadata-consensus --bins --example qps_http --example qps_direct
python3 tools/qps_rig.py up                         # three voters, three chunkservers, gateway on port 22010
target/release/examples/qps_http populate --addr 127.0.0.1:22010 --bucket temporalstore --objects 1000 --per-object 4 --value 1024 --conc 16
python3 tools/qps_rig.py step my-run --objects 1000 --rate 200 --secs 15 --conc 8 --value 1024 --read-frac 0
python3 tools/qps_ladder.py my-ladder write 1,8,64       # doubling offered rates until the cluster stops keeping up
python3 tools/qps_rig.py down
```

`qps_http run` options: `--rate` (offered per second), `--secs`, `--warm`, `--conc` (workers = most in flight),
`--read-frac` (0 = appends, 1 = ranged reads), `--rsize`, `--dist uniform|hot:N:F`, `--poisson`, `--prefix`, `--first`
and `--label`. Each run prints one JSON line (`p50_ms` ... `p999_ms`, per-kind latency, service time, goodput, errors,
`slow_secs_gt1s`, `series`). `qps_rig.py step` adds `proc`, `disk`, `class` and `amp` (bytes written per user byte,
from `write_bytes` and from directory growth).

## What to know when reading a result

- A knee found with a p99 limit on a busy box is mostly noise: `qps_ladder.py` takes `P50MAX` and `P99MAX` and
  its default limits are generous; use the median.
- Appends to the same object are serialized by the generator (each object holds its offset), so `--conc` above the
  object count adds no concurrency to appends.
- `populate` retries an append up to 60 times with a re-check of the object length, so a transient refusal does not
  abort a fill; its `retries` field is how often that happened.
