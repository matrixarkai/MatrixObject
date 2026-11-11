# Concurrent leased object reads

This workload measures the public remote read APIs with durable reader protection.
It uses three MatrixRaft metadata voter processes and one three-member,
journal-backed Star data group. All six processes run on one local WSL host.
Star retains N=3, W=3, R=1. Every measured read acquires and releases its exact
generation; a four-object batch uses four distinct objects.

## Workload and counters

The fixture contains four objects at each of 1, 128 and 1,024 extents, all
16 KiB long. Extent sizes are 16 KiB, 128 bytes and 16 bytes respectively. This
deliberately holds payload size constant while varying fragmentation; it is a
small resident dataset, not a capacity-scale workload.

Each configuration uses 1, 8 or 32 reader clients and batch widths 1 or 4. A client
performs eight measured operations. Each client owns its metadata and cached data
connections. Warmup completes for every client before counters and timing start.
All workers join before cleanup or failure propagation. The complete sweep
performs 1,968 operations and 4,920 object reads, excluding warmup.

Metadata uses the shipped 5,000-ms per-request consensus deadline. Each measured
SDK operation has a 30-second deadline and automatic 60-second leases. A diagnostic
attempt using the ordinary fault fixture's tighter 700-ms deadline exceeded that
limit at eight readers and four-object batches with 128 extents. The scale fixture
now explicitly selects the shipped default; existing fault fixtures retain their
700-ms or 3,000-ms membership deadlines. Product timeout defaults are unchanged.

Operation latency ends when the SDK returns. Throughput includes result
verification and thread dispatch. Batch throughput is in operations per second;
multiply by four for object reads per second in width-four rows. Percentiles use
nearest-rank ordering; a one-reader row has only eight samples.

Server CPU sums user and kernel CPU ticks from all six server processes.
Client CPU covers the test driver and all its reader threads. CPU and WAL samples
also bracket the post-read handle check and short quiescence sampling. CPU has
10-ms tick granularity on this host. WAL bytes sum observed logical file growth
across the three metadata voters after a short stability check. They include
Raft framing and hard-state records. Application checkpoints are disabled.

Data journal lengths must remain unchanged in every phase. Every result must
match its selected manifest and payload; after every phase the retained-handle
catalog must be empty. These assertions also run in a short CI workload.

## Local release sample

Storage code: 07b9e5a8. Release profile, all features, local WSL2 x86-64 on an AMD
Ryzen 7 PRO 5850U with 16 logical CPUs and about 24 GiB guest RAM. The host was shared
with other work and had no CPU or storage isolation. This is one sample run.
All 18 configurations completed with verified payloads, zero data-journal growth
and no retained handles remaining.

| Extents | Readers | Batch width | Ops/s | p50 ms | p99 ms | Server CPU ms/op | WAL bytes/op |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1 | 51.7 | 19.4 | 21.3 | 10.00 | 987.0 |
| 1 | 1 | 4 | 17.1 | 53.6 | 83.4 | 33.75 | 3948.0 |
| 1 | 8 | 1 | 279.7 | 27.6 | 36.3 | 2.66 | 580.5 |
| 1 | 8 | 4 | 81.9 | 96.8 | 107.5 | 8.75 | 2322.0 |
| 1 | 32 | 1 | 569.2 | 54.3 | 62.4 | 1.80 | 507.8 |
| 1 | 32 | 4 | 142.0 | 195.0 | 369.2 | 5.70 | 2031.0 |
| 128 | 1 | 1 | 65.9 | 14.9 | 15.5 | 10.00 | 1023.0 |
| 128 | 1 | 4 | 17.1 | 58.1 | 60.1 | 28.75 | 4092.0 |
| 128 | 8 | 1 | 251.7 | 31.5 | 34.1 | 4.84 | 586.5 |
| 128 | 8 | 4 | 66.3 | 120.8 | 130.9 | 15.31 | 2346.0 |
| 128 | 32 | 1 | 394.6 | 78.7 | 90.5 | 3.75 | 513.8 |
| 128 | 32 | 4 | 100.6 | 272.0 | 663.2 | 12.27 | 2055.0 |
| 1024 | 1 | 1 | 31.4 | 31.2 | 37.9 | 26.25 | 1026.0 |
| 1024 | 1 | 4 | 9.6 | 100.7 | 116.8 | 77.50 | 4104.0 |
| 1024 | 8 | 1 | 126.7 | 61.2 | 77.4 | 14.69 | 621.3 |
| 1024 | 8 | 4 | 31.6 | 236.1 | 342.9 | 66.25 | 2458.0 |
| 1024 | 32 | 1 | 193.4 | 163.6 | 173.4 | 15.66 | 516.8 |
| 1024 | 32 | 4 | 43.7 | 700.9 | 915.6 | 64.22 | 2156.8 |

At 32 readers, single-object throughput measured 569.2 ops/s at one extent and
193.4 ops/s at 1,024 extents. Server phase CPU increased from 1.80 to 15.66 ms
per operation. Four-object batches at 1,024 extents measured 43.7 ops/s, or
174.8 object reads/s, with 915.6-ms p99 batch latency.

Profiling remains necessary to separate manifest encoding, hashing, retained
catalog work, data-read dispatch and durable metadata admission. Compact grant
replies do not remove those costs. WAL growth also changes with Raft batching,
object-name lengths and log positions; it is not a fixed per-read constant.

This workload does not measure allocation counts, transferred network bytes,
physical device writes, checkpoint or repair interference, multi-host failure
domains, or capacity at large stored-data sizes. No competitor result or
production performance claim follows from this sample.

## Reproduce

Run on Linux or the existing local WSL setup from the repository root:

```sh
cargo test --release --locked --all-features --test network_metadata_consensus \
  leased_object_read_scale::measure_concurrent_fenced_reads \
  -- --exact --ignored --nocapture --test-threads=1
```

Require one passed test and 18 LEASED_READ_SCALE rows. For a quick correctness and
admission check:

```sh
cargo test --locked --all-features --test network_metadata_consensus \
  leased_object_read_scale -- --test-threads=1 --nocapture
```

The quick run requires two passed tests and one ignored measurement. It checks
concurrent whole-object and four-object reads, then fills all 128 retention slots,
requires an ordinary read to refuse without creating a handle, releases those
slots and requires the ordinary read to recover. The fixture owns and removes
its process directories, journals and child processes on completion or failure.
