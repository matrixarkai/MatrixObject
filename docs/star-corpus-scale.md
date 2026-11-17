# Star foreground operations at growing corpus sizes

The corpus workload measures fixed-size data requests against journal-backed
Star replicas. Background page count and manifest extent count are different
variables: the append traffic measurements in `star-append-amplification.md`
vary the latter. Neither bounded messages nor a small correctness run establish
flat latency at large stored-data volumes.

## Run

Use Linux/WSL and the existing shared Cargo target directory. From the repository
root, first validate the workload:

```sh
cargo test --locked --all-features --test network_metadata_consensus \
  star_corpus_workload -j1 -- --test-threads=1 --nocapture
```

The explicit release sweep is:

```sh
cargo test --locked --release --all-features --test network_metadata_consensus \
  measure_star_reads_writes_and_appends_as_corpus_grows -j1 \
  -- --ignored --test-threads=1 --nocapture
```

Record `git rev-parse HEAD`, build profile, host CPU/memory limits and
`getconf CLK_TCK` beside the output. Do not compare results from different
durability settings as the same workload.

## Configuration and isolation

Each background-size case starts three metadata voters and three separate,
journal-backed Star data processes. Each replica is limited to 256 MiB of page
payload and 65,536 pages. The workload preserves `N=3, W=3, R=1`, default data
server worker counts and durable journal synchronization. Setup uses bounded
batches of at most 32 writes, with three acknowledgements checked for every page.

The release sweep uses 1,024, 8,192 and 32,768 background pages, each containing
4 KiB and a distinct chunk identity: 4, 32 and 128 MiB per replica. Background
identities and foreground append identities use disjoint volumes. Each case runs
1, 8 and 32 clients with 128
operations per client, then restarts all six processes. Three fresh-fixture
repetitions rotate the background-size order. The ordinary test uses 8/256 background pages,
1/2 clients and four operations per client to validate the instrumentation and
recovery without running the performance sweep in every test invocation.

The timed operations are:

- Reads of complete 4-KiB background pages, spread across the page index and
  verified against the expected payload.
- Fresh 4-KiB writes to distinct page identities.
- 4-KiB appends to one private page per client, starting from a 4-KiB page.
  Starting length and append count are identical at every background size.

Reads sample the entire background index; the potential read working set grows
with the corpus. Cache hit rates can therefore differ. A fixed-working-set sweep
is a separate follow-up, not a conclusion that can be drawn from these rows.

Connections, capability negotiation, warm-up writes and allocation of the
starting pages occur before measurement. Read payload checks and mutation
acknowledgement checks are included in the individual latency samples. Failed
operations fail the workload; they are not omitted to inflate successful QPS.
Readiness and start waits are bounded so a failed client cannot leave the
coordinator waiting indefinitely.

Generated foreground pages remain present until that case ends. The phase order
and prior foreground traffic are identical for matching phases at each background
size. `stored_pages_before` reports the actual count, checked equal on all three
replicas; `background_pages` identifies only the seeded corpus. Each new
background-size case uses fresh processes and journals. Corpus files are removed
when its fixture ends, including failure cleanup.

## Output and correctness checks

Each `STAR_CORPUS` row reports operation, background pages and payload bytes,
the `group_commit` setting,
actual stored-page count before timing, clients, completed operations, replication
thresholds, phase elapsed time, QPS, p50/p95/p99 operation latency, aggregate
data-process CPU ticks and aggregate growth of the three data journals.
The one-minute host load average is recorded before and after each phase;
record other active workloads as well. These readings do not isolate the host
or turn a run with competing builds into an uncontended measurement.

CPU ticks are read from the data processes' Linux user/system counters. They
exclude client CPU and cannot be interpreted as milliseconds without `CLK_TCK`.
Phase QPS includes coordinator release and joining the workers; operation
percentiles measure the checked RPC call. Setup and restart are outside both.

Reads must add zero data-journal bytes. Successful writes must account for at
least three payload copies in the journals. The upper bound scales with the
number and size of suffixes, preventing whole growing pages from being journaled
on every append. After restart, the workload checks first/last background pages
and sampled acknowledged write/append pages directly on every replica, including
the original prefix and every appended byte in each selected page.

## Peer recovery release results

Source `81f2045`, built in release mode with all features, passed 81 phases,
141,696 checked operations and nine complete six-process restart checks. All
three data nodes used peer recovery with `N=3, W=3, R=1`. At 32 clients, medians
across the three repetitions were:

| Background pages | Read QPS | Fresh-write QPS | Page-append QPS | Median phase append p99 (ms) |
|---|---:|---:|---:|---:|
| 1,024 | 55,102 | 219.82 | 189.39 | 751.833 |
| 8,192 | 37,607 | 193.79 | 176.93 | 624.867 |
| 32,768 | 25,562 | 172.81 | 256.36 | 220.852 |

The percentile column is a median of phase percentiles, not a pooled-request p99.
Aggregate data-process CPU for these append phases was 1.92–2.47 ms per operation.
Reads added no data-journal bytes. Each 32-client write phase added 51,523,194
bytes to the three journals; each append phase added 51,903,264 bytes, independent
of background size and repetition, for 4,096 operations of 4 KiB each.

The 16-CPU WSL host had other workloads active, with load averages of 5.71–11.62.
The earlier XOR-profile run had different recovery work and higher contention.
These runs do not isolate the append optimization's contribution or establish
flat tail latency. Growing read working sets and independent journal syncs still
need separate profiling; checkpoints and repair interference remain unmeasured.

## Paired journal synchronization sweep

[Shared Star journal synchronization](star-journal-synchronization.md) adds an
explicit on/off control. The paired release workload uses the same executable,
peer-recovery profile, worker count and `N=3, W=3, R=1` for both modes:

```sh
cargo test --locked --release --all-features --test network_metadata_consensus \
  measure_star_shared_sync_at_growing_corpora -j1 \
  -- --ignored --test-threads=1 --nocapture
```

It rotates the three corpus sizes across three repetitions and reverses each
corpus's first mode between repetitions. Each mode starts fresh metadata and data
processes and runs 1/32 clients with 128 operations per client. The 18 fixtures
produce 108 phases, 228,096 checked operations and 18 full-cluster restart checks.
The ordinary correctness test also runs both modes. Compare matching modes and
corpus sizes; inspect host load and variability before attributing a difference
to journal synchronization. Reads and mutations remain separate timed phases.

### Sustained read measurement

The short sweep uses only 128 reads per client. Use the sustained read workload
when investigating smaller read-throughput differences:

```sh
cargo test --locked --release --all-features --test network_metadata_consensus \
  measure_star_sustained_reads_with_shared_sync -j1 \
  -- --ignored --test-threads=1 --nocapture
```

It performs 8,192 verified reads per client at 1,024 and 32,768 background pages,
with 1 and 32 clients, three repetitions and alternating mode order. Twelve fresh
fixtures produce 24 phases, 3,244,032 checked reads and 12 full-cluster restart
checks. Read working sets grow with the corpus; this is not a fixed-hot-set test.

Phase timing ends after every client completes its operations. Client connections
remain alive until elapsed time, data-process CPU and journal sizes are sampled;
connection teardown, thread joins and percentile aggregation follow measurement.
RPC latency samples cover each operation, including response validation. CPU
ticks cover the three data processes; client CPU is not included.

### Completed paired measurements

The idle-executor implementation at source `5c94b8b` passed the 108-phase paired
release sweep in 1,015.91 seconds. Journal growth was identical for each matched
mode pair. At 32 clients, medians across three repetitions were:

| Background pages | Append QPS, individual sync | Append QPS, shared sync | Append phase p99, individual/shared (ms) |
|---|---:|---:|---:|
| 1,024 | 190.38 | 326.54 | 369.860 / 192.114 |
| 8,192 | 247.87 | 370.14 | 404.853 / 159.915 |
| 32,768 | 265.68 | 395.40 | 422.394 / 142.660 |

These short phases included client cleanup in QPS timing. They showed higher
append throughput, but not universal latency improvement: fresh-write phase p99
at 32 clients worsened at 1,024 and 32,768 pages, and short read results were
mixed. The host was shared with other workloads, with load averages 5.95–15.65.

Source `a10ec8d`, rebased onto `59255ae`, passed the sustained-read workload in
258.59 seconds: all 24 phases, 3,244,032 checked reads and 12 full-cluster restart
checks. Every read phase added zero journal bytes. Load averages were 4.36–15.41.
For each corpus/client count, compare corresponding enabled/disabled phases
within a repetition, then take the median of the three ratios:

| Background pages | Clients | Median paired QPS ratio | Paired QPS ratio range | Median paired phase p99 ratio |
|---|---:|---:|---:|---:|
| 1,024 | 1 | 1.001 | 0.850–1.092 | 0.923 |
| 1,024 | 32 | 1.048 | 0.779–1.133 | 0.938 |
| 32,768 | 1 | 0.999 | 0.945–1.083 | 1.001 |
| 32,768 | 32 | 0.979 | 0.852–1.374 | 1.038 |

A ratio above one means higher QPS or higher latency respectively. These paired
results do not establish a read-throughput improvement or a consistent large
read regression. Variability remains substantial, especially at 32 clients.
Keep the per-repetition results rather than dividing separately computed medians.
Neither measurement establishes flat latency under checkpoints, repair, larger
corpora or end-to-end owned-object operations.

## Foreground checkpoint interference

Measure one foreground checkpoint during a fixed-suffix append phase:

```sh
cargo test --locked --release --all-features --test network_metadata_consensus \
  measure_star_append_checkpoint_interference_as_corpus_grows -j1 \
  -- --ignored --test-threads=1 --nocapture
```

Each fresh fixture seeds its background with checkpoints disabled, restarts the
three data nodes, and enables checkpointing at their restored journal length plus
128 KiB. Setup is below this additional allowance; the timed 128 appends cross
it. The interval exceeds all timed append bytes, so at most one checkpoint can
run on each data node. Controls restart the same way with checkpoints disabled.
The workload asserts an initial outcome journal and records each replica's first
transition to a checkpoint image immediately after the corresponding RPC.

Three corpus sizes, three rotated repetitions and alternating mode order produce
18 phases, 2,304 checked appends and 18 full-cluster restart checks. Replication
is N=3/W=3/R=1 with peer recovery, shared journal synchronization and four data
workers. The fixture explicitly removes inherited checkpoint and WAL zero-fill
settings; the latter would invalidate physical file length as an outcome-byte
position. The ordinary CI case uses 256 background pages and 64 appends per mode.

`STAR_CHECKPOINT` reports RPC p50/p95/p99, maximum latency, the slowest operation,
and maximum RPC latency at an observed checkpoint transition. A single pause can
be absent from p99 in a 128-operation phase. Per-operation journal probes follow
the RPC sample but are included in phase elapsed time and QPS. Data-process CPU
excludes the client and probes. `/proc/<pid>/io` provides aggregate `wchar`
(system-call write bytes, including socket replies) and `write_bytes` (kernel
storage write accounting); neither measures device-internal NAND writes.
Journal size before/after is reported separately because compaction can shrink
the file while causing substantial write traffic.

Every append requires all three acknowledgements. After the timed phase, all six
processes restart. Each data node must recover the complete foreground prefix,
every appended byte, the exact stored head, and sampled background pages. This
workload measures checkpoint interference; adding it does not move checkpoint
work off the foreground lock or establish flat append latency.

## Interpretation and remaining measurements

Source inspection identified two different costs to profile. The general standalone
RPC path in `src/cluster/replica_rpc_server.rs::apply_locked` captures page state
before mutation and uses `PageOutcome::between` afterward. The latter validates
page states and compares the complete prefix while selecting patches. A retained
payload handle can also force a copy when that page is mutated. This work scales
with the touched page's length, not the number of unrelated stored pages. The
local object-store journal transaction path is separate; improvements there do
not establish the same optimization in the standalone RPC path.

[Journaled Star append preparation](journaled-star-appends.md) now selects a
suffix-only decision for exact appends to validated pages in peer-recovery mode.
The corpus fixture and the three-member bring-up script select this profile;
single-copy standalone defaults retain local XOR recovery. Record the profile when
comparing runs. The general mutation path still handles other page shapes.

When configured, standalone journal checkpoints capture and serialize a complete
replica image from the foreground-locked service. That work scales with the
stored corpus. The current workload leaves this optional checkpoint policy off;
a separate interference phase must measure it before any flat tail-latency claim.

The bounded append decision preserves conditional state checks, read integrity,
physical-byte accounting and fail-closed persistence, with byte-compatible
outcomes. Re-run fixed-suffix tests at increasing page lengths independently of
the background corpus sweep. Background checkpoint work needs a safe journal
cut and recovery protocol before it can leave the foreground lock.

These are data-plane RPCs. The metadata voters run, but object selection, staged
ownership, manifest publication and read-session acquisition are not timed.
`Append` denotes appending inside one data page, not a completed public object
append. The workload does not establish storage-chunk rollover semantics.

The initial workload does not measure network bytes, server allocation counts,
client CPU, checkpoint interference, repair interference or TemporalStore
integration. Add those as separately labeled phases. Keep manifest prefix width
fixed in a subsequent end-to-end owned-append corpus sweep, and vary width in a
separate sweep. Hardware-specific latency and multi-host throughput require
different evidence from local WSL processes.

Investigate corpus-proportional foreground scans, copies or serialization before
claiming flat append latency. Ordered-index lookup can grow logarithmically.
Background checkpoints and repair must have their own tail-latency measurements;
disabling durability or lowering W is not a foreground optimization at the same
contract.
