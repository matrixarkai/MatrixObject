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
