<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->
# Immutable replica outcome journal

The standalone chunk server has an opt-in journal that separates immutable data
from checkpoint metadata. A checkpoint worker reconstructs a private catalog from
committed outcome metadata and saves changed index paths outside the replica mutex.
It does not capture the live page map or retain payload or receipt-map handles.
Foreground writes therefore do not copy a page because a checkpoint owns its data.

## Configuration

Set these variables for a disposable test deployment:

```sh
export MATRIXOBJECT_REPLICA_JOURNAL=/srv/matrixobject/test-node/journal
export MATRIXOBJECT_REPLICA_SEGMENTED_OUTCOMES=true
export MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES=8388608
export MATRIXOBJECT_REPLICA_LOCAL_RECOVERY=peers
export MATRIXOBJECT_REPLICA_RESIDENT_PAYLOAD_BYTES=67108864
export MATRIXOBJECT_REPLICA_RESIDENT_PAGE_ENTRIES=4096
chunkserver
```

Create the parent directory first and give each replica its own path. The API
entry point is `ReplicaNodeServer::bind_with_segmented_journal`. The default
continues to use the existing journal. An existing journal cannot be converted
by changing this setting: each reader refuses the other layout. There is no
in-place migration. Zero-filled journal frames and chunk Raft configuration
cannot be combined with this setting.

Segments default to 64 MiB. `MATRIXOBJECT_REPLICA_SEGMENT_BYTES` accepts sector-aligned
capacities from 24 through 512 MiB; the capacity must stay the same on restart.
`MATRIXOBJECT_REPLICA_RETENTION_SEGMENTS` accepts 2 through 1,000,000 and defaults
to 16. Both options require the segmented standalone journal. The API equivalent
is `bind_with_segmented_journal_options` with `ReplicaSegmentedJournalOptions`.
A foreground group contains at most 64 decisions and
8 MiB of encoded outcomes. No batching delay is added. Its payload, decisions,
and scalar replica metadata share one data synchronization before acknowledgment.
A larger group is split, retaining the exact scalar context of each sub-group.
Checkpoint fragments do not count as queued foreground mutations. Recovery retains
the uncheckpointed mutation bytes when calculating the next checkpoint threshold.

## Bounded payload residency

`MATRIXOBJECT_REPLICA_RESIDENT_PAYLOAD_BYTES` defaults to `0`, which retains
full residency. A positive value bounds the allocated capacities of page data
vectors after synchronized request completion. The separate
`MATRIXOBJECT_REPLICA_RESIDENT_PAGE_ENTRIES` limit defaults to `4096` and also
counts empty pages. These settings require segmented standalone storage and
`MATRIXOBJECT_REPLICA_LOCAL_RECOVERY=peers`. The API fields are
`ReplicaNodeOptions::resident_payload_bytes` and `resident_page_entries`.

Recovery loads page metadata and immutable span locations without hydrating
payloads. Capacity checks, page limits, inventory, heads and heartbeat usage
include cold pages. A cold range read captures its metadata and source file
owners under serialization, then performs verified payload I/O outside the node
mutex. A captured read retains its version across later writes and checkpoint
relocation. Normal reads reject discarded ranges; raw repair transfers retain
the complete state-checksum contract.

Mutations can materialize one cold working page. Grouped writes synchronize
before eviction when admission needs space, and eviction checks the resident
version against synchronized durable metadata. Page repair preserves sibling
catalog entries and can replace a corrupt cold payload without reading it.

The allocation budget covers resident page vectors. Request frames, reply and
response-cache buffers, staged repair data, one-page mutation working buffers,
metadata indexes and the separate log-chunk API require their own accounting.
It is not an RSS limit. Replication and acknowledgment policies remain deployment
settings; use three replicas with three durable write acknowledgments for Star.

## Publication and recovery

Storage is binary protobuf. State-only batches keep version 1, checkpoint storage
fragments use version 2, and state transitions with replica metadata use version
3. Ordinary replay refuses checkpoint storage records and refuses to discard a
version 3 batch's replica context. Replica recovery uses the context-aware path.

An indexed checkpoint contains immutable page metadata, shared payload
descriptors, ordered page spans, and a balanced page-key tree. Zero-filled holes
do not require payload extents. The worker tracks changed pages while replaying
outcomes; it writes their metadata and affected tree paths, reusing unchanged
records. Payload descriptors are shared across pages and checkpoints. Superseded
descriptors leave the worker cache when no catalog page uses them.

Index records use canonical binary protobuf with a 1 MiB body limit. Tree nodes
and extent descriptors are bounded to 16 KiB. Up to 64 records share a storage
batch; child nodes can precede their parents within the same synchronized batch.
Pointers bind record position, decision, identity, ordinal, length, and SHA-256.
Recovery checks key ordering, tree balance, subtree counts, and strictly earlier
children, including same-batch ordering. Large page metadata streams through a
single-page catalog instead of requiring an oversized tree node.

Scalar metadata and namespace/receipt maps use a separate streamed catalog.
The checkpoint becomes visible only when its small version-2 commit record is
synchronized. It binds the tree root, metadata catalog, epoch, and replay cut.
Interrupted preparation leaves the previous committed checkpoint valid.

Version-1 checkpoint catalogs remain readable. Their next checkpoint builds the
index once; subsequent checkpoints reuse it. After a version-2 commit is published,
older binaries refuse that journal. Keep a compatible binary for restart: binary
rollback requires an independently retained compatible deployment snapshot. The
default journal layout and its stored format are unchanged.

Background checkpoints retain their captured replay position. Recovery folds
subsequent mutations as metadata. Full-residency recovery materializes the
resulting pages; bounded-residency recovery leaves their payloads cold.
The captured position is the later of the last durable mutation and the previous
published checkpoint. The worker folds through that boundary before publication,
including any intervening mutations. This lets repeated quiet checkpoints advance
past obsolete storage-only records. Boundary capture uses the in-memory segment
order and performs no file reads or catalog traversal under the replica mutex.
A completed worker advances the cached boundary only when no newer durable
mutation has arrived; otherwise the newer mutation remains the next replay cut.
Recovery verifies the newest published checkpoint; corruption is refused rather than
silently falling back to an older checkpoint. File identity, exclusive ownership,
segment bounds, payload sector digests, and resulting page checksums are checked.

A bulk install persists its payload fragments and catalog before publishing its
commit. Publication is serialized with background checkpoint epochs. A superseded
worker cannot publish over the install, and recovery refuses an install checkpoint
whose replay position skips an intervening logical mutation. Retired catalogs are
destroyed outside the replica mutex. Background errors fence requests and are
reported as an offline replica by the heartbeat path.

Deployment recovery policy is applied after journal restore. A peer-recovery node
keeps no local recovery sidecar; a node configured for local recovery rebuilds it.
Checkpoints do not themselves establish membership or repair missing peer writes.

## Segment retirement

After a published checkpoint reaches the configured segment threshold, a worker
traces the current index, catalog fragments, every page's extent-metadata records,
and shared payload spans. It preserves these segments and the replay tail. The
replay cut alone is insufficient: an unchanged cold page can use much older data.
The next discovery threshold doubles from the retained count, with the configured
minimum and a one-million-segment ceiling. Busy readers and concurrent rollover
defer collection for 30 seconds; an explicit checkpoint allows a fresh attempt.

The manager publishes and synchronizes a version-2 sparse segment manifest before
unlinking any file. An interruption therefore leaves either the previous live set
or a durable list of retired files. Cleanup verifies identities and exclusive
ownership, processes at most 64 candidates per call, and synchronizes the directory
before clearing debt. Foreign identities remain quarantined. Recovery retries
cleanup; subsequent replica health checks continue it in a background worker.
Reader pins and superseded checkpoint workers keep discovery from removing records
they might still access. Discovery and unlinking run outside the replica mutex.

The `MOSEGM02` manifest requires a compatible binary after first publication.
Version-1 manifests remain readable and retain their existing encoding until
retirement. There is no automatic binary downgrade or journal-layout migration.

Existing `journal_status` and `checkpoint_journal` calls retain version-1 wire
behavior. `journal_maintenance_status` and `checkpoint_journal_with_maintenance`
explicitly request version 2, adding retired-file counts, committed retired bytes,
and cleanup progress. `maintenance_is_complete` waits for its checkpoint, queued
mutations, and already scheduled retirement work. For an I/O measurement, stop
writers, drain existing checkpoint and retirement work, request a final checkpoint,
then wait for this predicate on every data replica. Status reads do not append
journal records. A maintenance barrier completes its requested pass; it does not
certify that every eligible segment was visited or fully evacuated, and cannot
force collection through pinned readers.

## Background and manual compaction

With automatic checkpoints enabled, an idle maintenance worker can compact partly
live files once the configured retention threshold is reached. It does not depend
on requests or metadata heartbeats. It uses a nonblocking attempt to acquire the
replica lock and leaves queued foreground groups for their existing committer.
An explicit checkpoint can force a fresh attempt. The worker samples up to 64 sealed segments before the replay
tail, rotating its sampling cursor between passes. It selects one file whose
conservative live physical ranges occupy at most half its committed body. The
cursor is an in-memory maintenance hint and starts again after recovery.
Whole-record metadata charging can defer files whose remaining metadata is spread
across many otherwise obsolete batches.

A copying pass can be followed by another attempt after one second; an unproductive
scan or a busy reader defers analysis for 30 seconds. Existing collection backoff
also defers retries. An analysis-only pass that finds no work writes no journal records and does
not advance the published checkpoint epoch. Version-2 status counts that analysis
as maintenance in flight. A manual request arriving during analysis reserves a
separate epoch; repeated requests coalesce, and completion waits for a real root.

A pass copies at most 8 MiB of live payload. Overlapping and shared source ranges
are copied once and packed together into existing 1 MiB binary protobuf payload
fragments. Large extents can be evacuated across repeated passes; unmoved ranges
retain their old descriptors. Logical page offsets, zero-filled holes, checksums,
sequences, namespaces, and receipt state remain unchanged.

The copying worker reuses its 1 MiB fragment buffer with an 8 KiB allowance for
verified padded edge sectors, plus one 4 KiB sector buffer. Small-range cache keys
include the source extent and sector offset. Verified range reads append directly
to the destination; errors preserve its original bytes and length. Touched sector
digests, padding, and complete-payload digests retain their existing checks. This
removes temporary range buffers, not catalog or descriptor allocations.

Recovery and catalog reads reuse the verified provider buffer when a single span
covers the requested range. Oversized provider allocations are compacted before
return so padded sectors do not inflate resident page capacity. Reads spanning
multiple fragments or zero-filled holes retain their existing assembly path.
Final page checksum and state validation still run during materialization.

The index tracks actual extent-metadata record positions, including multiple valid
records describing the same payload. Compaction rewrites affected page records,
index nodes and ancestor paths, streamed page catalogs, and cached descriptor
locations. A read pin protects old records until the replacement checkpoint root
is published. Cancellation leaves unpublished storage fragments, not a new logical
state. The sparse manifest still has to commit before any old file is unlinked.

The payload-copy budget does not bound catalog traversal, metadata rewrites, or
total I/O. Manual maintenance resets both collection backoffs, but a single pass
can leave eligible files or live ranges for a later pass. Dense candidates can be
deferred without copying. These maintenance
constants do not change segment capacity, foreground batching, or the default
journal layout.

## Current limits

This is a test layout, not the default production layout. Automatic retirement
reclaims wholly unneeded segments; background and manual compaction can evacuate
partly live files. This does not establish a strict retained-byte or space-amplification bound.
Changed-page work grows with its span count and tree path; scalar metadata and
namespace/receipt maps still serialize completely. Bulk installs currently
synchronize individual payload fragments. Full residency remains the default.
With bounded residency enabled, cold mutations still materialize one working
page and touched-page span folding can grow with mutation history. Page repair
can rebuild catalog metadata proportional to the live corpus.

Measured compaction costs and sustained hot/cold corpus-growth measurements are
required before default activation.
Local multi-process tests establish functional recovery, not multi-host performance,
flat append latency, or a QPS improvement.

## Validation

Run these commands in Linux or WSL with a single Cargo build job:

```sh
cargo test --locked --all-features --lib -j1 resident_cache_ -- --test-threads=1
cargo test --locked --all-features --lib -j1 replica_checkpoint_ -- --test-threads=1
cargo test --locked --all-features --lib -j1 segmented_ -- --test-threads=1
cargo test --locked --all-features --test durable_payload_allocations -j1 -- --test-threads=1
cargo test --locked --all-features --test network_metadata_consensus -j1 segmented_star_replicas_keep_objects -- --test-threads=1
```

The process test starts three metadata nodes and three chunk servers, confirms
six distinct live PIDs with their own storage paths and established sockets, writes
objects through Star replication, changes metadata leadership, appends data, kills
and restarts all six processes, and reads every object from every data replica.

## Standalone checkpoint control

`RemoteReplicaClient::journal_status()` reports bounded journal progress without
walking pages or reading payloads. `checkpoint_journal()` synchronizes preceding
mutations and starts a checkpoint. Both commands address the connected standalone
replica directly; they are refused on memory-only nodes and Raft groups. A busy
checkpoint is refused without fencing an otherwise healthy node. I/O failures
fence the node, including failures observed while polling background publication.

The canonical binary protobuf control request is bounded to 32 bytes and its
status reply to 256 bytes. Unknown actions, versions, duplicate fields, unknown
fields, and noncanonical encodings are refused. Status includes a 16-byte journal
identity, layout, started and published epochs, in-flight state, queued record
count, uncheckpointed logical bytes, committed journal bytes, and segment count.
Committed journal bytes exclude preallocated space and ancillary files; they are
not a measurement of storage I/O. Legacy epochs and identity last until reopen;
segmented identity and committed epochs survive restart.

Quiesce writers, wait for any current checkpoint, request a checkpoint, then poll
`status.checkpoint_is_published(&requested)` and `!status.checkpoint_in_flight`.
The helper refuses a changed journal identity or layout. A quiescent completed
barrier must also report zero pending records and zero checkpoint debt. A status
observation can reap a completed worker but does not append journal records.

## Corpus I/O measurement

The paired local WSL measurement uses three metadata processes and three data
processes, with three acknowledged copies and one-copy reads. Its timed requests
use the data replica API directly; metadata publication QPS is outside that scope.
Every phase drains warmup checkpoints before sampling data-node `/proc/PID/io`.
Writes and appends then include completed automatic checkpoints plus a final
explicit checkpoint. Request QPS and latency describe acknowledgment; cycle QPS
and CPU include checkpoint drain. Read phases do not create extra checkpoints.

The output includes kernel write bytes, cancelled write bytes, their signed
difference, kernel read bytes, and all-file-descriptor write characters/calls.
The latter include sockets. These Linux process counters are not Windows device
I/O or allocation counts. All live payloads remain cached by the service. The
1 MiB checkpoint threshold is a stress configuration, not a default change.
One paired run is diagnostic evidence, not a sustained throughput guarantee.

```sh
cargo test --locked --all-features --lib -j1 journal_control_ -- --test-threads=1
cargo test --locked --all-features --test network_metadata_consensus -j1 checkpoint_corpus_measurement_drains_both_layouts_and_recovers -- --test-threads=1
cargo test --locked --release --all-features --test network_metadata_consensus -j1 measure_checkpoint_io_at_growing_corpora -- --ignored --nocapture --test-threads=1
```

The functional case covers both layouts and full process restart. The explicit
release measurement pairs 1, 16, and 64 MiB background payloads per data node,
16 clients, and 512 requests per phase, using the same foreground workload and
rotating layout order. Every read checks its complete payload; retained first
and last written pages and cold pages are checked on every replica after restart.

One release run at commit `aae965b1` in local WSL produced these append results. Each phase appends
2 MiB of logical data, with three acknowledged copies. Kernel writes sum all
three data processes through the completed final checkpoint.

| Background payload per replica | Layout | Append p99 | Cycle QPS | Kernel write bytes / logical byte |
| --- | --- | ---: | ---: | ---: |
| 1 MiB | legacy | 105 ms | 320 | 27.68 |
| 1 MiB | segmented | 58 ms | 396 | 5.95 |
| 16 MiB | legacy | 688 ms | 116 | 95.77 |
| 16 MiB | segmented | 67 ms | 373 | 8.40 |
| 64 MiB | legacy | 1919 ms | 58 | 313.57 |
| 64 MiB | segmented | 157 ms | 193 | 16.47 |

These are short stress phases on one machine, with layout order rotated by corpus
size and no cross-host validation. Segmented latency and amplification still grow;
this run does not establish flat latency or a production throughput guarantee.
All cached read phases recorded zero kernel write bytes. This table predates the
incremental index and does not measure its behavior. Safe reclamation, bounded
payload caching, and sustained measurements remain necessary before default
activation.

## Large live-corpus measurement

The explicit `measure_segmented_star_io_at_large_live_corpora` case measures
64, 256, and 1024 MiB of live background payload per data replica. Each size uses
fresh three-metadata and three-data processes, three acknowledged copies,
16 clients, and 512 requests per read, write, or append phase. It retains the
1 MiB checkpoint stress threshold. The fixture advertises the same 1536 MiB
capacity and 524288-page limit to metadata, data processes, and restarted nodes.
These are benchmark settings, not deployment defaults.

Each phase reports acknowledgement latency, checkpoint drain, request and cycle
QPS, data CPU ticks, kernel I/O, and the sum of the three data-process RSS values.
RSS includes shared mappings in each process and is not heap-allocation traffic.
The current implementation still retains every live payload in memory. Restart
verification reads the first and last background pages and retained foreground
results from every replica; measured reads verify their complete payloads.

```sh
cargo test --locked --release --all-features --test network_metadata_consensus -j1 measure_segmented_star_io_at_large_live_corpora -- --ignored --nocapture --test-threads=1
```

This case alone does not establish sustained throughput, multi-host behavior,
bounded resident memory, or flat append latency. Record host contention with the
results and use source-matched release binaries for measurements.

## Legacy checkpoint stream buffering

The legacy checkpoint writer coalesces fragmented encoder reads in a fixed 64 KiB
output buffer. It flushes that buffer before rewriting the checkpoint header,
synchronizing the file, and atomically publishing it. Flush errors propagate to
the replica's existing failure fence. This changes neither stored bytes nor the
full-corpus payload rewrite; it targets small-write syscall overhead. The table
above predates this buffering change.


## Catalog metadata residency

The payload-byte and resident-page limits apply to the working payload cache,
not the complete node heap. Durable page catalogs, payload locators, checkpoint
indexes and allocator retention also consume memory as page count grows.
Budget these structures separately when selecting node capacity; a small payload
cache alone does not establish a bound on total process memory.
