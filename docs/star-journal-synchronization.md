# Shared Star journal synchronization

Journal-backed standalone nodes share a durable synchronization across mutation
requests already offered by their bounded request executors. Each mutation still
has its own versioned binary protobuf outcome. Every successful response follows
the synchronization covering its outcome, including a due checkpoint. The
replication thresholds remain the client's `N=3, W=3, R=1`.

## Configuration

`ReplicaNodeOptions::journal_group_commit` defaults to `true`.
`MATRIXOBJECT_REPLICA_GROUP_COMMIT=true` is the equivalent process setting;
`false` selects individual-request synchronization for controlled measurements
or operational diagnosis. Invalid settings are refused before serving.

The setting applies only to a standalone durable journal. In-memory nodes and
Raft groups retain their existing execution paths. The startup line reports
the effective `group_commit=true` or `group_commit=false` setting.

`ReplicaNodeOptions::journal_deferred_completion` defaults to `false`.
`MATRIXOBJECT_REPLICA_DEFERRED_COMPLETION=true` enables experimental deferred
completion when group commit is active. The journal queue holds at most 64
requests; every queued request retains its originating connection's input-byte
reservation. Executors return after submission instead of waiting for sync.
The same request, mutation and aggregate byte limits apply to each group, with
no batch-fill timer. Full queues apply backpressure. Replies still follow durable
sync; abandoned work closes its pending connection without acknowledging success.
This mode needs workload-specific latency and amplification measurements before
use. It does not increase executor threads or change replication thresholds.

## Admission and completion

By default, the writer accepts offered requests through a rendezvous channel. It does not
maintain another buffered queue of payloads; waiting executors retain their
existing connection-buffer charges. A group contains at most 64 requests and
64 mutations, counting entries inside conditional batches. Its aggregate encoded
input payload is at most the configured frame limit, 8 MiB by default. An offered
request that would exceed a group bound is held for the next group. Requests
are not split across groups.

The writer drains available offers without a batch-fill delay. When the service
is idle and no offers are outstanding, a request mutates and synchronizes on its
current executor. Contended requests join the writer; new requests do not bypass
an existing backlog. Measure low-concurrency latency as well as throughput.

The service lock covers ordered mutation, outcome append and synchronization.
Reads, repair and checkpoint installation cannot observe an unsynchronized group.
Responses are released after this durable completion point. Client disconnection
can leave a committed operation unconfirmed; clients must preserve that uncertainty.

Groups are not transactions. Sequence and state conditions retain their existing
per-request behavior. A refused request does not cancel other admitted requests;
a conditional batch can retain successful entries preceding its first refusal.
A group containing only refusals performs no journal synchronization.

An append or synchronization failure fences the node and prevents successful
responses from the group. Later reads and writes are refused until restart.
Failed service state cannot be published through deferred checkpoint completion.
The existing journal recovery protocol determines which unconfirmed outcomes
survived; no successful reply is issued on the assumption that a future sync
will persist its bytes.

## Write traffic accounting

`RemoteReplicaClient::journal_write_statistics` requests version-3 binary protobuf
status. `checkpoint_journal_with_write_statistics` starts a checkpoint with the
same status version. Versions 1 and 2 keep their existing wire encoding.
Segmented journals return `OutcomeWriteStatistics`; legacy journals return no
write counters. This does not change the persisted outcome format.

The counters cover successfully synchronized records since this journal was
opened. They separate foreground batch counts, decision counts, record bytes and
payload bytes from checkpoint record bytes, payload bytes and metadata bytes.
Checkpoint payloads include relocation and bulk installation. Record bytes include
envelopes and sector padding, excluding file initialization and manifests. They
are storage record traffic, not device I/O; compare them with kernel counters to
account for physical writes and cleanup. Replica totals must include every replica.

Sample with quiescent writers and completed maintenance when measuring a complete
interval. Sampling can wait behind an append. `checked_delta` rejects different
per-open identities, decreasing counters and saturated values, so a restart or
counter overflow cannot silently produce an interval result. Counters do not
include uncertain appends whose synchronization failed; a failed workload is not
a complete amplification measurement. Foreground decisions per batch measure
outcome packing, including decisions inside conditional batches.

## Copy admission and reclamation

Maintenance still retires wholly unreachable files. It defers copying live
records while committed journal record bytes fit within twice a conservative
live-payload byte estimate. Once that budget is exceeded, the existing rotating
candidate selection and bounded copying run normally. The candidate must still
be at least half obsolete; the per-attempt payload-copy limit remains 8 MiB.
Segment sizing, retention triggers, reader protection and checkpoint publication
rules are unchanged.

The estimate uses incremental physical byte unions, subtracting complete distinct
metadata records. Shared payload ranges are counted once; overlapping metadata
can only reduce the estimate. Metadata alone cannot inflate the payload budget.
The decision avoids a corpus-wide scan on the ordinary indexed path. The byte
budget is a copy-admission condition, not a promise that disk occupancy never
exceeds twice live payload: foreground writes, checkpoint emission and pending
retirement can temporarily add bytes before subsequent maintenance.

## Validation and measurement

The `shared_sync` library tests cover ordered batch/digest replies, independent
refusals, one synchronization for multiple admitted requests, reply withholding,
append and sync failure, failed-state checkpoint refusal, idle completion,
worker shutdown and request/mutation/byte group bounds. The existing TCP allocation
regression measures warm 4-KiB appends through a 64-MiB page prefix with this path
enabled. The process workload verifies writes and recovery with the setting both
enabled and disabled.

The [paired corpus sweep](star-corpus-scale.md#paired-journal-synchronization-sweep)
uses the same release executable and durability settings for both modes, rotates
corpus order and alternates mode order. Report QPS, phase latency percentiles,
CPU, journal bytes and host contention with each result. Synchronization sharing
reduces barrier frequency under load; it does not reduce the required three
payload copies or remove per-outcome journal framing.

Legacy journal checkpoints serialize a complete image while holding the foreground
lock. Segmented journal checkpoints capture state and fold dirty index paths in a
background worker. Pinned snapshots can require payload copy-on-write, and general
overwrites still scan page data. Checkpoint and repair interference require their
own measurements before claiming flat tail latency.
