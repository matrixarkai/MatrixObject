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

## Admission and completion

The writer accepts offered requests through a rendezvous channel. It does not
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

Checkpoints still serialize a complete image while holding the foreground lock.
Pinned snapshots can require payload copy-on-write, and general overwrites still
scan page data. These paths require separate profiling and optimization before
claiming flat tail latency under checkpoint or repair interference.
