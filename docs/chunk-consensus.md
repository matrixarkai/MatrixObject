# Chunk servers with independent Raft voters

Build `chunkserver` with `--features chunk-consensus`. Each process owns one
MatrixRaft `DurablePeer`, its own page state, and its own existing storage
directory. The existing star-write client remains available for standalone chunk
servers. A Raft group accepts requests through its current leader; it does not
accept star writes independently at every voter.

Example for voter 1 of three:

```sh
MATRIXOBJECT_REPLICA_ADDR=127.0.0.1:17301 \
MATRIXOBJECT_CHUNK_RAFT_NODE_ID=1 \
MATRIXOBJECT_CHUNK_RAFT_GROUP_ID=1 \
MATRIXOBJECT_CHUNK_RAFT_PEERS=1@127.0.0.1:18301,2@127.0.0.1:18302,3@127.0.0.1:18303 \
MATRIXOBJECT_CHUNK_RAFT_STATE_DIR=/existing/chunk-1 \
MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS=3000 \
cargo run --features chunk-consensus --bin chunkserver
```

Use a distinct API address, node ID, and directory for each process. Every voter
has the same peer catalog, group ID, capacity, and page-count bounds. The catalog
contains 1..7 distinct IP sockets with nonzero voter IDs and ports; wildcards,
multicast addresses, and duplicate sockets including mapped aliases are refused.
The request timeout accepts 100..30000 milliseconds. Partial configuration and
combining Raft state with `MATRIXOBJECT_REPLICA_JOURNAL` are refused.

## Durability and request behavior

Writes and discards are prepared against committed state and encoded as `MOPO`
binary protobuf page outcomes. The peer WAL syncs its Raft records before the
outcome is applied and acknowledged. Recovery applies decided outcomes; it does
not execute original write commands. Reads, inventories, heads, and pings require
a current-term quorum confirmation and application through its commit index,
so an isolated minority cannot answer from stale state. These reads append no log
entries. One confirmation may be pending, including after a caller timeout.
A timeout does not mean a submitted write was rolled back: it may commit later.
The pending proposal remains serialized until commit or a leadership change.

`chunk.peer.base` is a canonical `MOCB` binary protobuf bootstrap containing the
capacity and page limit. `chunk.peer.wal` is the independently durable MatrixRaft
peer log; `chunk.peer.lock` prevents a second writer. Missing paired files,
truncated WALs, and changed persisted bounds fail closed. The bootstrap is part
of the group identity, so voters with different storage bounds cannot agree.
There is no automatic import of a standalone chunk journal.

The existing health-report settings can publish each voter's committed physical
usage to metadata. Reports include applied and recovered data on followers, and
stale processes are expired by the metaserver. Process health does not establish
leader routing or prove that a recovering follower has caught up. The process
test also verifies catch-up and updated reports after writes while a voter is
offline.

## Current bounds and remaining work

The request queue holds 32 entries, peer queues hold eight packets, and peer TCP
frames are bounded at 8 MiB with 100 ms transfer deadlines. MatrixRaft currently
limits individual proposals to 4 MiB and its WAL to 512 MiB. Oversized proposals
are refused without changing committed page state. The candidate preparation
currently clones replica state. Quorum-confirmed reads avoid additional log
entries. Write history can use the opt-in application checkpoint policy below;
large-scale measurement remains required.

The process tests cover three independent voters, acknowledged writes and
discards, leader loss, refusal below quorum, restart of all processes, capacity
refusal, storage guards, shutdown, and metadata reports. They are functional
fault tests, not a large-scale throughput result. Metadata placement of Raft groups
is described in `chunk-group-catalog.md`. The HTTP distributed backend,
authentication, file-backed snapshots for larger state, and membership changes
remain unfinished.

## Bounded leader routing

Use `RemoteReplicaClient::connect_verified_raft_catalog(&endpoints, &identity, &options)`
with an operator-supplied complete configuration to check every connection,
including the first, before data can be sent. The expected voter's node ID may
name any member of that configuration.

For discovery from a trusted initial voter,
`RemoteReplicaClient::connect_raft_catalog(&endpoints, group_id, &options)` selects
a leader from one through seven distinct API IP sockets. Before sending a
data request, the client checks the expected group ID and pins the first voter's
membership, peer addresses, capacity, and page limit. Every new connection must
match that configuration. A numeric ID reused by two different voter groups is
therefore refused. An unavailable endpoint can be skipped before connection; an
identity mismatch or failed handshake stops the call without sending data.

Identity responses use bounded canonical binary protobuf described in
`proto/chunk_group_identity.proto`. Leaders and followers can describe their
immutable configuration without adding a read barrier to the log. This does not
assert quorum, catch-up, or readiness, and does not authenticate a server. A trusted
initial configuration and trusted transport are still required. Old servers and
standalone replicas cannot satisfy a verified Raft catalog. The older
`connect_catalog` API remains available for explicitly trusted catalogs without
identity verification.
Do not count several connections to the same group as independent star-write
replicas. Catalogs reject hostnames, zero ports, wildcards, multicast addresses,
and duplicate sockets including mapped IPv4 aliases before opening a connection.

Each call visits the catalog at most once, starting with its currently selected
endpoint. A voter that has not admitted a request returns typed status 6 with an
empty payload. Only this response allows an already-sent mutation to move to
another voter. Ordinary refusals stop the call, even if their text mentions a
leader. Older servers without the typed response therefore do not redirect.
Connection failures before submission may advance to another endpoint.

Reads, heads, inventories, and pings may also fail over after transport failure.
A write or discard with a lost reply remains uncertain and is never automatically
replayed. The next explicit call can establish a fresh connection. A catalog does
not wait through an election or continuously poll for a leader.

The chunk client closes its connection after a transport timeout or malformed
response. Single-endpoint clients must be replaced before another request.
The read timeout bounds the entire response frame, including a fragmented header
and payload; slow bytes do not reset that deadline. Fully decoded server refusals
leave the connection usable. Malformed routing responses close the connection.

## Application checkpoint recovery

After upgrading every voter, set `MATRIXOBJECT_CHUNK_RAFT_CHECKPOINT_BYTES`
to enable automatic capture, for example `33554432` (32 MiB of new WAL history).
The default is `0`, which disables capture for a controlled rolling upgrade.
Nonzero values must be between 1024 and 134217728 bytes. Library callers use
`checkpoint_after_bytes` in their Raft options. The setting does not change group
identity and may differ between voters; every voter must support restoration.

Capture runs on the exclusive application worker after earlier proposals and
reads settle. The canonical binary protobuf formats in `proto/node_checkpoint.proto` and
`proto/chunk_checkpoint.proto`
bind service kind and exact applied Raft position to the validated binary service
snapshot. Recovery restores that state before applying later protobuf outcomes.
Metadata restoration rebuilds the placement roster and checks the immutable group
catalog. Chunk restoration preserves page contents, discards and receipts, checks
configured capacity/page limits, and reconstructs physical used-byte accounting.

MatrixRaft captures the envelope into a synced payload file and atomically
publishes a version-4 WAL manifest while retaining the uncommitted suffix.
A lagging voter receives bounded version-3 peer fragments; it persists and
restores application state before acknowledging Raft progress. Capture reads
the envelope header and service bodies without assembling another complete
protobuf buffer. Restoration checks identity and canonical field ordering before
allocating each body. Existing inline and segmented checkpoints remain readable.

Upgrade every voter to this runtime before enabling capture. Older runtimes
refuse the new WAL and packet versions. Back up the WAL, its `.payloads`
directory, and `.runtime.lock` sidecar together while the node is stopped; the
WAL alone no longer contains the snapshot. Missing or damaged payloads refuse
recovery. Retired payload files are removed after active transfers release them.

Chunk checkpoints now use the version-2 binary protobuf page stream in
`proto/chunk_checkpoint.proto`. Capture borrows immutable page payloads and
redundancy bytes instead of copying the complete dataset into a serialization
buffer. Checksum scans also read the payloads directly. Recovery allocates each
page once, validates the complete service checksum, and only then installs state.
Receipts, settings, counters, discard ranges and compression accounting are kept;
fragment digests are rebuilt from validated data as in the earlier snapshot codec.
The older version-1 application envelope remains readable.

On a 64-bit host the checkpoint file may be up to 16 GiB. Each page payload and
metadata record remains limited to 256 MiB, and configured capacity and page
count limits are checked during restore. Metadata records, page descriptors and
receipts still occupy memory; the application itself still keeps resident page
data. Atomic installation temporarily retains the old and recovered service states.
Snapshot export and final WAL publication remain synchronous and can delay requests.
If an image exceeds a format bound, the server retains WAL history and retries
after another configured interval. The WAL still stops writes at 512 MiB.

Disable capture on every voter before a rolling upgrade, then upgrade every
chunk voter before re-enabling capture. Do not downgrade after version-2 capture. A voter with
only the version-1 application decoder cannot restore version-2 checkpoints even
if its MatrixRaft runtime supports file-backed payloads. Metadata voters use a separate version-2 stream; see
[metadata recovery](metadata-consensus.md#application-checkpoint-recovery).

Process tests exercise repeated compaction, catch-up after an offline interval,
and total restart. Chunk tests transfer state larger than one peer frame and
check subsequent writes, page state and capacity enforcement. Metadata tests
check placements and health roster recovery. These are functional fault tests,
not power-loss or multi-host throughput measurements.

## Conditional data updates

`RemoteReplicaClient::write_chunk_if_sequence` sends canonical binary protobuf
request code 8. A zero expected sequence requires an absent page; a positive value
must equal the current page write sequence. The check and write run on the same
exclusive application state. Consensus preparation checks the projected state of
earlier admitted updates; the committed binary outcome contains the result.
A stale condition returns a precondition failure without changing page data.

This works for standalone journal-backed nodes and Raft groups. It is a condition
on one page in one replica or group, not a transaction spanning star-write peers
or the metaserver. An uncertain reply is not automatically retried. Read the
current state and resolve the operation before issuing another mutation.
Upgrade each serving node before using this verb; older nodes refuse code 8.
See `metadata-consensus.md` for conditional placement commits.

To exercise recovery beyond the old application limit on one host:

```sh
cargo test --locked --release --all-features --test network_chunk_consensus chunk_streamed_checkpoints_recover_large_process_state -- --exact --ignored --nocapture
```

This uses three processes and 272 MiB of page data, forces a checkpoint larger
than 256 MiB, catches up an offline voter, commits with that voter, and verifies
every page after a total restart. It is not a multi-host throughput measurement.

## Background checkpoint capture

Each node keeps at most one capture worker. At a settled applied position it
exports an immutable snapshot, then moves serialization, capture validation and
payload file sync to that worker. Peer traffic and later writes continue while
capture runs. The completed payload is published at its original applied
position, retaining every later log entry for recovery. A newer received
checkpoint can supersede the worker result without replacing newer state.

Over-limit snapshots keep WAL history. Worker failures stop the node rather than
silently treating a failed capture as durable. Orderly shutdown joins the worker
and discards an unpublished payload. Snapshot export copies collections on the consensus loop; checksum sealing and
chunk compression statistics run on the worker. Final WAL replacement and sync,
and incoming snapshot restoration still
run synchronously. This change does not promise a bound on request latency or
constant memory; later writes can also fill the WAL before capture completes.

## Peer receive scheduling

The peer listener handles up to 16 incomplete connections concurrently. Each
connection retains the existing four-byte length header and one packet, bounded
at 8 MiB. Incomplete payload buffers total at most 16 MiB per listener; a frame
that cannot fit is dropped. The completed-packet queue remains bounded at eight
entries and drops packets when full, leaving retransmission to Raft.

A nonblocking sweep reads at most 64 KiB from each active connection. Partial
headers and stalled payloads therefore do not hold up other active peer sockets.
The original 100 ms absolute frame deadline remains in effect. Expiry, malformed
lengths, EOF and shutdown release the connection and its incomplete buffer.
Connections waiting in the operating-system accept backlog are admitted when a
slot becomes available. This is bounded receive scheduling, not authenticated
admission or protection against a sustained connection flood.

## Star-replica quorum read memory

`RemoteReplicaSet::read_chunk` still polls every configured replica and requires
exact agreement on write sequence, checksum and returned bytes. After collecting
replies, voting moves each payload into its vote key instead of cloning it.
Matching duplicates are dropped, and the winning payload is moved into the result.
This removes additional payload copies during voting without replacing byte
comparison with digest comparison or changing the quorum threshold.

For `r` successful replies containing `n` bytes each, the collected payloads still
occupy about `r * n` bytes. Vote-table nodes, socket buffers and decoding overhead
are additional. This is not a streaming read or a bound on total process memory.
The allocation-retention guard also verifies that equal checksum strings cannot
hide differing payload bytes.

## State-fenced star writes

`RemoteReplicaSet::write_chunk` first reads every member's page head. It requires
a read quorum that agrees on the complete head, or a read quorum reporting an
absent page. At least a write quorum must match that selected state before any
mutation is sent. Only those matching members receive a conditional write using
the observed sequence, with zero requiring an absent page. A concurrent sequence
change is refused by the serving node. Equal sequences alone cannot admit members
whose lengths, checksums or discarded ranges differ.

This adds a head round trip to each member per star write. All serving replicas
must support conditional write request code 8; an older server's refusal does not
fall back to an unconditional write. An uncertain mutation is not retried, and
partial application can still occur if a member fails after preflight. Callers
must resolve uncertain results before issuing another operation.

The check prevents an already divergent member from counting toward a fresh write
acknowledgment. It does not repair that member, establish
cross-client linearizability, authenticate replies. Repair of missed writes remains unfinished. Raft-group
writes continue through their leader and do not use this star fan-out.

## Conditional discards

`RemoteReplicaClient::discard_chunk_if_sequence` uses request code 9 with the
canonical binary protobuf `ChunkDiscard` in `proto/conditional_requests.proto`.
Its payload is bounded at 96 bytes. The page must exist at the expected positive,
nonexhausted write sequence. Sequence zero and `u64::MAX` are refused. Conditional
writes also refuse an existing page at `u64::MAX`, since a saturated counter could
otherwise leave a stale condition valid after mutation.

The condition and discard execute on the same exclusive application state.
Standalone nodes sync binary page outcomes to their journal; Raft voters prepare
and apply the same outcome representation. Recovery restores the decided discard
and sequence instead of evaluating the original condition again. Malformed,
noncanonical, unknown-version and oversized request payloads are refused.

`RemoteReplicaSet::discard_chunk` now uses the same complete-head preflight as
star writes and sends conditional discards only to matching members. It requires
an existing page quorum, including for zero-length requests. A write quorum must
match before mutation begins. A sequence change after preflight is refused by the
serving node. A member failing after admission can still leave partial application;
a lost response is not automatically retried. Upgrade all serving members before
using this star-discard path: older servers refuse code 9, without fallback to
unconditional deletion. Direct `discard_chunk` on a single client retains its
existing unconditional behavior.

The tests cover divergent-member exclusion, refusal before mutation when a
matching write quorum is unavailable, journal recovery, Raft leader loss, total
restart, stale conditions and sequence exhaustion. This does not add cross-client
transactions, repair or whole-replica catch-up.

## Reconnecting star members between operations

`RemoteReplicaSet` retains the configured client options and the initial resolved
socket address of each member. Before a later read, head, ping, write or discard,
it attempts one reconnect for connections already marked unusable by a transport
or framing error. Healthy connections are retained. Reconnection uses the pinned
IP socket rather than resolving the caller's hostname again; endpoint labels and
membership order remain unchanged. Each attempt uses `connect_timeout`, so an
unreachable member can add that delay to an operation.

An operation that discovers a broken connection does not reconnect and resend its
request. A later explicit operation can reconnect. A lost write or discard reply
remains uncertain and is never replayed by this mechanism. Successfully pinging a
restarted member proves connectivity only. Writes and discards still select a
matching page-state quorum before mutation: recovered matching pages can contribute
again, while pages missing intervening writes remain excluded. Reads still require
a matching quorum. No data is copied to bring stale members up to date, and no
whole-node readiness or failure-domain claim follows from a successful reconnect.

Process tests restart journal-backed members at the same address both with and
without missed quorum writes. Socket tests drop a write reply and require the next
connection to carry only the caller's later ping. Standalone clients outside a
replica set retain their existing closed-connection behavior.

## Release-build large checkpoint recovery

On October 29, 2026, commit
`84006f35c7de6c7fc4753115badc0415b290a70d` passed
`chunk_streamed_checkpoints_recover_large_process_state` in 39.06 seconds
using a release build on a 16-logical-CPU WSL2 host. Three chunk processes use
300 MiB capacity each, a 300-page limit and a 128 MiB checkpoint interval.
One follower remains offline while 272 one-MiB pages are created. The test
requires a checkpoint larger than 256 MiB, restarts the lagging follower,
then stops the other follower so a new write requires the recovered member.
All processes then restart, and every original page is checked byte for byte
at sequence one along with the later write.

The later conditional creation encountered a leadership-change response that
left its outcome uncertain. The test resolved it through conditional creation
and readback, and the final sequence remained one. Reproduce with:

```sh
cargo test --release --locked --all-features --test network_chunk_consensus chunk_streamed_checkpoints_recover_large_process_state -- --exact --ignored --nocapture --test-threads=1
```

This verifies Raft checkpoint recovery on one host. It does not implement
repair for star replicas, establish multihost throughput, or measure peak memory.

## Bounded chunk read confirmation batches

A chunk Raft leader can confirm up to 32 already queued read-only requests
with one quorum read barrier. Admission ends before the barrier starts. Reads
arriving afterward remain queued for a later confirmation; a pending barrier is
never reused as a cached leadership lease. Application state is read only after
that barrier's index has applied locally. Leadership loss refuses every request
in the affected batch.

Admission stops at the first mutation and retains it as one deferred request.
The next operation consumes that mutation before later reads, preserving queue
order. Additional read requests whose deadlines have already expired are refused
before confirmation. The existing bounded request channel remains unchanged;
the runtime holds at most 32 reads plus one deferred mutation outside it.
No mutations are batched by this change, and accepted writes still wait for
application or leadership change even if a caller times out.

The admission guard checks the batch limit, expiration, late-arrival exclusion
and mutation ordering. The process guard runs eight concurrent clients reading
bytes, heads and page counts, verifies the WAL is unchanged, then confirms a
subsequent write and refusal of reads after quorum loss. Batching reduces the
number of confirmations when reads arrive together; it does not remove page
readback from object publication or establish a measured throughput improvement.

## API socket readiness

On Unix, metadata and chunk API I/O workers wait for socket readiness instead
of waking every millisecond when idle. A nonblocking local socket pair wakes
an I/O worker when an executor queues its reply, and shutdown signals each live
worker through the same mechanism. Readable client sockets, writable queued
responses and listener activity also wake the worker. While a request is in
execution, its client socket is excluded from readiness polling to avoid spinning
on a disconnected client.

Each wait is bounded by the nearest connection deadline and at most 50 ms.
Connection counts, request/response memory budgets, request executors and frame
deadlines retain their existing limits. A full worker does not poll the listener
until it has room for another connection. Notification writes are nonblocking;
a full notification socket is already readable. Failure to create the wakeup
socket pair uses the earlier one-millisecond loop, as do non-Unix builds.
The existing locked libc version supplies the Unix poll interface; no new
library version is introduced.

The readiness guard covers idle waiting, listener activity, completion signals,
notification saturation and shutdown. Connection-sharing and fragmented-frame
tests continue to exercise the same production loop. Consensus workers use activity signals as described below.

A local October 29, 2026 release run of the 51-process complete-object workload
passed with this readiness path. It retained the same 16 data groups, three
metadata voters, eight clients and four 256-byte pages per one-KiB object as the
preceding run on `15262ea499c3ad162516c044b0c018e537f625b5`.

| Observation | Preceding polling run | Socket-readiness run |
|---|---:|---:|
| Initial 1,024 writes | 76.177 s / 13.4 objects per second | 61.303 s / 16.7 objects per second |
| Post-failover 256 writes | 17.898 s / 14.3 objects per second | 14.735 s / 17.4 objects per second |
| Initial write p50 / p95 / p99 | 622.217 / 866.056 / 990.861 ms | 493.545 / 701.542 / 764.310 ms |
| Post-failover write p50 / p95 / p99 | 557.602 / 796.297 / 843.047 ms | 460.356 / 756.156 / 875.186 ms |
| Reads after full restart, 1,280 objects | 16.651 s | 13.544 s |
| Complete recovery scenario | 130.54 s | 107.17 s |
| Server CPU core equivalents during a roughly 10 s sample | 7.54 | 2.66 |
| Server context switches per second during that sample | 286,302 | 80,667 |

Both runs verified every object after leader failures and full restart. The
samples covered the same 51 live server processes and 786 threads on one
16-logical-CPU WSL2 host. CPU is the change in process user/system ticks divided
by elapsed time; context switches sum the per-thread counters. These short
samples and single workload runs are observations, not statistical performance
guarantees, peak resource measurements or multihost qualification. The result
supports reducing polling overhead; substantial production-scale work remains.

## Peer socket readiness

Unix metadata and chunk Raft receivers now use the same bounded readiness wait
and shutdown notification mechanism as the API workers. When no frame made
progress, the receiver waits for readable peer sockets, listener activity when
there is a free slot, the earliest frame deadline, or shutdown. It does not watch
the listener while all sixteen frame slots are occupied, so a queued connection
cannot cause a busy loop. Non-Unix builds and wake-socket creation failures retain
a one-millisecond wait.

The packet protocol and resource bounds are unchanged: one frame per connection,
an 8 MiB individual packet limit, a 16 MiB incomplete-buffer budget, a 64 KiB
read allowance per connection per sweep, and the configured absolute frame
deadline. Completed packets still enter the existing bounded queue without
blocking; Raft remains responsible for retransmission after dropped packets.

The readiness guard fills all frame slots, queues another connection, checks
that waiting reaches its timeout, then verifies a shorter frame deadline, readable
peer data and shutdown notification. Existing guards still check malformed and
fragmented packets, total-buffer admission and shutdown with slow peers.

The subsequent local 51-process release run with peer readiness verified all
1,280 objects through leader failures and full restart in 87.11 seconds. Initial
1,024 writes took 47.133 seconds (21.7 objects/second), with p50/p95/p99 latencies
of 377.869/545.580/608.317 ms. The 256 writes after failover took 12.735 seconds
(20.1 objects/second), with p50/p95/p99 of 405.769/600.267/675.074 ms. Verified
reads took 8.628 seconds after leader failures and 10.129 seconds after full
restart. The workload and host were the same as the API-readiness run above.

A roughly ten-second sample covered all 51 stable server processes with 786
threads and measured 1.32 CPU core equivalents and 39,437 context switches per
second. The preceding API-only readiness sample measured 2.66 cores and 80,667
switches per second. These are single local observations, not randomized or
statistical comparisons, sustained production targets, or multihost evidence.

## Consensus activity waits

On Unix, both metadata and chunk consensus loops receive wakeup signals after
successful request admission and after a peer packet enters the bounded incoming
queue. A signal queued before the loop starts waiting remains readable, avoiding
a lost wakeup between checking a queue and sleeping. The loop immediately runs
another pass after admitting local work so a new proposal or read barrier is
processed without waiting for unrelated network traffic.

When there is no newly admitted local work, the loop waits until activity,
shutdown or its next 100 ms Raft tick. The shared readiness wait caps any single
sleep at 50 ms, which also bounds checks for background checkpoint completion.
A busy loop still checks elapsed tick time on every pass. The 32-request and
eight-packet channel capacities are unchanged, as are ordered application,
quorum read confirmation and handling of accepted operations after caller timeout.
Non-Unix builds and wake-socket creation failures retain waits of at most 2 ms.

The inbox guard checks admission bounds, signals queued before waiting, timeout
wakeup, a later producer and shutdown. The peer-receiver guard also verifies that
an admitted packet signals consensus activity before its consumer reads it.

The local 51-process release workload with activity-driven consensus waits
verified all 1,280 objects through leader failures and full restart in 52.22
seconds. Initial writes took 31.979 seconds (32.0 objects/second), with
p50/p95/p99 of 263.138/366.777/410.745 ms. Post-failover writes took 8.639 seconds
(29.6 objects/second), with p50/p95/p99 of 262.763/493.307/510.662 ms. Verified
reads took 1.167 seconds after leader failures and 2.039 seconds after full restart.
The preceding peer-readiness run above took 87.11 seconds overall, with 21.7 and
20.1 writes/second. These are single local observations for the same workload,
not statistical comparisons, production targets or multihost qualification.
