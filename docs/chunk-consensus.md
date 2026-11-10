# Chunk servers with independent Raft voters

Client digest probes use replica opcode 24 for up to eight ranges under one
confirmed Raft read. The existing pipelined APIs retain their return types.
A missing-page batch falls back to individual probes when the caller needs
per-page absence. An unknown opcode is cached for that socket and uses the
scalar pipeline until reconnection. Other refusals and malformed reply counts
remain errors. Batch reads participate in catalog leader routing and the
caller's configured deadline; uncertain mutations are never replayed.

Standalone chunk servers use coordinator-driven star writes by default. Build
`chunkserver` with `--features chunk-consensus` and supply complete Raft
configuration to run a consensus group. Each group process owns one MatrixRaft
`DurablePeer`, its own page state, and its own existing storage directory. A Raft
group accepts requests through its current leader; it does not accept star writes
independently at every voter.

Example for voter 1 of three:

```sh
MATRIXOBJECT_REPLICA_ADDR=127.0.0.1:17301 \
MATRIXOBJECT_CHUNK_RAFT_NODE_ID=1 \
MATRIXOBJECT_CHUNK_RAFT_GROUP_ID=1 \
MATRIXOBJECT_CHUNK_RAFT_PEERS=1@127.0.0.1:18301,2@127.0.0.1:18302,3@127.0.0.1:18303 \
MATRIXOBJECT_CHUNK_RAFT_STATE_DIR=/existing/chunk-1 \
MATRIXOBJECT_CHUNK_RAFT_TIMEOUT_MS=5000 \
cargo run --features chunk-consensus --bin chunkserver
```

Use a distinct API address, node ID, and directory for each process. Every voter
has the same peer catalog, group ID, capacity, and page-count bounds. The catalog
contains 1..7 distinct IP sockets with nonzero voter IDs and ports; wildcards,
multicast addresses, and duplicate sockets including mapped aliases are refused.
The request timeout defaults to 5000 and accepts 100..30000 milliseconds. Partial configuration and
combining Raft state with `MATRIXOBJECT_REPLICA_JOURNAL` are refused.

## Durability and request behavior

Write batches begin from committed state and encode writes and discards as `MOPO`
binary protobuf page outcomes. The peer WAL syncs its Raft records before the
outcome is applied and acknowledged. Recovery applies decided outcomes; it does
not execute original write commands. Reads, inventories, heads, and pings require
a current-term quorum confirmation and application through its commit index,
so an isolated minority cannot answer from stale state. These reads append no log
entries. One confirmation may be pending, including after a caller timeout.
A code-18 head batch confirms up to eight ordered page heads, including explicit
absence, with one Raft read barrier for the whole batch.
A code-17 protobuf request groups up to eight page ranges of at most 64 KiB each
and 512 KiB total into one read command. The ordered protobuf reply is checked
against the configured reply budget before page bytes are copied. The group
therefore performs one quorum confirmation for the whole bounded read batch;
larger ranges retain the scalar path.
A timeout does not mean a submitted write was rolled back: it may commit later.
The pending batch remains serialized until commit or a leadership change.

A chunk leader groups up to 32 queued mutations into one proposal admission,
with at most 4 MiB of encoded outcomes. Each outcome is prepared against the
preceding successful speculative outcome; live state is unchanged until commit.
Preparation retains the isolated candidate already produced for each write,
without decoding its outcome again. Reads and membership commands end the batch,
and a request that exceeds the remaining byte budget is deferred in queue order.
Expired requests are refused before admission. No batch waits for new arrivals.
An individually oversized outcome retains the existing proposal-size refusal.
Every admitted request receives its own committed result; admission alone sends
no success reply, and uncertain writes are never automatically replayed.

Page outcome construction checks the exact protobuf length without serializing
a temporary payload. Final encoding allocates the complete envelope once. This
preserves the envelope size limit and wire bytes; canonical decoding still
checks the encoded representation. `outcome_encoding_cost` checks allocation
volume for 64 KiB, 1 MiB and 4 MiB full-page changes, exact wire bytes, replay,
and the envelope boundary. These are allocation checks, not throughput results.

Applying an outcome with no byte patches and unchanged data length retains the
existing immutable page buffer. Sequence and other metadata still advance, and
checksum, capacity and installation checks still run. Byte patches and length
changes use an isolated buffer so rejected outcomes cannot alter live data.
The allocation test also checks buffer sharing, changed-byte isolation and
failure atomicity for 64 KiB, 1 MiB and 4 MiB pages.

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
are refused without changing committed page state. Candidate preparation shares
page payloads and receipts while cloning their indexes. It starts with empty
runtime read caches and validation indexes, leaving the live service caches
unchanged. Configuration, capacity checks, mutation controls and physical counters
are retained; binary outcomes are unchanged. Index cloning still scales with the
number of stored pages. Quorum-confirmed reads avoid additional log entries.
Write history can use the opt-in application checkpoint policy below;
large-scale measurement remains required.

The ignored release test `measure_raft_preparation_with_warm_read_caches` compares
complete single-write preparation with and without copying the caches and checks
identical outcomes. A local WSL run with 4,096 pages of 4 KiB and 16 MiB of warmed
reads measured median preparation of 8.32 ms with cache copying and 0.36 ms without
it over 30 samples. This is preparation timing on one shared host, not distributed
write throughput or a latency guarantee.

The process tests cover three independent voters, acknowledged writes and
discards, leader loss, refusal below quorum, restart of all processes, capacity
refusal, storage guards, shutdown, and metadata reports. They are functional
fault tests, not a large-scale throughput result. Metadata placement of Raft groups
is described in `chunk-group-catalog.md`. Binary APIs support optional
[mutual TLS](api-tls.md), including replicated load and recovery validation.
New standalone groups support [committed voter replacement](metadata-consensus.md#chunk-voter-replacement-for-new-groups) with distinct node identities.
Placement-catalog membership changes use committed fences and verified controller refresh,
as described in [chunk-group routing and membership](chunk-group-catalog.md). Repeated voter
rejoin tests cover multiple replacement rounds with intervening writes, leader loss and full
restart. Intermittent leadership and deadline failures in the 67-process workload remain
unresolved; passing bounded runs do not establish sustained availability.

The HTTP distributed backend and per-user authorization remain unfinished.
File-backed checkpoint recovery is described below. To exercise already-running metadata and data quorums,
use the [external workload probe](distributed-probe.md); it supports a separate
read-only verification run after an operator-controlled restart.

## Larger striped-object recovery workload

The ignored release test `sixteen_groups_recover_one_mib_striped_objects` uses
16 three-voter data groups and three metadata voters: 51 local processes.
Eight clients write 128 one-MiB objects, each as four 256-KiB pages striped over
two groups. Every group must hold data and create application checkpoints.
The test stops all 17 leaders, verifies the original objects, writes 32 more,
restarts every process and verifies all 160 objects byte for byte. It also checks
manifest generation, page sequence, group placement, physical usage and committed
capacity accounting. Payload bytes are a deterministic repeating pattern with an
object-specific prefix. Mutation failures are not replayed to make the run pass.

An October 30, 2026 release run on the local WSL setup passed in 25.63 seconds:

| Phase | Objects | Elapsed | Rate |
|---|---:|---:|---:|
| Initial writes | 128 | 13.112 s | 9.8 objects/s |
| Reads after leader loss | 128 | 0.598 s | — |
| Writes after leader loss | 32 | 2.775 s | 11.5 objects/s |
| Reads after full restart | 160 | 0.858 s | — |

The final corpus is 160 MiB of logical object data, replicated across three
members per selected group. This is one bounded functional and timing run on a
shared host, not a throughput guarantee, a measured memory bound or a substitute
for larger deployments. The existing 1-KiB, 1,280-object workload remains available
to exercise more manifests and pages. CI runs a smaller 12-process, 64-KiB-object
case named `striped_payloads_recover_with_exact_bytes_and_capacity_accounting`.

```sh
cargo test --release --locked --all-features --test network_chunk_consensus coupled_groups::sixteen_groups_recover_one_mib_striped_objects -- --exact --ignored --nocapture --test-threads=1
```

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

The page-stream decoder returns an owned, internally validated snapshot. Raft and
standalone journal recovery consume that value when installing state, avoiding a
second scan of every payload byte. The validated value has no mutable access.
Public snapshot installation still validates its input, and journal header/body
checksums and configured capacity checks remain mandatory. This does not change
the persisted format or make recovery asynchronous.

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

`RemoteReplicaSet::read_chunk` contacts a rotating read quorum first and requires
exact agreement on write sequence, checksum and returned bytes. Optional members
are contacted only when failure or disagreement prevents that first wave from
deciding. Voting moves each payload into its vote key instead of cloning it.
Matching duplicates are dropped, and the winning payload is moved into the result.
All waves share one absolute response budget; a stalled first wave cannot grant
an optional member a fresh timeout. When only part of the first wave answers, the
collector briefly drains near-simultaneous replies, retires an incomplete socket
and hedges to an optional member inside the remaining budget.

For `q` successful quorum replies containing `n` bytes each, the healthy path
retains about `q * n` bytes. A fallback wave can raise that toward the configured
replica count. Vote-table nodes, socket buffers and decoding overhead are
additional. This is not a streaming read or a bound on total process memory. The
allocation-retention guard also verifies that equal checksum strings cannot hide
differing payload bytes.

## State-fenced star writes

`RemoteReplicaSet::write_chunk` learns support for complete-state conditional
writes with one read-only capability request per connected set. When every
connected member supports it, a rotating read quorum selects the complete page
head, or agrees that the page is absent. If the configured write quorum is
larger, preflight requires that larger quorum so an impossible write is refused
before mutation. The mutation is then sent to every connected member with that
expected sequence and state checksum. Each server
compares the complete state under the same lock that admits the write, so an
optional member with equal sequence but different length, bytes or discarded
ranges refuses the mutation. A healthy 2-of-3 write therefore uses two head
requests and three mutation requests after capability discovery is cached,
instead of three head requests and three mutations.

Capability request code 19 is read-only. State-conditional write code 20 uses
canonical bounded protobuf and carries at most one MiB of data. If any connected
member does not advertise the new verb, the coordinator keeps the established
path: it reads every member's head and sends sequence-conditional request code 8
only to members whose complete state matched the selected quorum. There is no
mutation downgrade after code 20 is sent. An uncertain mutation is not retried,
and partial application can still occur if a member fails after preflight.
Callers must resolve uncertain results before issuing another operation.

The conditional mutation is sent to every matching member before responses are
polled. Once a write quorum has acknowledged the terminal write or discard frame,
the coordinator briefly drains already-arriving replies, then returns without
waiting for slower extra replicas. The short drain keeps healthy near-simultaneous
sockets hot. Any incomplete exchange retires that socket and is never replayed.
The next operation reconnects failed members in waves of up to eight concurrent
connection attempts and repeats the head fence. Healthy sockets do not create
reconnect work, and a member whose result was not observed cannot vote for another
mutation unless its complete page state matches the selected quorum. The returned
count contains acknowledgements observed before return; it may equal the quorum
even when a slower member applies the request after its reply socket is retired.

`RemoteReplicaSet::write_chunks` admits up to thirty-two ordered writes. One bounded
code-18 binary protobuf request asks each member for up to thirty-two distinct page
heads. A typed refusal from an older server safely falls back to groups of eight
because this phase is read-only. The coordinator votes every page independently,
then groups writes by the members matching their selected state. Repeated writes
to one page carry consecutive expected sequences in input order, so up to thirty-two
appends can share one head fence. Each group is partitioned into code-14 binary protobuf
mutation frames of at most eight writes and 4 MiB of page data. Eight small writes
with the same matching set use one head frame and one mutation frame per member.
Eight default one-MiB writes use one head frame and two four-write mutation frames
instead of eight head frames and eight mutation frames. A typed unknown-request refusal permits
the read-only head phase to fall back to scalar fencing before any mutation is
sent. A group is successful only when its complete batch is acknowledged by a
write quorum. Frames are sent without replay; an error can follow an earlier
acknowledged frame, so candidate pages must be probed before retrying.

Metadata-routed standalone groups use identity version 3. Their fixed `voters`
numbers correspond positionally to `api_endpoints`; peer endpoints and bootstrap
fields are empty because these members do not run chunk Raft. Object writes use
the same majority for the read fence and mutation quorum. They admit up to 32
fresh pages per coordinator call while retaining the eight-write, 4 MiB mutation
frame bounds. Identity versions 1 and 2 remain Raft routes.

Fresh page batches are sent to every eligible member concurrently. The client
acknowledges a batch after the write quorum has applied it. A slower optional
member can complete within the bounded drain; otherwise its connection is
retired and inventory repair can restore that copy. The write result counts
durable acknowledgments, not the number of attempted sends.

The check prevents an already divergent member from counting toward a fresh write
acknowledgment. It does not repair that member, establish cross-client linearizability
or authenticate replies. Explicit repair is available through `repair_chunk` and
`repair_inventory_batch`; see [maintenance and repair](configuring.md).
Raft-group writes continue through their leader and do not use this star fan-out.

## Bounded object-write admission

Replica request codes 14 and 16 carry the same canonical binary protobuf with at
most eight conditional page writes and no more than 4 MiB of page data. Code 16
returns an ordered canonical protobuf vector containing each committed range's
write sequence, length and SHA-256 digest. The object writer uses code 16 only
for pages of at most 64 KiB and starts with up to eight real writes. This first
request follows leader routing, discovers support and verifies the stored bytes
without an empty capability or later digest exchange. An older server returns a
typed unknown-request refusal before admission, which permits the client to use
code 14 or its bounded individual pipeline followed by digest or full-byte
verification. A server implementing the earlier four-write bound returns a typed
invalid-batch response before admission; the client remembers that bound for the
connection and sends four-write groups. Larger pages retain the individual path.
Empty code-14 requests remain harmless for older upgraded clients.

For a version-3 standalone route, the object writer instead uses the conditional
star batch and derives the expected digest only for a fresh whole-page write.
Offset appends and nonzero starting sequences are refused on this path. A cached
fixed-member set remains reusable after a member transport failure; later
operations attempt to reconnect that member but can proceed when the configured
majority still agrees. Initial construction of the public `RemoteReplicaSet`
continues to require every configured endpoint.

On a Raft voter, each accepted member becomes an independent conditional write
in the consensus inbox. Adjacent writes can enter the existing proposal batch.
One batch response is successful only after every member has a confirmed reply;
code 16 exposes no partial digest vector. An uncertain member or mixed leadership
result refuses the whole response.
The members are not an atomic transaction. Failed publication never replays an
uncertain write. A standalone journal node applies the bounded batch under one
lock, appends one binary outcome per accepted page, and performs one durability
sync before replying. At the maximum batch width this changes eight durability
syncs into one without changing the eight recoverable outcomes. If a later
member is refused, the accepted prefix is synced before the error is returned.
Recovery can therefore observe that prefix;
the shared sync reduces write amplification but does not make the pages atomic.
This reduces request admission overhead for small object pages; it does not
reduce replicated page payload bytes or physical storage writes.

## Reusing verified object-read connections

Complete object reads retain up to eight successfully verified data-group
connections on the metadata client, matching the bounded group-read window. A
later read reuses each connection only when the placement identifier and the
complete route still match and the connection accepts the new timeout and
deadline options. The metadata route remains confirmed for every read. A
changed route, unusable socket or failed read drops that cached connection and
requires a new group-identity handshake.

Multi-group reads keep their existing bounded parallelism: an uncached group
connects and reads in its own worker, while a route-matched cached group begins
on the retained socket. Successful workers return their connections to the
bounded cache; no failed or incomplete exchange is reused. Repeated reads of up
to eight groups therefore perform one data connection and identity handshake
per stable group instead of repeating both for every object, without adding
mutation retries or weakening manifest sequence, length and checksum validation.

## Star head-read scheduling

`RemoteReplicaSet::head_chunk` contacts a rotating quorum first. It asks optional
members only when the first wave cannot produce exact agreement on logical length,
write sequence, payload checksum, state checksum and discarded ranges, or cannot
produce an absence quorum. Complete-state reads and repair use the requested
quorum width for the same scheduling. A 2-of-3 healthy head therefore sends two
requests instead of three and does not wait on the optional member.
Primary and optional waves share one absolute response budget. If the first wave
uses that budget, the optional request is not sent after the operation has expired.
If only part of the first wave answers, a short drain preserves near-simultaneous
replies before the optional request is sent within the remaining budget.

The socket tests cover rotating healthy waves and a split first wave that requires
the optional member. Full-head voting, absence detection, distinct-peer checks,
per-connection deadlines and failure handling retain their existing semantics.
No worker threads, mutation retries or new protocol messages are introduced.
Repair still validates complete bytes and rechecks its source quorum before
staging and committing each destination.

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
unreachable member can add that delay only while the client lacks a serving
quorum. With a healthy quorum, up to eight bounded workers finish optional
initial connections and later reconnects outside the operation's critical path.
Routed construction returns as soon as the configured read and write quorums are
connected, and below-quorum recovery stops waiting as soon as service is restored.
Every unfinished endpoint remains part of the fixed membership and is collected
or retried by later operations.

The Linux mutual-TLS regression
`routed_star_tls_connect_returns_when_quorum_is_ready` starts two serving replicas
and holds a third accepted socket before its handshake response. The optional
member has a four-second connect timeout; routed construction must return through
the two-member quorum in under two seconds and keep the unfinished member tracked.

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

Replica request code 17 carries up to eight ordered ranges in canonical binary
protobuf, with at most one MiB per range and 4 MiB per frame. A distributed
star coordinator partitions eight default one-MiB pages into two four-page
frames, sends each frame to a rotating read quorum and votes each returned page
independently by sequence, checksum and exact bytes. Optional members are used
when the first wave cannot decide every range. An older member, a tighter reply
budget or a page set that is not present on the same members selects scalar reads
so every page can still form its own quorum; reads are safe to repeat. Raft
clients keep the conservative large-page pipeline described by their
leader-routing policy and do not opt into these wider frames.

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
Write batches use separate admission and never share a read confirmation.
Accepted writes still wait for application or leadership change even if a caller
times out.

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
shutdown or its next 300 ms Raft tick. This places idle Raft heartbeats about
600 ms apart and lengthens leader replacement after a stopped voter. The shared
readiness wait caps any single sleep at 50 ms, which also bounds checks for
background checkpoint completion.
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

### Outbound peer packet age

Each outbound packet has one 100 ms delivery deadline starting when the consensus
worker queues it. Queue waiting, TCP connection and both frame writes share that
deadline. Expired packets are discarded before connecting, and partial delivery
closes the connection. Raft retransmission supplies later packets; the sender does
not replay a failed packet or extend its deadline. Per-peer queues remain bounded
to eight packets so an unavailable voter cannot block consensus admission.

## Leadership changes during read confirmation

If a quorum read loses its leadership term before application execution, the
server sends the typed `NotLeader` response. Catalog clients can then try another
configured voter within the existing single-pass routing bound. Storage,
transport, and other consensus errors retain their ordinary error response;
matching error text never permits routing. Accepted writes retain their uncertain
outcome handling and are not replayed after leadership loss. This does not add
an election wait or guarantee availability while every voter refuses a request.

## Request deadline diagnostics

Consensus response errors distinguish a deadline from a disconnected worker and
include the last observed request phase: queued, preparing, read confirmation,
or proposal completion. Each admitted request retains one shared atomic byte
until its worker and waiting caller release it. The markers do not change queues,
consensus decisions, request deadlines, or durable formats.

Read failures state that no mutation was submitted. Mutation failures remain
uncertain in every phase, including queued: the worker can advance concurrently
with the diagnostic observation. Never infer replay permission from the phase
or its text. A caller timeout does not cancel a pending proposal or read barrier.
The phase narrows investigation; it does not prove that CPU, storage, networking,
or quorum availability caused the delay.

## Authenticated peer transport

Optional [mutual TLS](peer-tls.md) protects metadata and chunk Raft peer ports.
Without that configuration peer traffic remains plaintext. Client API transport
and authorization remain separate requirements.

### Bounded star request and response collection

Star fan-out encodes one immutable request frame shared by all selected members.
Each member advances its send by at most 64 KiB per sweep using nonblocking I/O.
A member that stops reading cannot prevent healthy members from receiving the
request. Its write deadline starts when the exchange is prepared; its response
deadline starts only when the entire request has been sent. An optional absolute
client deadline clamps both stages.

Replies also advance in bounded 64 KiB sweeps. Sweeps that transfer bytes continue
immediately. When no member advances on Unix, one `poll` waits for pending sends
to become writable or pending replies to become readable, bounded by the earliest
exchange deadline and a 50 ms wake interval. This removes the fixed one-millisecond
timer from healthy low-latency fan-out. Non-Unix builds retain that timer fallback.
Waiting on earlier members never grants later members a fresh timeout. All selected
exchanges are resolved or expired before returning, with results kept in membership
order and the same quorum checks. Partial requests and incomplete or invalid
responses retire their sockets; fully decoded replies restore blocking operation
for reuse. An expired partial write can still have reached the server and is never
automatically replayed.

No extra worker threads are created. Pending sends share one encoded frame, and
each pending response retains at most its configured payload bound plus its frame
header. Reconnecting failed sockets still happens before the exchange and uses
the configured connection limits.

An ignored release measurement compares construction of the same 8,384,512-byte
conditional batch using owned page copies and a copied shared slice against
borrowed page bytes encoded into one shared vector. It alternates the two paths
over 24 samples per run. Four WSL runs recorded median construction times of
22.5–25.9 ms for the owned path and 6.2–7.6 ms for the borrowed path. This
isolates local encoding and memory copies; it does not measure socket sends,
metadata work, durable sync, or end-to-end object latency. Reproduce it with:

```sh
cargo test --release --lib --features chunk-consensus,metadata-consensus star_frame_construction_measurement -- --ignored --nocapture
```

A Linux socket test restricts the first member's receive window and withholds
reads while an 8 MiB mutation is sent to three members. Both healthy members must
receive and acknowledge the complete request before the stalled member's send
timeout. The test checks a partial frame at the stalled member and no replay at
the healthy members. It fails with the former sequential send implementation and
passes with bounded send sweeps in local WSL. A separate socket test waits on a
pending reply and verifies that readiness wakes the fan-out collector.
