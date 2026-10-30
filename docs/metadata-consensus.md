# Network metadata consensus

Build the metaserver with `cargo build --features metadata-consensus --bin metaserver`.
This feature needs Rust 1.89 or newer and protocol build tools (Protobuf compiler,
Autoconf, Automake, and Libtool). One process hosts one durable MatrixRaft voter.
The metadata API port and the peer port are separate.

For a three-voter group, give every process the same peer map, group id, and initial
seed settings. Assign each process a different node id and existing state directory:

```sh
MATRIXOBJECT_METASERVER_ADDR=127.0.0.1:17200 \
MATRIXOBJECT_METASERVER_SEED_DATA_NODES=3 \
MATRIXOBJECT_METASERVER_RAFT_PEERS=1@127.0.0.1:18201,2@127.0.0.1:18202,3@127.0.0.1:18203 \
MATRIXOBJECT_METASERVER_RAFT_NODE_ID=1 \
MATRIXOBJECT_METASERVER_RAFT_STATE_DIR=/var/lib/matrixobject/meta-1 \
./target/debug/metaserver
```

Use node ids 2 and 3, their own API ports and directories for the other voters.
`MATRIXOBJECT_METASERVER_RAFT_GROUP_ID` defaults to 1.
`MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS` defaults to 3000 and accepts 100 through
30000 milliseconds. The fixed voter set supports one through seven nodes. Use an
odd voter count for fault tolerance. Do not combine this mode with
`MATRIXOBJECT_METASERVER_STATE_DIR`, which selects single-node journal storage.
Configuration without the required feature is rejected.

Only the current leader serves metadata calls. Reads, including probes and placement
planning, confirm a current-term quorum and wait for its commit index to apply
locally before reading the state. They append no log entries. Mutations prepare a binary
protobuf outcome against committed state, replicate it, and acknowledge only after
durable commitment and application. Followers and leaders without quorum refuse
calls. A timeout after proposal is ambiguous: the write may subsequently commit.
Do not automatically retry uncertain mutations. `MetaDirectory::open` and
`RemoteMetaClient::connect_catalog` accept one through seven metadata API endpoints.
A call tries each endpoint at most once. A typed `NotLeader` response (status 6,
empty payload) certifies that the request was not proposed or applied, so the client
can try another configured endpoint. A read can also move after a disconnect.
After any submitted mutation loses its response or receives an ordinary refusal,
the client returns that result without sending the mutation to another node.
Connection failures before submission can select another endpoint safely.

Single-endpoint `connect` remains available. Older clients do not understand the
new routing status and refuse it. All clients retire damaged connections before
another call; a read deadline covers the complete response frame, including its
header and payload. Catalog attempts use the configured connection and frame
limits per endpoint. Configure stable API addresses and one consistent Raft group;
a catalog must not mix unrelated single-node services.

`metadata.peer.base` stores the initial state and `metadata.peer.wal` stores binary
protobuf consensus records containing the same placement outcome bytes used by the
local journal. An exclusive lock protects the directory. Recovery replays committed
entries into fresh state. Missing or zeroed history is refused. A bootstrap digest
separates voters configured with different initial state. Existing stored bootstrap
state takes precedence over seed environment settings on restart. Do not delete,
copy between nodes, or independently replace these files in a live group.

The worker prepares bounded mutation batches and retains accepted proposals until
they commit or leadership changes, even if callers time out. Consecutive admitted
reads of mutable state share one quorum confirmation; later writes cannot pass that read group.
Only one read confirmation is outstanding, and caller timeouts do not discard it. Requests and packet
queues are bounded; network delivery has a total deadline. Packet loss is handled
by Raft retransmission. Storage or application failure stops the server. Shutdown
joins the consensus and network workers.

This is an integration stage, not a production-readiness claim. Without optional
peer TLS and [API TLS](api-tls.md), those transports are unauthenticated and
unencrypted; use an isolated test network. Membership changes, per-user
authorization and deployment-scale validation remain required. File-backed
checkpoint recovery is described below. The [external workload probe](distributed-probe.md)
can exercise an existing deployment and verify its objects after restart. Mutable-state reads use quorum confirmation
without appending entries. Group identity and the immutable bootstrap data-group
catalog are served locally by running voters; this does not report current health
or establish quorum. Mutations prepare changes against committed state and batch
ordered proposals; these paths still require deployment-scale measurement. The underlying WAL stops writes at its 512 MiB bound.

The process tests exercise three independent servers, leader termination, quorum
loss, complete restart, binary outcome replay, storage identity, election no-op
checkpoints, and shutdown with a slow connection. They do not establish power-loss
durability or large-scale performance.


## Configuring real chunk endpoints

Set `MATRIXOBJECT_METASERVER_DATA_ROSTER` on every metaserver before its first
start. Entries are comma-separated `node-id@socket-address@failure-domain`, for
example `chunk-a@10.0.1.10:17400@rack-a,chunk-b@10.0.2.10:17400@rack-b`.
Addresses must be concrete IP sockets with nonzero ports. IDs and domains use
1..128 ASCII letters, digits, dots, underscores or hyphens. Up to 4096 distinct
node IDs and endpoints are accepted; IPv4-mapped addresses are normalized before
duplicate detection. Domain names may repeat to describe shared failure domains.
The roster is sorted by node ID before allocator indices and the consensus
bootstrap identity are computed. Every peer must receive the same roster and
seed capacity, although entry order may differ.

Configured nodes start offline. A controller must verify each chunk endpoint and
publish its health through `heartbeat_placement_node` before it can receive new
placements. Automatic reporting and expiry are configured separately below. This prevents a configured but absent process from being selected
solely because its address appears in an environment variable.

`MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES` supplies initial capacity, which must
be positive for an explicit roster. The old `MATRIXOBJECT_METASERVER_SEED_DATA_NODES`
loopback bootstrap remains available for development, but cannot be combined with
an explicit roster. Neither mechanism changes a roster recovered from disk;
editing bootstrap settings is not a membership update. A live in-memory roster
also cannot be replaced by calling the seed method again.

The process test starts three metaserver voters and three journal-backed chunk
servers. It checks offline admission, canonical bootstrap across differently
ordered configurations, placement-selected star writes, leader loss, and full
restart recovery from binary outcomes. This validates the directory and chunk
client path. The HTTP object-server backend still requires separate integration.


## Automatic chunk health and expiry

On each chunk process set `MATRIXOBJECT_CHUNK_NODE_ID` to its roster ID and
`MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS` to 1..7 comma-separated metaserver API
IP sockets. `MATRIXOBJECT_CHUNK_ADVERTISE_ADDR` defaults to the bound socket;
set it explicitly when binding a wildcard address or advertising through a
different address. Every report must match the endpoint stored in metadata.
The binary heartbeat request uses code 71; older servers refuse it as unknown.
It does not change the existing heartbeat request or durable protobuf format.

`MATRIXOBJECT_CHUNK_HEARTBEAT_MS` defaults to 1000 and accepts 100..60000 ms.
For a Raft control plane, set `MATRIXOBJECT_CHUNK_METADATA_IDENTITY_FILE` to a
trusted canonical binary protobuf `MetaGroupIdentity` file (schema:
`proto/meta_identity.proto`). Each heartbeat connection and leader redirect is
checked against this configuration before sending a report. The file is read
once at startup with a 4096-byte limit; missing, malformed, oversized, or invalid
files stop startup. A different live metadata cluster is refused, with no fallback
to unverified reporting. Restart the chunk process to load a deliberately changed
identity. Without this option the existing unverified catalog behavior remains.
Embedding callers set `ReplicaHeartbeatOptions::expected_metadata_identity` to
`Some(identity)` for the same check, or `None` for the existing behavior. The
identity must come from trusted deployment configuration; fetching it from an
unverified endpoint does not establish that the endpoint is the intended cluster.

Reports carry the actual configured capacity and current page payload bytes,
including zero-filled space. The byte count is initialized once after recovery
and updated from the changed page on each mutation; reporting never scans all
pages. A failed journal causes reports to mark the node offline. Reports preserve
the metadata administrator's draining flag. Placement without a typed free-space
report subtracts the larger of physical usage and committed allocations from
capacity, so overlapping bytes are not counted twice. Invalid or partial configuration is
refused before the process begins serving.

Set `MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS` on each metaserver to enable expiry
(1000..3600000 ms, default 0 disables it). Use a TTL comfortably larger than the
report interval plus expected election, network, and storage delays, for example
10000 ms with 1000 ms reports. Maintenance checks at most once a second and uses
the normal durable mutation path; only a consensus leader can commit expiry.
Expiry records contain only changed nodes. An empty local pass appends nothing;
an empty consensus pass uses a compact read barrier. Clocks must be synchronized
across processes because heartbeat and expiry timestamps use UTC wall time.

Each reporting worker closes its metadata connection after one report, retains
the last successful catalog ordering, and observes shutdown between calls.
Connections have 100 ms connect/write and 500 ms whole-frame read deadlines.
No mutation with an uncertain reply is replayed on another node; a later interval
produces a fresh health measurement. Shutdown wakes interval waits immediately;
an in-flight call remains bounded by the configured catalog and client deadlines.
Abrupt termination is detected by TTL expiry, not by a graceful-offline message.

The six-process health test verifies automatic admission, used-byte updates,
metaserver leader loss, killed-chunk expiry, exclusion from new placement, and
journal-backed chunk recovery. Additional tests cover wrong endpoints, draining,
storage failure, binary request truncation, bounded shutdown, partial configuration,
and a single-node expiry within a 1024-node roster. These are functional checks,
not a large-scale throughput result. Heartbeats support [API mutual TLS](api-tls.md).
Full HTTP integration, per-user authorization and membership changes remain
separate work. Network chunk Raft is described in `chunk-consensus.md`.

## Application checkpoint recovery

After upgrading every voter, set `MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES`
to enable automatic capture, for example `33554432` (32 MiB of new WAL history).
The default is `0`, which disables capture for a controlled rolling upgrade.
Nonzero values must be between 1024 and 134217728 bytes. Library callers use
`checkpoint_after_bytes` in their Raft options. The setting does not change group
identity and may differ between voters; every voter must support restoration.

Capture runs on the exclusive application worker after earlier proposals and
reads settle. The canonical binary protobuf envelope in `proto/node_checkpoint.proto`
binds service kind and exact applied Raft position to the validated binary service
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

Metadata capture now uses the version-2 binary protobuf envelope documented in
`proto/metadata_checkpoint.proto`, carrying service encoding 18 and explicit
roster shape 33. Existing version-1 images remain readable. The file limit is
16 GiB on a 64-bit host; existing per-string and collection decoder limits remain.
Capture counts encoded bytes, streams through the decoder to check recoverability,
then streams the checked state into the durable payload. Unsupported state retains
WAL history and retries after another configured interval; the WAL still stops
writes at 512 MiB.

Transport buffers use 64 KiB pieces and a one-slot channel. Encoding uses scratch
space per record, while application maps remain resident. Snapshot export clones
state; validation temporarily holds source and decoded maps. Snapshot export and final WAL publication
are synchronous and can delay requests. This is not a constant-memory application
store or a production-readiness claim.

If capture is enabled, disable it on every voter before a rolling upgrade. Upgrade
all voters to support version 2 before re-enabling capture. Older application
readers cannot restore these images; do not downgrade after version-2 capture.

The explicit release test `metadata_stream_recovers_more_than_256_mib` recovers
272 MiB of text records after a durable single-peer restart. It does not establish
large-state multi-host recovery or throughput.

Process tests exercise repeated compaction, catch-up after an offline interval,
and total restart. Chunk tests transfer state larger than one peer frame and
check subsequent writes, page state and capacity enforcement. Metadata tests
check placements and health roster recovery. These are functional fault tests,
not power-loss or multi-host throughput measurements.

## Conditional placement updates

`RemoteMetaClient::commit_object_placement_if_current` sends canonical binary
protobuf request code 73. Pass `None` to require an absent placement, or supply the
complete placement record last read. The current record must match every field,
including its node list, length and update time. The proposed record keeps the
same object identity, uses a newer positive version, and must still satisfy the
normal plan-checksum and capacity checks. A stale condition changes neither the
placement nor its committed-byte accounting.

Standalone persistence and Raft use the existing binary protobuf outcomes.
Within a consensus batch, later conditions see earlier admitted updates. Only
one of two competing updates against the same prior record can succeed. A lost
reply is still uncertain and is not automatically replayed on another catalog
endpoint. Read current state before resolving that uncertainty.

This conditional commit is independent of data writes. It does not create a
transaction across metadata and chunk groups; callers must coordinate publication
and retain data needed by an unresolved operation. Upgrade all serving metadata
nodes before using code 73. The payload schemas for both conditional operations
are in `proto/conditional_requests.proto`.

## Verified metadata catalogs

Use `RemoteMetaClient::connect_verified_raft_catalog` with a trusted
`MetaGroupIdentity` to check the first connection and every redirected or
reconnected connection. The identity includes the configured group ID, sorted
voter IDs, corresponding peer addresses, and the effective Raft ID derived from
the persisted bootstrap image. `connect_raft_catalog` checks a supplied numeric
group ID and then pins the first voter's complete identity; it cannot establish
that the first endpoint is the intended deployment when IDs are reused.

`metadata_group_identity` reads request code 74 directly from the connected voter.
Its canonical binary protobuf schema is `proto/meta_identity.proto`. Followers
can answer without a voting majority. This describes configuration, not current
leadership, readiness, or authentication. Standalone nodes refuse the request.
The older `connect_catalog` API remains unverified for existing callers; use the
verified constructor when configuring a distributed deployment. Upgrade all
metadata voters before enabling verified clients.

A changed configuration stops the call before its data request is sent. A lost
mutation reply remains uncertain and is not replayed. Identity checks do not
make separate metadata clusters interchangeable or authorize changing membership.

## Large metadata transfer validation

`metadata_group_stream_transfers_more_than_256_mib` runs three durable MatrixRaft
peers with a 272 MiB metadata fixture. It isolates one voter, captures the leader,
drops a file-transfer packet, duplicates subsequent file packets, and verifies
catch-up plus a later mutation. It then reopens all three peers and validates
metadata checksums and restored state. The 9 MiB companion runs without an ignored
test flag. CI selects both tests explicitly.

These tests generate deterministic fixture mutations directly in their harness;
they exercise the production checkpoint codec, peer packets, storage and replay,
but not the metadata request API, separate processes, or multi-host networking.
Existing process tests cover placement and health operations on smaller state.

## Background checkpoint capture

Each node keeps at most one capture worker. At a settled applied position it
exports an immutable snapshot, then moves serialization, capture validation and
payload file sync to that worker. Peer traffic and later writes continue while
capture runs. The completed payload is published at its original applied
position, retaining every later log entry for recovery. A newer received
checkpoint can supersede the worker result without replacing newer state.

Incoming file checkpoints also have one finalization worker per peer. The last
fragment starts payload copying, checksum validation and disk sync off the Raft
loop; both node loops keep polling ready work on their regular timer. Completion
is acknowledged only after validation, and an older-term result is discarded.
Invalid transfer payloads do not stop a healthy voter. Shutdown retains the peer
storage lock until this worker finishes. Fragment staging, WAL publication and
application restoration remain synchronous.

Live installation reuses the payload already validated and synced by the receive
worker, avoiding another full-file checksum scan on the Raft loop. Received
checkpoints retain their commit-position floor and exact manifest checks.
Restart recovery still validates the complete payload before restoring it.

Over-limit snapshots keep WAL history. Worker failures stop the node rather than
silently treating a failed capture as durable. Orderly shutdown joins the worker
and discards an unpublished payload. Snapshot export copies collections on the consensus loop; checksum sealing and
chunk compression statistics run on the worker. Final WAL replacement and sync,
and incoming snapshot restoration still
run synchronously. This change does not promise a bound on request latency or
constant memory; later writes can also fill the WAL before capture completes.

Object manifests are now included in the captured metadata service state.
Empty catalogs retain snapshot version 18; populated catalogs use version 19
with a checksummed binary protobuf catalog. Checkpoint restore installs the
catalog, preserving object generations and deletion tombstones. All readers must
understand version 19 before it is written. Version-2 placement outcomes now bind manifest and placement changes atomically
and replay through the journal and Raft apply paths. Manifest reads and conditional publication are available over binary protobuf
requests. Publication validates configured group names; callers still own durable
extent verification and immutable writes. Distributed HTTP routing remains work
in progress.

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

## Automatic health reports during clock rollback

When deciding an automatic health report, metadata uses the greater of its wall
clock and that node's retained heartbeat timestamp. A backward clock adjustment
therefore does not reject a fresh report as stale. Equal timestamps are allowed;
the server does not increment the retained value or advance it artificially.
The floor comes from restored or projected metadata state, so it survives replay,
snapshot recovery, and leadership changes without a new persisted format.

Explicitly timestamped heartbeat requests still reject older timestamps. Node
identity, endpoint, capacity and outcome checks remain unchanged. Expiration still
uses wall time: a rollback can delay expiry, and synchronized metadata clocks
remain necessary. This protects automatic report ordering, not a timing lease or
a guarantee that quorum remains available during host stalls.

## Authenticated peer transport

Optional [mutual TLS](peer-tls.md) protects metadata and chunk Raft peer ports.
Without that configuration peer traffic remains plaintext. Client API transport
and authorization remain separate requirements.

### Worker observations on request timeout

Consensus deadline and disconnected-worker errors include both the request's
last observed phase and the shared worker's last observed phase with its elapsed
monotonic milliseconds. Worker phases distinguish peer packets, application-ready
processing, checkpoint restoration, checkpoint publication, request processing,
idle waits and shutdown; chunk voters also report state-lock waits. Phase and
start time are sampled as one atomic observation without acquiring the service
state lock. These are diagnostics only, not proof of liveness, cancellation or
safe replay. A timed-out write still has an uncertain outcome and must not be
automatically replayed. Normal replies and durable formats are unchanged.

A durable checkpoint record can become visible before application restoration
finishes. A recorded checkpoint is not a voter-readiness certificate. Use quorum
operations to establish service availability after recovery.


### Optional object operation deadline

`MetaClientOptions::deadline` and `ReplicaClientOptions::deadline` accept an
absolute `std::time::Instant`. The default `None` preserves the existing
per-connection and per-frame timeout behavior. When set, the earliest limit
bounds connection attempts, catalog rotation, identity checks, request writes,
and response reads. Numeric IP endpoints (including a port) are required for
deadline-bound connections because synchronous hostname resolution cannot be
bounded by the socket timeout.

For `read_object`, the replica option supplies a shared deadline for metadata
lookup and all data reads. For `write_object`, `options.replica.deadline` also
covers placement, allocation, data writes, verification, publication, and any
confirmation read. An earlier metadata-client deadline still wins. The temporary
object deadline is restored on return, including routing to another metadata
endpoint. A new operation should supply a new absolute deadline.

Expiry before publication returns `BeforePublication` with any staged manifest.
Expiry after publication is attempted retains `PublicationUnconfirmed` unless
the exact publication was confirmed; it never authorizes replay. Staged pages
are not automatically reclaimed. The deadline bounds network waits, not CPU
execution or operating-system scheduling. It does not cancel server-side work.
Callers constructing either option struct without a default update must now
provide `deadline: None` or an explicit deadline.


### Concurrent immutable data-group writes

`ObjectWriteOptions::max_parallel_groups` defaults to 4 and accepts 1..4.
After metadata placement, identity validation of every selected group, and page
allocation, the writer groups candidate extents by data group. It writes and
verifies up to that many groups concurrently, with one connection and one worker
per active group. A single-group batch runs on the calling thread. Set the limit
to 1 for serial execution. Applications must also bound their number of concurrent
object operations; this is a per-operation limit, not a process-wide worker pool.

The workers borrow the input bytes and immutable candidate manifest. Within each
group, each page is conditionally written once and then read back to verify its
bytes and sequence. All workers share the same optional absolute deadline. The
manifest is published only after every group succeeds. On a group error, worker
startup failure, or worker panic, all started workers are joined before returning
an unpublished staged manifest; later batches do not start. A sibling group may
have durable staged pages even when the operation fails. No mutation is replayed
and no staged page is automatically reclaimed.

The nine-process recovery checks exercise both serial and concurrent execution
through updates, stale generations, concurrent publication, leader loss and full
restart. A separate two-group refusal check confirms that a healthy sibling
finishes while another group exhausts its page capacity, with no manifest becoming
visible. Wire messages and binary durable outcomes are unchanged. Exhaustive
`ObjectWriteOptions` literals must supply the new field.


In one before/after local WSL run with 51 processes, mutual-TLS Raft peers,
16 three-voter data groups, three metadata voters, four writers and one-MiB
objects striped across two groups, initial write throughput increased from
7.7 to 9.7 objects/s. Median write latency changed from 494769 to 405153 us,
and p99 from 747370 to 623076 us. The post-leader-loss batch changed from
7.0 to 8.9 objects/s. Both runs recovered and verified all 160 objects after
full restart. These are single-run observations on one PC, not a production
throughput guarantee or evidence of independent host failure domains.


### Four-way object recovery validation

The regular network chunk suite includes
`four_way_authenticated_objects_recover_after_leader_loss_and_restart`: 15 local
processes, four three-voter data groups, three metadata voters, and objects with
one immutable page in each of four groups. It checks exact bytes, generation,
placement membership and physical/committed capacity after leader loss, further
writes and total restart. Raft peers use mutual TLS. Each complete object read or
write in the multi-group workload now has a fresh 10-second absolute network
deadline in addition to the individual connection and frame limits.

Run the larger local WSL case explicitly:

```sh
cargo test --release --locked --all-features --test network_chunk_consensus authenticated_sixteen_groups_recover_four_way_objects -- --ignored --nocapture --test-threads=1
```

One local run with 51 processes, 16 data groups and four writers completed the
initial 128 one-MiB writes at 10.7 objects/s (median 350027 us, p99 632830 us).
After losing the leader of every group and the metadata leader, another 32 writes
completed at 9.9 objects/s. All 160 objects then recovered after total restart.
Post-restart verification measured 123.71 MiB/s, with object-read median 45738 us
and p99 116194 us. The reported verified throughput includes payload comparison
and placement checks; read percentiles time the object read itself. Results are
single-run observations on one PC, not multi-host production guarantees. Client
API authentication remains a separate requirement from peer TLS.
