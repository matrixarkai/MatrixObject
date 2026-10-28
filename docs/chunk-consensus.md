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
reads settle. The canonical binary protobuf envelope in `proto/node_checkpoint.proto`
binds service kind and exact applied Raft position to the validated binary service
snapshot. Recovery restores that state before applying later protobuf outcomes.
Metadata restoration rebuilds the placement roster and checks the immutable group
catalog. Chunk restoration preserves page contents, discards and receipts, checks
configured capacity/page limits, and reconstructs physical used-byte accounting.

MatrixRaft publishes a synced replacement WAL atomically while retaining the
uncommitted suffix. A lagging voter receives the checkpoint through bounded peer
fragments; it persists and restores application state before acknowledging Raft
progress. Small checkpoints use inline records; larger ones use segmented records.
Keep the `.runtime.lock` sidecar together with the WAL. Do not downgrade a voter
after checkpoints have been enabled: older runtimes refuse these records.

The encoded application checkpoint is limited to 256 MiB and remains memory-backed.
Capture and restoration can temporarily retain several copies and delay requests.
If state exceeds the limit, the server reports that it is retaining WAL history
and retries only after another configured interval. It does not discard data or
truncate history to fit. The WAL still stops writes at 512 MiB; deployments with
larger state require a file-backed snapshot API. This setting is not a substitute
for that work or a claim of production readiness.

Process tests exercise repeated compaction, catch-up after an offline interval,
and total restart. Chunk tests transfer state larger than one peer frame and
check subsequent writes, page state and capacity enforcement. Metadata tests
check placements and health roster recovery. These are functional fault tests,
not power-loss or multi-host throughput measurements.
