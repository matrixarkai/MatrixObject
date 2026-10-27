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
a committed barrier, so an isolated minority cannot answer from stale state.
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
currently clones replica state, and each read adds a barrier to the log; these
costs still need optimization and large-scale measurement.

The process tests cover three independent voters, acknowledged writes and
discards, leader loss, refusal below quorum, restart of all processes, capacity
refusal, storage guards, shutdown, and metadata reports. They are functional
fault tests, not a large-scale throughput result. Metadata placement of Raft groups,
group identity verification in clients, the HTTP distributed backend,
authentication, snapshots, log compaction, and membership changes remain unfinished.

## Bounded leader routing

Use `RemoteReplicaClient::connect_catalog(&endpoints, &options)` to select a leader
from one through seven distinct API IP sockets. All endpoints must belong to the
same Raft group; this is trusted configuration, with no identity handshake yet.
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
