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
fault tests, not a large-scale throughput result. Automatic chunk-leader client
routing, metadata placement of Raft groups, the HTTP distributed backend,
authentication, snapshots, log compaction, and membership changes remain
unfinished. Current clients must select the leader explicitly and must not
blindly retry an uncertain mutation.

After a transport timeout or malformed response, discard that client connection
and reconnect. Do not reuse a stream whose response boundary is uncertain.
