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
planning, commit a barrier before reading the state. Mutations prepare a binary
protobuf outcome against committed state, replicate it, and acknowledge only after
durable commitment and application. Followers and leaders without quorum refuse
calls. A timeout after proposal is ambiguous: the write may subsequently commit.
Do not automatically retry mutations. The current directory client connects to its
first configured endpoint; automatic leader discovery and safe retry routing remain
unfinished. Connect to the leader when testing this mode.

`metadata.peer.base` stores the initial state and `metadata.peer.wal` stores binary
protobuf consensus records containing the same placement outcome bytes used by the
local journal. An exclusive lock protects the directory. Recovery replays committed
entries into fresh state. Missing or zeroed history is refused. A bootstrap digest
separates voters configured with different initial state. Existing stored bootstrap
state takes precedence over seed environment settings on restart. Do not delete,
copy between nodes, or independently replace these files in a live group.

The worker serializes proposals and keeps one accepted proposal pending until it
commits or leadership changes, even if its caller times out. Requests and packet
queues are bounded; network delivery has a total deadline. Packet loss is handled
by Raft retransmission. Storage or application failure stops the server. Shutdown
joins the consensus and network workers.

This is an integration stage, not a production-readiness claim. Peer traffic is
unauthenticated and unencrypted; use an isolated test network. Membership changes,
snapshot transfer, WAL compaction, leader discovery, authenticated transport, and
large-scale validation remain required. Each read currently appends a barrier and
each mutation clones metadata to prepare its outcome; these costs must be addressed
before scale claims. The underlying WAL stops writes at its 512 MiB bound.

The process tests exercise three independent servers, leader termination, quorum
loss, complete restart, binary outcome replay, storage identity, election no-op
checkpoints, and shutdown with a slow connection. They do not establish power-loss
durability or large-scale performance.
