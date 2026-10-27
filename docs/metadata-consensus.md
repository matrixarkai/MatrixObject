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

The worker serializes proposals and keeps one accepted proposal pending until it
commits or leadership changes, even if its caller times out. Requests and packet
queues are bounded; network delivery has a total deadline. Packet loss is handled
by Raft retransmission. Storage or application failure stops the server. Shutdown
joins the consensus and network workers.

This is an integration stage, not a production-readiness claim. Peer traffic is
unauthenticated and unencrypted; use an isolated test network. Membership changes,
snapshot transfer, WAL compaction, authenticated transport, and
large-scale validation remain required. Each read currently appends a barrier and
each mutation clones metadata to prepare its outcome; these costs must be addressed
before scale claims. The underlying WAL stops writes at its 512 MiB bound.

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
not a large-scale throughput result. Full HTTP integration, network chunk Raft,
authentication, membership changes, and log compaction remain separate work.
