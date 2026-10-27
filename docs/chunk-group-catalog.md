# Chunk groups in metadata

A metadata server can load a fixed chunk-group catalog with
`MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE`. Generate the binary file with
`ChunkGroupCatalog::new(groups)?.to_bytes()?`. The envelope is `MOGC` followed by
canonical protobuf; `proto/chunk_group_catalog.proto` defines the messages.
The maximum catalog is 512 groups and 2 MiB of protobuf.

Each group supplies a logical placement ID, failure domain, immutable Raft
identity, ordered API sockets, and ordered heartbeat member IDs. Both ordered
lists correspond to the sorted voter IDs in the identity. API sockets and peer
sockets must be distinct across the entire catalog. Configure each chunk voter
with its matching heartbeat ID and API socket. This bootstrap mode cannot be
combined with the individual-node roster or legacy seed settings.

Only the logical group is eligible for placement. Its capacity is one voter's
configured capacity, its usage is the maximum reported voter usage, and it is
online when a majority of voter reports are online and not draining. The group
heartbeat timestamp is the quorum-th newest live voter report. Expiry and
heartbeat changes update the logical target in the same metadata outcome.
Fresh reports do not prove Raft connectivity or that a voter has caught up;
chunk reads and writes still require the chunk group's consensus checks.

`RemoteMetaClient::read_chunk_group` resolves the persisted catalog entry.
`RemoteMetaClient::connect_chunk_group` uses that entry to verify chunk voter
identity before sending data. Resolving a catalog alone does not authenticate
an operator or prove that a leader is currently available.

Local metadata snapshots and metadata Raft bootstrap files preserve the catalog
in a checksummed binary protobuf envelope. Existing snapshots without a catalog
retain their original encoding. Recovered membership takes precedence over new
bootstrap settings. Membership is static; this interface does not perform
online voter replacement.

Catalog-enabled state requires a metadata binary that understands MOGB and the
ReadChunkGroup verb. Upgrade all metadata voters and clients before bootstrapping
a catalog-enabled deployment. Older binaries cannot reopen the new envelope;
there is no automatic downgrade or conversion of an existing individual-node
roster into groups. Keep a verified backup before changing deployment formats.

## Coupled quorum validation

The six-process test starts three metadata voters and three chunk voters with
independent durable directories. It writes 33 payloads and commits their logical
group placements, kills both elected leaders, restarts all six processes, and
checks placement recovery, payload bytes, write sequences, and one-copy capacity
accounting. It then removes each service's quorum and checks that chunk data and
metadata reads cannot succeed from a minority. Missing chunk voters expire from
metadata placement eligibility through their real health reports.

Run it with:

```sh
cargo test --locked --features metadata-consensus,chunk-consensus --test network_chunk_consensus coupled_groups::metadata_and_chunk_quorums_recover_together_and_refuse_minority_operations -- --exact
```

This is a bounded functional and recovery test. It does not establish throughput,
large-cluster capacity, authentication, or readiness for production traffic.
