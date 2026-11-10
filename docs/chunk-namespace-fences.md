# Durable chunk namespace fences

Chunk namespaces can be closed independently of their stored pages. A closure
pins a sorted set of immutable page identities: page index, complete length,
write sequence and SHA-256. Ordinary writes and discards then fail, including
writes to page indices that do not yet exist. Other namespaces stay writable.

The replica client exposes `fence_namespace` and `read_namespace_fence`.
The first closure has generation 1. Every later transition requires the exact
next generation and removes at least one pin without changing surviving pins.
An empty closure is a permanent tombstone. A namespace cannot be reopened or
reuse a retired page index.

An installation or inventory repair can restore a pinned page only when its
complete length, sequence and SHA-256 match. Install begin checks the shape;
commit checks the current fence and full payload again. A transfer begun before
closure cannot bypass a later retirement. EC payload reconstruction also keeps
the pinned sequence and validates the reconstructed bytes before installation.
Open namespaces retain their existing EC repair sequence behavior.

Closure does not certify that every existing replica already has the pinned
bytes. It may precede repair of a missing or damaged copy. Reads retain their
existing behavior; callers must still authenticate object extents.

## Durability and wire bounds

Journal-backed Star nodes persist a binary protobuf outcome before acknowledging.
Volatile nodes refuse closure. Optional Raft groups commit the same outcome
through consensus. Queries on Raft groups use the existing quorum read barrier.
An uncertain closure reply is not automatically replayed; query the namespace
and compare the complete record before deciding whether to retry.

Each fence permits at most 4,096 page pins and 512 KiB of wire bytes. The catalog
permits 65,536 namespaces and 64 MiB, including terminal tombstones. Retirements
cannot grow its pins. Nested protobuf counts and lengths are bounded before
allocation, and noncanonical payloads are refused.

Nonempty catalogs use snapshot version 23; empty catalogs retain version 22
bytes. Checkpoints preserve tombstones and pin identities, including streamed
Raft state transfer to a lagging voter. Upgrade all chunk nodes and snapshot
consumers before enabling closure: older binaries cannot consume the new verbs,
outcomes or nonempty fence snapshots. Downgrading those nodes is unsupported.

## Scope and remaining work

These are explicit data-node controls. Ordinary object publication does not yet
close its namespace automatically. There is no metadata controller distributing
closure or retirement decisions, no stale-node readmission protocol, and no
physical deletion authority. Tombstones are bounded and never compacted away.
Keep reclamation disabled until reader protection, ownership reconciliation,
fence propagation and interrupted-deletion recovery are connected and tested.

Local WSL checks cover stale mutations, exact repair, retirement, staged-transfer
races, journal and checkpoint restarts, streamed checkpoints, lagging voters,
leader loss and quorum loss. They do not establish multi-host performance.
