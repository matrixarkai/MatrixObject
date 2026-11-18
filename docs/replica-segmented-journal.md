<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->
# Immutable replica outcome journal

The standalone chunk server has an opt-in journal that separates immutable data
from checkpoint metadata. A checkpoint worker reconstructs a private catalog from
committed outcome metadata and streams a new catalog outside the replica mutex.
It does not capture the live page map or retain payload or receipt-map handles.
Foreground writes therefore do not copy a page because a checkpoint owns its data.

## Configuration

Set these variables for a disposable test deployment:

```sh
export MATRIXOBJECT_REPLICA_JOURNAL=/srv/matrixobject/test-node/journal
export MATRIXOBJECT_REPLICA_SEGMENTED_OUTCOMES=true
export MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES=8388608
export MATRIXOBJECT_REPLICA_GROUP_COMMIT=true
export MATRIXOBJECT_REPLICA_LOCAL_RECOVERY=peers
chunkserver
```

Create the parent directory first and give each replica its own path. The API
entry point is `ReplicaNodeServer::bind_with_segmented_journal`. The default
continues to use the existing journal. An existing journal cannot be converted
by changing this setting: each reader refuses the other layout. There is no
in-place migration. Zero-filled journal frames and chunk Raft configuration
cannot be combined with this setting.

Segments default to 64 MiB. A foreground group contains at most 64 decisions and
8 MiB of encoded outcomes. No batching delay is added. Its payload, decisions,
and scalar replica metadata share one data synchronization before acknowledgment.
A larger group is split, retaining the exact scalar context of each sub-group.
Checkpoint fragments do not count as queued foreground mutations. Recovery retains
the uncheckpointed mutation bytes when calculating the next checkpoint threshold.

## Publication and recovery

Storage is binary protobuf. State-only batches keep version 1, checkpoint storage
fragments use version 2, and state transitions with replica metadata use version
3. Ordinary replay refuses checkpoint storage records and refuses to discard a
version 3 batch's replica context. Replica recovery uses the context-aware path.

A checkpoint catalog contains typed page metadata, a dictionary of shared
immutable payload descriptors, and ordered page spans. Zero-filled holes do not
require payload extents. Catalog bytes stream into fragments of at most 1 MiB.
The checkpoint becomes visible only when a small commit record is synchronized.
Its identity, epoch, record boundaries, length, and SHA-256 bind the complete
catalog. Interrupted preparation leaves the previous committed checkpoint valid.

Background checkpoints retain their captured replay position. Recovery folds
subsequent mutations as metadata and materializes only the resulting pages.
It verifies the newest published checkpoint; corruption is refused rather than
silently falling back to an older checkpoint. File identity, exclusive ownership,
segment bounds, payload sector digests, and resulting page checksums are checked.

A bulk install persists its payload fragments and catalog before publishing its
commit. Publication is serialized with background checkpoint epochs. A superseded
worker cannot publish over the install, and recovery refuses an install checkpoint
whose replay position skips an intervening logical mutation. Retired catalogs are
destroyed outside the replica mutex. Background errors fence requests and are
reported as an offline replica by the heartbeat path.

Deployment recovery policy is applied after journal restore. A peer-recovery node
keeps no local recovery sidecar; a node configured for local recovery rebuilds it.
Checkpoints do not themselves establish membership or repair missing peer writes.

## Current limits

This is a test layout, not the default production layout. Old segments are retained;
there is no reclamation or retention bound. Checkpoints still rewrite the complete
metadata catalog, so metadata amplification and checkpoint CPU grow with the live
catalog. Bulk installs currently synchronize individual payload fragments. Live
payloads remain resident in the existing service, and startup materializes them.

Incremental metadata, safe segment reclamation, a bounded payload cache, and
measurements under sustained corpus growth are required before default activation.
Local multi-process tests establish functional recovery, not multi-host performance,
flat append latency, or a QPS improvement.

## Validation

Run these commands in Linux or WSL with a single Cargo build job:

```sh
cargo test --locked --all-features --lib -j1 replica_checkpoint_ -- --test-threads=1
cargo test --locked --all-features --lib -j1 segmented_ -- --test-threads=1
cargo test --locked --all-features --test durable_payload_allocations -j1 -- --test-threads=1
cargo test --locked --all-features --test network_metadata_consensus -j1 segmented_star_replicas_keep_objects -- --test-threads=1
```

The process test starts three metadata nodes and three chunk servers, confirms
six distinct live PIDs with their own storage paths and established sockets, writes
objects through Star replication, changes metadata leadership, appends data, kills
and restarts all six processes, and reads every object from every data replica.
