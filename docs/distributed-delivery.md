# Distributed delivery plan

This plan tracks the remote metadata and chunk services. A feature available on
the in-process `MatrixObjectStore` API is not evidence that the remote API
supports it. All distributed acceptance runs use separate processes in local
WSL; these runs establish recovery behavior, not multi-host performance.

## Current foundation

| Area | Implemented behavior | Evidence |
|---|---|---|
| Star durability | Routed three-member groups require three durable write acknowledgements and one readable copy. Repair selects a majority source independently. | `docs/chunk-group-catalog.md`, `docs/star-repair-worker.md`; journal-backed process recovery tests |
| Metadata consensus | Current-term quorum reads, conditional manifest publication, binary protobuf outcomes, checkpoint recovery and membership changes | `docs/metadata-consensus.md`; `network_metadata_consensus` |
| Data consensus | Optional Raft groups, batched conditional writes and digest probes, verified routing, leader failover | `docs/chunk-consensus.md`; `conditional_digest_batches_replicate_ordered_page_outcomes` |
| Object access | Immutable extents, bounded authenticated range reads, suffix-only append, resumable unpublished writes and appends, metadata-only copy | `docs/object-manifests.md`, `docs/authenticated-object-reads.md`; multi-group process tests |
| Repair | Bounded resumable inventory, head-first checks, periodic payload scrubs, retry backoff and isolation of stopped groups | `docs/star-repair-worker.md`, `docs/star-repair-scale.md` |
| Catalog enumeration | Bounded cross-bucket traversal, tombstones and revision-checked continuation through metadata failover | `docs/manifest-listing.md`; `manifest_listing_continues_after_leader_loss_and_preserves_tombstones` |

The default routed Star threshold is `N=3, W=3, R=1`. A missing member can
therefore leave existing objects readable while preventing new publication.
Digest batches contain at most eight ranges; the object writer retains a
32-group connection cache. Repair defaults to 16 pages per inventory batch,
four workers in all-groups mode and a five-minute completed-sweep interval.
These limits control different resources and must not be increased together
without measuring memory, open sockets and foreground latency.

## Ordered implementation

### 1. Protect readers and staged publications

Introduce explicit read sessions and staged-write ownership. A reader must keep
its selected manifest generation alive until completion or a bounded session
expiry. A writer must retain allocated pages until publication is confirmed or
its fenced staging ownership expires. Retried operations must distinguish an
unconfirmed publication from a refused publication.

Choose and document failover semantics before adding deletion: either restore
protection durably or invalidate old sessions and wait out their protection
window before permitting reclamation. A namespace's exclusive mutation lease
does not provide shared reader retention. A listing revision does not provide
retention either. Persist decisions as versioned binary protobuf outcomes when
they must survive failover; avoid copying the complete metadata state per renew.

Acceptance: hold a reader across overwrite and delete; pause a staged append;
lose the metadata leader; restart every voter; expire or revoke each session.
Neither protected payload may be reclaimed. Stale owners must be unable to
publish after their staging protection has ended.

### 2. Reclaim unused pages safely

Build bounded chunk inventory collection with a cursor per server and retry
failed servers on a later sweep. Combine inventory with all-bucket live extent
ownership, retained generations and the protections above. Cross-bucket copies
can share pages. Partial scans, unavailable members and revision changes must
produce no deletion authority. Fence concurrent publication before authorizing
deletion; a successful catalog scan alone is insufficient.

Record bounded reclamation work, retry acknowledgements idempotently, and resume
after worker or metadata restart. Separate abandoned pages, superseded replicas
and logical object deletion. Report pending work and reclaimed physical bytes.

Acceptance: copy across buckets, delete the source and read the copy; interrupt
each collection and deletion phase; lose a replica and a metadata leader;
retry duplicate decisions. Verify both absence of data loss and eventual space
recovery after every protection ends.

### 3. Keep repair and placement current as the cluster grows

Refresh the all-groups repair catalog while running; discovery currently occurs
only at startup. Retain bounded worker concurrency, per-group cursors and
unconfirmed-commit stops. Add capacity-aware movement with distinct host/rack
constraints, maintenance/draining fences and bandwidth budgets. Validate the
remote path independently of in-process placement helpers.

Acceptance: add groups during repair, change membership during a sweep, drain a
node, exceed one destination's capacity and interrupt movement. Existing groups
must keep progressing; ownership and durability must remain explicit throughout
each move. WSL can simulate failure domains but cannot establish rack isolation.

### 4. Complete the remote object lifecycle

Audit remote operations separately for conditional delete, retained versions,
multipart upload, listing, ownership and access control. Define wire contracts
and conflict behavior before exposing each operation. Avoid presenting local
compaction, stale-space reuse, retention or multipart support as remote support.
Audit chunk sealing and truncation independently: every replica must reject a
stale writer after a seal, and repair must preserve the selected final length.

Acceptance: exercise conflicts, duplicate requests, partial uploads, restart and
leader loss through the public remote client. Verify authorization and limits
before mutation, including malformed and truncated protobuf messages.

### 5. Measure and reduce storage costs

Measure small writes, large writes, suffix appends, range reads, overwrites and
repair with the same durability settings. Record latency percentiles, throughput,
CPU time, allocation count/bytes, physical bytes written, journal bytes, network
bytes and final occupied bytes. Separate write, read and space amplification.
Include checkpoint and repair interference, an unavailable member and concurrent
clients. Keep raw results outside the source tree unless deliberately curated.

Retain bounded batching and connection reuse. Optimize rollback copying,
checkpoint stalls and scan allocations only after the relevant counters identify
them as costs. Local WSL results do not establish multi-host latency or throughput.
Compression, erasure-coded placement, tiering and raw-device storage require
separate durability and recovery designs before being advertised remotely.

## Cross-service validation

For TemporalStore, record one append's logical payload, encoded outcome,
object-store suffix bytes, network transfer and durable journal growth. Repeat
at increasing existing object sizes: retained-prefix copying or rewriting must
be reported separately from the appended bytes. Keep local and distributed
outcomes binary protobuf and test recovery from truncated suffixes.

For MatrixRaft, exercise metadata and optional data-group failover with the
exact dependency revision used by MatrixObject. For MatrixCache and matrixark,
exercise integration only where an existing supported adapter exists; do not
claim distributed storage behavior from an in-memory mock. Track rename and
license audits separately from runtime correctness and performance evidence.

## Release gates

Run compatibility and bounded-message tests, the complete object-connection test
target, digest failover tests, revision-checked enumeration and journal-backed
Star recovery. Run larger group-count and TemporalStore integration workloads
after lifecycle changes, recording the exact commits and configuration of both
stores. Keep reclamation disabled until protection and interrupted-deletion
acceptance tests pass. The five public repositories remain Apache-2.0 and open
to worldwide contributions throughout delivery.
