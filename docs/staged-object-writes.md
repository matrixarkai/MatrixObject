# Durable staged object writes

Time-bounded reservations, renewal and explicit expiry are described in
`staged-ownership-leases.md`. Existing reservations keep their permanent lifetime.

`allocate_staged_object(bucket, key, expected_generation, page_count, append)`
reserves a fresh page namespace and records its target generation before data
is written. Use `None` for a new object, or the exact current generation for an
overwrite. An append requires a live version-2 manifest. Tombstones count as
existing generations. Reservations do not make an object visible or reserve
physical disk capacity; placement and data writes can still be refused.

Pass the returned `StagedAllocation` to
`write_staged_object(allocation, expected_manifest, data, options)`. The helper
checks the active record, verifies group routes and writes fresh pages at the
configured durable threshold. It then atomically publishes the manifest and
consumes ownership. Appends retain the expected prefix and write only suffix
pages. The page count must cover the payload at `options.page_bytes`; reserve
one page for an empty payload. Owned writes use their dedicated namespace rather
than the client's amortized allocation lease. Star and Raft routes share this API.

Ownership records contain names, generation, mode and page count. They contain
neither payloads nor complete old extent lists. Extent checksums stay in the
normal publication outcome. Publication checks every new page against the owned
namespace, index range and initial sequence. Appends must preserve the exact
prior extent prefix. An active owned namespace refuses ordinary manifest
publication, so it cannot accidentally bypass atomic ownership consumption.

## Failures, resume and abort

Keep the allocation record and any candidate returned in `ObjectWriteFailure`.
`resume_staged_object(allocation, expected, candidate, data, options)` verifies
the same candidate, probes staged digests, writes only absent pages and uses
the staged publication path. An exact already-visible candidate resolves a
previously successful publication without allocating or publishing again.
Star resume still requires the durable write threshold, independent of its
one-copy committed read threshold. Metadata and data work share the operation
deadline; uncertain writes are not automatically replayed.

`abort_staged_allocation(id)` removes active ownership. A later staged publication
using that ID is refused, including after metadata failover or restart. IDs are
committed positions and cannot be reused within the same retained history.
Abort is idempotent and does not undo an already published object. Missing
ownership alone does not establish whether a prior publication committed.
Use the exact candidate and current manifest to resolve visibility, and retain
the original uncertainty when another generation has replaced it.

`read_staged_allocation(id)` inspects a reservation;
`list_staged_allocations(after, limit)` enumerates up to 32 IDs per page. These
are quorum-confirmed reads. An uncertain allocation response may leave an active
record before any data I/O. Enumeration supports operator inspection, not automatic
ownership recovery: matching target fields do not identify a request. Do not adopt
another writer's reservation by matching its target. Permanent reservations
require explicit abort; leased reservations receive bounded automatic cleanup
after their deadlines. Handles belong to one
metadata history; use a pinned metadata catalog when reconnecting to endpoints.
They are not separate authorization credentials.

## Bounds, storage and remaining fencing

At most 128 reservations are active. Each reserves 1–4,096 page IDs. Their
encoded records share the 64-MiB manifest checkpoint body limit with current
manifests and reader handles. Capacity and reply bounds are checked before
mutation. Standalone journals and metadata Raft use version-4 binary protobuf
placement outcomes. Catalog version 3 carries ordered reservations. Publication
releases its reservation's encoded space in the same transition. Speculative
Raft batches restore only affected records and objects before admission.

Metadata opcode 93 uses `proto/staged_allocation.proto`. Publication requests
are bounded at 2 MiB plus 8 KiB; replies at 1 MiB plus 8 KiB. Upgrade every
metadata voter and storage reader before enabling owned allocations. Older
binaries cannot replay the new outcome or catalog versions. Catalogs without
staged records retain their prior encoding.

Ordinary object writes keep their existing allocation path while the publication
fence is disabled. `publication-admission.md` describes the durable switch that
requires owned publication and rejects legacy publication after ownership removal.
Ownership alone is not a permanent blacklist of retired namespaces. Raw chunk
writes still require data-node fencing before reclamation can be enabled.
Abort does not cancel a page write already in flight or physically delete data.
Expiry, automatic reader protection and interrupted physical reclamation also
remain unfinished. Neither a missing reservation nor a successful inventory
scan alone authorizes deletion. Reclamation remains disabled.
