# Durable manifest retention

`RemoteMetaClient::retain_manifest(bucket, key, generation)` records one exact
current, live generation and returns a `RetainedManifest`. Retaining a stale
generation or tombstone is refused. The handle stores immutable extent metadata;
it does not copy payloads or contact chunk servers. Retention and release use
binary protobuf outcomes in both the standalone journal and metadata Raft path.

Keep the handle until every read using it has finished. The current object may
then be overwritten or deleted without changing the retained generation.
`read_retained_manifest(id)` retrieves that generation;
`read_retained_object(id, max_bytes, options)` reads and verifies published checksums
using current verified group routes, the caller's byte limit and one shared
operation deadline. Availability still depends on the data replicas.

Call `release_manifest(id)` when finished. Repeated release is safe, including
after reconnecting. IDs are committed metadata positions and are never reused
within the retained metadata history. A released handle cannot name a later
retention. Handle reads and enumeration require current-term quorum confirmation;
retention mutations wait for durable commit. Unknown request codes are refused
by older servers; clients do not substitute an unprotected read.

There is no automatic expiry. Abandoned handles continue retaining data until an
operator explicitly releases them. A lost creation reply can leave such a
handle, so `list_retained_manifests(after, limit)` enumerates up to 32 IDs per
page with an exclusive numeric cursor. Read each ID to inspect its manifest.
Listing pages are independent reads and do not constitute a reclamation snapshot.
Retain is not replayed automatically after a transport-uncertain result. Creation
and release have the same access assumptions as the metadata API; a handle is
not a separate authorization credential.

At most 128 handles can be retained. Their encoded records share the catalog's
64-MiB checkpoint body bound with current manifests and staged allocations. A full catalog refuses new
retention before changing state. Ordinary `read_object` calls do not implicitly
create handles; callers needing explicit retention must use this API and release
only after all users finish. Keeping a handle is not a backup and does not repair
lost or corrupt replicas.

## Storage and upgrades

Metadata opcode 92 uses `proto/manifest_retention.proto`. Requests are bounded at
8 KiB; replies at 1 MiB plus 128 bytes. `RetainedManifest` and catalog field 3 are
defined in `proto/object_manifest.proto`. Catalog version 2 carries sorted,
unique retention IDs. Catalogs without handles or staged allocations retain
their version-1 encoding; staged allocations use catalog version 3.
Placement outcome version 3 contains only one handle creation or removal. Its
index and retained prior state are checked before mutation. Speculative Raft
batches restore every affected handle before proposal admission, including
failure and unwinding paths; they do not clone the entire retention catalog.

Upgrade every metadata voter and every process that can recover its storage
before retaining the first manifest. Older binaries cannot replay version-3
outcomes or decode catalogs containing handles. Checkpoints retain handles across
leader loss and restart; they are not invalidated when a client disconnects.

## Reclamation remains separate

Any future collector must preserve extents named by current manifests and by
every retained manifest, including cross-bucket copies. It must also fence
staged writers and concurrent retention/publication. The current listing revision
tracks publication only; it does not track handle changes and cannot authorize
deletion. This API does not implement staged-write ownership, expiry, physical
reclamation or automatic protection for ordinary reads. Reclamation remains
disabled until those protocols and interrupted-deletion tests are implemented.
