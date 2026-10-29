# Object manifest publication

The `object_manifest` module defines a binary protobuf catalog for object names
and immutable chunk extents. `proto/object_manifest.proto` documents its fields.
The catalog is carried in metadata service snapshots, local metadata snapshot
files and streamed Raft checkpoints. Atomic binary publication outcomes can be
decided and replayed through the placement journal and Raft application path.
The metadata request API exposes reads and conditional publication. HTTP routing
does not yet use this API.

Each manifest identifies a bucket, key and consecutive generation. Extents cover
the object contiguously, identify a configured chunk group and page, and state the
required page write sequence and checksum of the extent's data bytes. A reader
must verify both against data-node responses. The codec validates structure; it
cannot prove that a data quorum stored the bytes or that a group identity is trusted.
The checksum is an integrity check, not authentication.

A standalone manifest outcome contains the complete before and after values. Apply checks
the expected value before changing state. Replaying the identical resulting value
is a no-op; a stale value fails without mutation. Deletion publishes an empty
tombstone at the next generation. Recreating an object must name that tombstone;
absence cannot be substituted for a deletion. Tombstones are not automatically
removed, since removal would allow an old absent-object writer to recreate state.

Bounds are 4,096 extents and 1 MiB per manifest, 100,000 catalog entries, and 64 MiB
per catalog checkpoint body. Publication enforces the checkpoint size bound before
mutation. The versioned envelopes use canonical protobuf and an eight-byte FNV-1a
integrity trailer. Unknown fields, damaged bytes, duplicate catalog identities,
unordered catalog images, overlapping extents, invalid page extents and skipped
generations are rejected. These limits are explicit initial format limits rather
than a large-scale capacity claim.

Publication validates configured group names and commits the decided outcome
before acknowledging. The caller must verify group identities and data durability. Manifests
and placements are captured together at one applied position. Writers must store
immutable
new data extents before publishing visibility, resolve uncertain publication by
reading the manifest, and never reclaim old extents while readers can still use
them. Recovery must replay catalog outcomes after the matching checkpoint before
serving reads. These are required integration steps, not implemented guarantees
of the current HTTP path.

Metadata snapshots with no manifests keep the version-18 encoding and checksum.
A nonempty catalog uses version 19 and stores the canonical binary catalog after
the placement roster. Both the service checksum and snapshot envelope cover it.
The snapshot export cache stamp also includes the catalog. Installation restores
the catalog together with metadata, including deletion generations.

Upgrade readers before writing version-19 snapshots: older readers reject them.
Local metadata snapshot files retain their 64 MiB total limit, including the
catalog and all other metadata; a catalog near its own limit may not fit that
local envelope. Streamed Raft checkpoints retain their separate total limit.
The catalog is serialized as one bounded blob, so streaming does not eliminate
its temporary encoding allocation. Snapshot support alone does not validate
data-node durability.

Placement outcome version 2 adds a typed manifest outcome. Its one placement
change must match the manifest object identity, generation, length and sorted
set of extent groups. Creation expects both records absent; deletion retains a
zero-length placement and a manifest tombstone. Recreating the object names that
tombstone. Capacity reservations move atomically with both records. As with the
existing placement model, each target reserves the full object length.

Decision checks writable target groups and capacity without changing state.
Replay does not repeat health-policy admission. Apply validates all expected
records, capacity changes and catalog limits before
mutating. A stale outcome or malformed binding leaves the complete state unchanged.
Placement indexes fence duplicate log application; the enclosing Raft runtime
owns log replay position. Version-1 outcomes remain readable but cannot overwrite
a manifest-backed placement. The ordinary placement commit/release methods also
refuse such objects, requiring atomic publication instead.

The metadata client exposes this outcome layer over the network. A distributed
HTTP data path is not enabled yet. Callers must verify group identities and
immutable data extent durability before proposing a publication.
Older readers reject version-2 outcomes; upgrade readers before writing them.


`RemoteMetaClient::read_manifest` returns live values and deletion tombstones;
an absent object returns NotFound. `publish_manifest` accepts the complete
expected and resulting values through `ManifestOutcome`, and returns the committed
manifest. Requests and replies use bounded canonical binary protobuf
(`proto/manifest_requests.proto`), rejecting unknown fields and malformed values.
The reply is checked against the requested object or resulting manifest.

Raft reads use the quorum read barrier. Publication is acknowledged only after
committed apply. Speculative batches save and restore only affected manifest
entries along with placement and node records, including catalog size accounting.
Local durable nodes preflight the complete snapshot bound, sync the outcome
journal, and then apply it. That local preflight clones and serializes state;
it is not a constant-cost operation and is not used by the Raft path.

Catalog routing may retry an explicitly unadmitted NotLeader response. It never
automatically replays an uncertain publication; read the manifest to resolve
the result. Unknown chunk-group names are rejected before proposal. The metadata
node does not read extent payloads or prove their durability or immutability.
Clients must finish immutable data writes first, verify group identity and read
sequence/checksum, and preserve old extents until readers no longer need them.
