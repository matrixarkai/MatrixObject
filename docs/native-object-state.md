# Native object metadata readers

Initialized LOG objects use chunk manifest format 2. Each object carries a stable
16-byte incarnation. Every extent carries its storage generation, qualified writer
epoch and three configured member identities with distinct durable initialization
values. Older manifest readers reject the version. Legacy publication and writer
commands refuse these objects.

`NativeObjectState` reserves an object mutation slot and owns newly allocated
chunks before data writes. Each extension appends one committed allocation to a
persistent vector and checks its range, epoch and storage identities. Publication
checks the captured object version and every owned extent before atomically
updating the chunk catalog and consuming the stage. Rollover preserves earlier
extents and seals only the old tail under the admitted epoch. The command layer
must verify storage durability, writer authority and object conditions; these
state transitions perform no storage IO.

Aborted and expired stages release the mutation slot but retain durable cleanup
ownership. Cleanup records may be forgotten only after durable retirement on every
storage member, including protection against delayed requests. A persistent
position watermark prevents reuse after cleanup and restart.

Read pins retain one shared manifest per captured generation, including across
an overwrite. Renewal and removal compare the expected deadline. Snapshots encode
the manifest once and carry compact per-reader records. A fresh object read still
needs an authoritative current view; a cached pin alone does not provide coherence.

Metadata snapshot version 25 carries the canonical binary protobuf native-state
image. Snapshot capture shares immutable state, install restores it, checksums bind
its contents and cache stamps track native revisions. Validation rejects future
allocation positions, shared native storage across objects, overlapping staged
storage and conflicting ownership across manifest catalogs. Earlier snapshots
keep their existing representation when the feature is absent.

Placement outcome version 18 carries one canonical binary protobuf native command.
It binds the leader's captured clock and consecutive metadata positions. Begin,
extend, publish, abort, expiry, read-pin and writer-lease transitions validate private
persistent-table clones before replacing live tables together. The leader checks
admission limits; followers validate the committed transition without local limits.
Legacy mutation commands refuse keys owned by staged or published native objects.

Journal recovery checks checksums, canonical encoding and contiguous positions,
skips the prefix included in its starting snapshot and applies later commands.
Normal command application continues to reject stale positions.

Feature bit 7 is not advertised or raised by startup. RPC admission, actual read-pin
acquisition, storage retirement, conditional object operations and default-backend
routing still need their coupled implementation. The default object backend remains
unchanged.
