# Object manifest publication

The `object_manifest` module defines a binary protobuf catalog for object names
and immutable chunk extents. `proto/object_manifest.proto` documents its fields.
This is a library component; metaserver persistence, replicated publication and
HTTP routing do not yet use it. It does not make the HTTP server distributed.

Each manifest identifies a bucket, key and consecutive generation. Extents cover
the object contiguously, identify a configured chunk group and page, and state the
required page write sequence and checksum of the extent's data bytes. A reader
must verify both against data-node responses. The codec validates structure; it
cannot prove that a data quorum stored the bytes or that a group identity is trusted.
The checksum is an integrity check, not authentication.

A publication outcome contains the complete before and after values. Apply checks
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

Node integration must bind each publication to its placement decision, persist
and replicate the decided outcome before acknowledging, and capture manifests
with the rest of metadata at one applied position. Writers must store immutable
new data extents before publishing visibility, resolve uncertain publication by
reading the manifest, and never reclaim old extents while readers can still use
them. Recovery must replay catalog outcomes after the matching checkpoint before
serving reads. These are required integration steps, not implemented guarantees
of the current HTTP path.
