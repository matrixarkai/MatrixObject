# Native object metadata readers

Initialized LOG objects use chunk manifest format 3. Each object carries a stable
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

Metadata snapshot version 26 carries the canonical binary protobuf native-state
image. Snapshot capture shares immutable state, install restores it, checksums bind
its contents and cache stamps track native revisions. Validation rejects future
allocation positions, shared native storage across objects, overlapping staged
storage and conflicting ownership across manifest catalogs. Earlier snapshots
keep their existing representation when the feature is absent.

Placement outcome version 19 carries one canonical binary protobuf native command.
Version 19 carries native change version 2 and requires physical identities for new stages and chunks. Historical outcome18/change1 and format2 snapshots remain readable, but cannot mutate a physical namespace. It binds the leader's captured clock and consecutive metadata positions. Begin,
extend, publish, abort, expiry, read-pin and writer-lease transitions validate private
persistent-table clones before replacing live tables together. The leader checks
admission limits; followers validate the committed transition without local limits.
Legacy mutation commands refuse keys owned by staged or published native objects.

Journal recovery checks checksums, canonical encoding and contiguous positions,
skips the prefix included in its starting snapshot and applies later commands.
Normal command application continues to reject stale positions.

Feature bit 7 is not advertised or raised by startup. The guarded command and
client path now connects durable stage allocation, initialized data writes,
publication and authoritative read-pin acquisition. Ordinary deployments cannot
activate this experimental path. The default object backend remains paged.

`NativeObjectClient` uses metadata-approved three-member Star routes and physical
storage incarnations. Stage creation assigns the leader's clock and allocation
positions. Requests carry a separate TTL; clients cannot choose committed epochs
or absolute deadlines. The metadata journal and replicated command path use the
same binary protobuf outcomes. Raft batch preparation restores all native draft
tables before proposals commit.

`NativeTailWriter` starts all three member sends before waiting for acknowledgments,
using the existing readiness poller and requiring all three durable replies.
Each personalized protobuf frame shares one immutable record payload. Partial
and interrupted vectored writes resume across the header, payload and suffix;
TLS consumes bounded plaintext slices without assembling a second payload.
Wire bytes, physical member identities and the complete-prefix rules are unchanged. It retains the original attempt and at most one record fragment
on an unsettled request. Intermediate fragments keep the earlier complete-prefix
marker; only the final fragment publishes the complete append. A correlated
successful APPEND reply acknowledges its original tagged request. Probe lengths,
uncertain exchanges and equal bytes from another attempt cannot acknowledge it.
Lease deadlines are derived conservatively from the leader clock and local
monotonic time. Fencing stops further writes from the old writer.

A six-process test uses three metadata voters and three separate data processes.
It writes sealed chunks and an active tail, verifies 64 appends with no metadata
commits, retries the original attempt after losing a member, rejects a foreign
equal-byte attempt and a released read pin, then verifies every byte after
restarting all six processes. This fixture explicitly seeds the guarded feature;
it does not prove default activation or TemporalStore end-to-end latency.

Writer handoff/reopen and automatic chunk switching, active-object replacement,
data-derived version and modification-time attributes, storage retirement,
namespace migration, the complete object API and TemporalStore routing remain
unfinished. The network command rejects an active-tail replacement until its
storage handoff is implemented.

Physical member bindings use manifest format 3 and contain each replica's durable
storage incarnation in configured member order. Format 2 forbids those fields;
format 3 requires distinct nonzero physical identities. Native state images use
inner version 2 when physical bindings are retained or staged. Snapshot floor 26
prevents older readers from dropping those identities; downgraded envelopes are
refused even when their outer checksums are valid.

A read pin resolves an immutable physical binding for one extent, with work bounded
by its three members. The protocol5 pinned reader checks physical identities on
probes, record identity inspection, repair writes and payload reads. It never
initializes, fences or truncates a member. Old pins do not accept a higher observed
epoch as newly authorized. The native client acquires and revalidates actual
metadata pins. Sealed reads also check the metadata footer. Effective epoch updates
through writer handoff and the full object API remain unfinished.

A staged multi-chunk write seals each completed nonfinal chunk before returning to
its caller, releasing its open-tail payload memory. The final chunk stays open
until publication chooses whether it is appendable. Publication reuses the same
all-member complete-length and sealed-footer checks. A retry of an already sealed
stage chunk must match its complete footer and every original fragment tag;
equal bytes from a different attempt do not acknowledge ownership.
