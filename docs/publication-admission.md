# Publication admission and atomic copy

`RemoteMetaClient::read_publication_fence()` returns zero while ordinary
publication is permitted. `enable_publication_fence()` commits a one-way switch
and returns its metadata position. Repeating enable returns that same position.
The switch survives checkpoints, leader loss and restart. It cannot be disabled
within the retained metadata history. Both queries and mutations require quorum.

Migrate writers to owned allocation before enabling this mode. Once enabled,
ordinary page allocation is refused. An older writer with cached page IDs may
still send data, but its fresh extents cannot become visible through ordinary
manifest publication. This includes a candidate whose owned reservation was
aborted. A current owned reservation may publish atomically and consume its
ownership, including a reservation created before the switch.

Tombstones, empty objects and updates retaining the exact prior extent list
remain permitted. Reads and explicit reader retention continue working. Ordinary
copy into a different destination must migrate to the atomic copy API below;
an arbitrary extent list is not evidence that another object still retains it.
Enabling the fence does not migrate clients or acquire reader handles for them.

## Atomic metadata copy

`copy_object_atomic(source_bucket, source_key, bucket, key, expected, max_bytes, update_time_ms)`
copies the live source observed at the publication position. It checks the
destination expectation and source byte limit, then publishes the destination
at the same decided metadata transition. It allocates no pages and performs no
chunk I/O. Later source changes do not alter the copied extents. This API works
with and without the publication fence.

The outcome records source names and generation, not another copy of the source
extent payload. Apply checks that the source is still live at that generation
and its format, length and complete extent list equal the destination data.
These checks precede mutation. Copy remains subject to logical capacity and
draining restrictions, but does not require data-group write availability.
It can therefore publish metadata while replicas are offline; reading the copy
still requires available, valid data. The operation spans one metadata history
and does not implement a transaction across independent metadata groups.

`AtomicCopyFailure::Refused` means this publication was not admitted.
`Unconfirmed` means it may have committed. The client does not replay an
unconfirmed mutation. An identical destination alone does not prove that this
copy's destination expectation succeeded, since another writer can copy the
same data. Preserve that uncertainty when the result cannot be attributed to
the request. The source must be current and live; this API does not restore a
deleted source from a reader handle.

## Persistence and migration

Metadata opcode 93 actions 6 and 7 enable and read the fence. Outcome version 5
records its transition; catalog version 4 stores the committed position. Metadata
opcode 94 uses `proto/manifest_copy.proto`; outcome version 6 binds the source
identity to the copied data. The copied manifest uses the existing reply format.
Copy requests are bounded at 1 MiB plus 16 KiB. Existing catalog and outcome
encodings are retained when these fields are absent. Fence headers count toward
the shared 64-MiB catalog body limit.

Upgrade every metadata voter and every storage reader before enabling the fence
or issuing atomic copies. Older binaries cannot replay the new outcomes or
catalog images. A recovered fence takes precedence over a fresh disabled
bootstrap. Use a pinned metadata catalog when continuing across endpoints.
An uncertain enable can be inspected with the quorum-confirmed read operation;
the enable operation is idempotent.

## Reclamation is still disabled

The fence closes manifest publication through retired or unowned namespaces.
It does not stop raw chunk writes, cancel writes already in flight or protect
ordinary readers automatically. Data-node fencing, reader-session integration,
expiry, allocation request identity and interruption-safe deletion remain
unfinished. A positive fence or a successful inventory scan alone does not
authorize physical reclamation.
