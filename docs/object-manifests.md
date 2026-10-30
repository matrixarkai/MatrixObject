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

`RemoteMetaClient::read_object` fetches one committed manifest and assembles its
extents using verified data-group connections. It returns the manifest with the
bytes only after every extent matches its exact length, write sequence and FNV-1a
data checksum. A deleted or absent object returns NotFound; a live empty object
returns an empty buffer. The caller supplies a maximum object byte count, checked
before allocating or connecting to data nodes. Allocation failure returns an error.
The client visits all extents of each group together, retaining one data-group
connection and looking up each distinct group once. It copies validated responses
into their original object offsets, preserving byte order even for alternating
groups. Memory includes the bounded complete output, at most 4096 ordering entries,
one extent response and transport buffers. This API reads a complete object, not a
stream or range; it does not enable HTTP routing or reclaim old pages. A concurrent
manifest update does not change the captured generation. Overwriting one of its
pages causes sequence validation to fail even when the replacement bytes match.

## Allocating pages before publication

`RemoteMetaClient::allocate_object_pages(count)` commits a namespace for 1..4096
pages. `allocation.page(index)` maps its 64-bit ID to a volume/chunk pair and checks
the page index. Concurrent allocations in one retained metadata history have
distinct IDs. The ID comes from the next committed metadata sequence, so heartbeats
and other mutations may advance it between allocations; callers must not assume
consecutive IDs. The small version-1 placement outcome advances that sequence
without copying node records or the object catalog. Existing snapshots preserve it.
Raft replies after committed apply; local nodes sync the outcome journal before
replying and refuse allocation without durable storage. Exhaustion is an error.
Requests and replies are bounded canonical binary protobuf (request code 77).

Allocate once for a new object generation, then write each page using
`write_chunk_if_sequence(page, 0, bytes, 0)` through a verified data group. The zero
condition requires absence, so an old manually addressed page cannot be overwritten
by a new allocation. Confirm data writes and publish the manifest only afterwards.
A lost allocation reply leaves an unused namespace: request another allocation,
never guess the lost ID from metadata status. Allocation does not reserve capacity,
create data pages, or publish an object. It does not automatically replay an
uncertain request, garbage-collect unused IDs, or reclaim abandoned data writes.

Uniqueness belongs to one authoritative metadata history. Do not reset or clone
that history while retaining its data groups; recovering both planes must preserve
their committed state. Raw page writes are still available and can mutate existing
pages, so writers must honor immutable-page ownership. A read detects a changed
sequence but cannot prevent a raw writer from modifying the page. Automated write
orchestration, access control and safe reclamation remain separate work.

## Writing complete objects

`RemoteMetaClient::write_object(bucket, key, expected, data, options)` joins the
steps above. `expected` is the exact current manifest, including a tombstone when
recreating an object, or None for absence. Preflight validates names, generation,
size, page count and options, then checks the current manifest before writing.
Placement selects up to `target_groups` configured groups, capped by page count.
The client looks up each selected group once and verifies every group before
allocation. It retains these configurations for this operation and keeps the
first write target’s verified connection through allocation. It writes and verifies
all pages of that group before moving to each remaining group once. Switching groups
opens a new connection using the saved configuration and verifies its complete
identity again. Manifest extents retain their original object order; only I/O order
changes. For nonempty objects with P pages over G healthy groups, preflight and writing use at most
2G - 1 group connections instead of up to P + G - 1. Reads use G group lookups
and connections instead of up to P. Configurations are not cached across operations, and each lookup
checks that the returned placement name matches the request. The client then
allocates fresh pages and distributes contiguous
extents across those groups. Each page is written with expected sequence zero,
then read back to check sequence one and exact bytes. Only then does the client
submit the conditional publication. Empty objects need no data pages.

`ObjectWriteOptions` defaults to a 64 MiB object limit, 1 MiB pages and one data
group. Pages must be 1..1048576 bytes, with no more than 4096 per object. The caller
supplies the modification time. The input is borrowed; temporary payload memory
is proportional to one page plus transport buffers, bounded manifest state and
one configuration per selected group (each wire response is at most 4 KiB).
Connections have their configured per-call timeouts; there is no whole-operation
timeout. Placement currently reserves the complete object length on every target
group, even when extents are spread across groups.

`ObjectWriteFailure::BeforePublication` means publication was not admitted. This
includes a request that was never sent and a typed leader refusal from every
contacted voter. Ordinary error text is not evidence of non-admission.
Its optional staged manifest identifies candidate pages, some of which may be
unwritten or unverified; it is diagnostic state, not authorization to publish it.
A data error never triggers an automatic write retry or publication. Failed writes
may leave data behind, which the client does not reclaim. An early manifest check
reduces wasted writes but does not replace the final publication condition.

After a publication error whose admission is uncertain, the client reads the manifest and returns success only
when the exact attempted result is visible. Otherwise it returns
`PublicationUnconfirmed` with the complete attempted outcome. It never replays
publication automatically. Reconnect and read metadata to investigate this state;
a different value may mean the attempted generation was superseded, so it does not
prove that publication never committed. Catalog clients can reconnect for this
read; a failed single-endpoint connection needs replacement by the caller.

The helper does not enforce page immutability against raw writers, reclaim old or
abandoned pages, add authentication, or enable the HTTP server's distributed path.

## Complete-object load and recovery check

The `object_writer_load_preserves_manifests_and_pages_through_recovery` integration
test runs the complete writer with six clients, three data groups and twelve
processes. It verifies 120 objects across leader loss and full restart and runs in
CI. It shares the process harness with the older placement/data workload, which
continues to run independently.

The larger check is explicit because it starts 51 processes (sixteen three-voter
data groups and three metadata voters):

```sh
cargo test --locked --all-features --test network_chunk_consensus \
  coupled_groups::sixteen_groups_preserve_object_writes_and_reads_through_recovery \
  -- --exact --ignored --nocapture
```

This profile uses eight clients, 1 KiB objects split into four 256-byte pages,
16 KiB checkpoint intervals, and sixteen metadata request executors per voter.
The fixture sets `MATRIXOBJECT_METASERVER_THREADS=16` explicitly and checks the
startup report. It writes 1024 objects, loses every data-group leader and the
metadata leader, verifies all objects, writes another 256, restarts all processes,
and verifies all 1280 objects. Verification checks payloads, manifest generation,
page sequences and matching placement records, plus group usage and committed
capacity. Every data group must carry objects. Each write/read phase keeps its
180-second bound; increasing concurrency does not loosen that deadline.

An October 29, 2026 run on one 16-logical-CPU WSL host with debug binaries took
212.62 seconds overall. Initial writes took 121.81 seconds (8.4 objects/second,
median 0.942 seconds, p95 1.426 seconds); the post-failover writes took 28.29 seconds
(9.0 objects/second). Verified reads took 21.49 seconds after leader loss and
32.46 seconds after the full restart. These are local functional-load measurements,
not production throughput or multi-host scaling results.

A subsequent run with operation-scoped group-configuration reuse passed in
302.78 seconds. Initial writes took 171.50 seconds (6.0 objects/second, median
1.323 seconds, p95 2.130 seconds), post-failover writes took 35.16 seconds, and
verified reads took 30.68 and 54.48 seconds. This sample was slower than the earlier
run and does not demonstrate a throughput improvement. The protocol test proves
fewer group lookups and connection attempts; throughput still needs controlled
measurements on deployment hardware. The initial phase also approached its
180-second bound, so this host/profile has limited demonstrated load margin.

With local bootstrap-catalog reads, another run passed in 242.42 seconds. Initial
writes took 138.74 seconds (7.4 objects/second, median 1.065 seconds, p95 1.649
seconds), post-failover writes took 33.68 seconds, and verified reads took 22.48
and 37.81 seconds. This is faster than the preceding sample but slower than the
first sample; these runs do not isolate code changes from host variability. All
1280 objects passed recovery checks with the original phase limits.

The same object workload with the default two metadata executors hit the unchanged
180-second write-phase bound after 777 completed writes, before recovery checks.
Each executor waits for its Raft result, so two executors limit how many requests
can reach the existing batcher while 48 chunk processes report health. Sixteen
executors passed this workload. Size `MATRIXOBJECT_METASERVER_THREADS` for the
request and heartbeat load, then measure on the deployment hardware; the product
default remains two. Per-page readback and the remaining metadata operations
still cost round trips. This check does not certify large catalog cardinality, large-object
bandwidth, sustained soak behavior, authentication, or multi-host fault isolation.

## Isolating an unavailable data group

The twelve-process test
`coupled_groups::unavailable_data_group_is_isolated_and_recovers_without_losing_objects`
starts three data groups and one metadata group, then removes two voters from one
data group. After heartbeat expiry, metadata must mark that group offline while
the other two stay online. Existing manifests remain readable; payload reads from
the minority must fail. A conditional write against an existing page must fail
without changing that page, including if its request finishes after recovery.

The test verifies objects on the healthy groups and publishes 24 more objects,
all placed outside the offline group. Restoring just one stopped voter must
restore quorum, health and all 72 object payloads with their original manifests
and capacity accounting. It then starts the final voter. This is process-stop
isolation on one host; asymmetric network partitions and independent host loss
remain separate deployment tests. Heartbeat expiry is an availability signal,
not an authorization or data-integrity boundary.

## Bootstrap catalog reads

In Raft mode, catalog reads require confirmation from a metadata quorum. A
follower redirects the client; a minority cannot answer even for a missing group.
Reads do not append outcomes. The immutable inventory survives metadata restart,
while committed membership fences identify current chunk voters.

`ReadChunkRoute` returns inventory and fence together. Object readers and writers
select current incoming and outgoing voters, exclude learners and removed members,
and verify each socket's member identity before sending payload operations. A
route does not prove data-group availability. A caller obtains a fresh route for
a later operation when membership changes; uncertain writes are never replayed.

## Connection failures during identity discovery

Metadata and data Raft catalog clients can try the remaining endpoints when an
identity exchange leaves its connection unusable. This applies both to initial
discovery and to a new connection during routing. The application request has not
yet been sent on that connection, so this does not replay a possibly committed
mutation. Each catalog pass remains bounded by its endpoint count and configured
connection/read/write timeouts.

Every replacement connection must still return the required complete identity.
A decoded server refusal, unsupported identity request, or valid identity that
conflicts with the expected configuration stops the attempt. After an application
mutation is sent, an uncertain result still stops automatic routing. The socket
tests cover initial and redirected discovery failure, exhaustion, identity and
unsupported-request refusal, and the existing no-replay behavior.

## Suspended leaders and late requests

The Linux process test
`coupled_groups::suspended_leaders_rejoin_without_serving_stale_objects` uses nine
processes: three metadata voters and two three-voter data groups. It publishes an
object across both data groups, sends SIGSTOP to each current leader, and confirms
the children are stopped through `/proc`. The processes retain their sockets and
in-memory state. Reads sent directly to the suspended leaders must time out, while
the remaining quorums publish and read a replacement object.

After that replacement is acknowledged, the test sends a publication with the old
expected manifest and expected-zero writes for already-created replacement pages
to the still-suspended leaders. These calls time out and are treated as uncertain.
The test resumes the leaders with SIGCONT and checks that any successful direct
reads return the newer state, never stale payloads or absence. It then removes a
different voter from each group so progress requires the resumed voters, publishes
another generation, and verifies that generation after a full restart.

Child ownership is retained during suspension, so normal completion and assertion
failure both clean up the processes. This test covers process pauses, surviving
sockets, late conditional requests and rejoining voters on one Linux host. It does
not establish asymmetric network-partition behavior or independent-host isolation.

## Catalog cardinality and recovery

The manifest catalog admits at most 100,000 distinct object keys and a 64 MiB
encoded catalog body. Both limits apply; large keys or extent lists can reach the
byte limit first. Tombstones retain their slots and generation fences, so deleting
an object does not free capacity for a different key. Replacing or recreating an
existing key is still allowed at the count limit when its expected state matches
and the resulting encoded catalog stays within the byte limit.

The CI test
`object_manifest::tests::full_manifest_catalog_replays_and_recovers_without_losing_capacity_fences`
creates 100,000 empty objects with short keys and paired placement records through
publication outcomes. It applies each canonical binary outcome to a second state
machine and compares the states. It checks both admission and decoded-outcome
replay reject an extra object without mutation, then deletes selected objects at
the beginning, middle and end of the catalog. Snapshot encoding, byte decoding,
stream decoding and installation preserve those tombstones and all placements.
Recreation after recovery requires the retained generation and keeps the catalog
at 100,000 keys.

An October 29, 2026 debug run produced a 13,700,603-byte metadata snapshot and
completed in 19.86 seconds on the local WSL host. This is a bounded in-process
metadata correctness check with empty objects. It does not measure data-node
storage, network throughput, distributed checkpoint transfer, peak memory, or
production behavior at that cardinality. Safe tombstone reclamation and larger
catalogs require further work; the current limits are not a production-scale
certification.

## Large-catalog Raft checkpoint recovery

The CI test
`cluster::meta_checkpoint::tests::full_manifest_catalog_transfers_between_raft_voters_and_recovers_later_publication`
extends the durable-peer checkpoint harness with 100,000 empty-object manifests
and their paired placements. A deterministic fixture command initializes the
catalog on a quorum while the third voter is isolated. The leader checkpoints
that state, then proposes a real binary publication outcome that tombstones one
object. The lagging voter must install the streamed checkpoint and apply the later
publication in order.

The harness drops the first file-transfer packet and duplicates subsequent file
packets. All voters must reach matching validated metadata checksums, retain all
100,000 entries, and observe the new tombstone generation. It then closes and
reopens every durable peer and repeats those checks from disk recovery. The
observed checkpoint was 13,700,618 bytes; receiver installation and peer restart
exercised three snapshot restores. The five enabled metadata-checkpoint tests,
including the original record-based transfer scenario, passed together in 28.31
seconds on the local debug build.

This uses the production Raft peer packet codec, durable storage, streamed
checkpoint capture/restore and binary publication application. Packets are driven
by an in-process test loop, and bulk catalog population uses a fixture command.
It does not measure TCP server admission, live-client throughput, independent-host
failure isolation, or production memory use at 100,000 objects.

## Full-catalog live metadata servers

`cluster::meta_server::catalog_recovery_tests::full_catalog_tcp_servers_checkpoint_rejoin_and_restart`
starts three TCP metadata servers from identical preseeded 100,000-entry catalogs.
The seed is installed inside the test before Raft starts; no production seeding
API is added. With one follower stopped, clients publish 64 tombstones over TCP.
The surviving voters must compact a file-backed checkpoint beyond the stopped
voter's initial log position. The stopped server reopens its retained storage,
and the other follower is stopped so the recovered voter is required for quorum.
A client then recreates one object and all servers restart from retained storage.
Selected changed and unchanged manifests must retain their expected generations.

The servers run production request and Raft workers with real TCP peer transport
on one host. Metadata request pools use 16 workers explicitly; product defaults
are unchanged. Empty objects isolate metadata recovery and do not test chunk
payload transfer. This is a recovery guard, not a throughput or multi-host result.

## Default maximum object recovery

The CI test
`coupled_groups::maximum_default_object_survives_group_failover_and_total_restart`
writes one 64 MiB object through `write_object` with the default object and page
limits. Nine processes provide three metadata voters and two three-voter data
groups. The test uses sixteen metadata request executors, 128 MiB capacity and
128 pages per data voter, one-second heartbeats and 16 MiB chunk checkpoint
intervals. Each data group holds 32 of the object's 64 one-MiB pages.

The reader verifies the complete payload and manifest before faults, after every
group leader and the metadata leader are stopped, and after all processes restart.
A fresh publication after leader loss must also survive restart. A one-byte-over-limit
write is refused before staging or publication, and a read limited below the
object's length is refused. Chunk checkpoints must have been created before the
leader failures; recovery may combine checkpoint state and retained outcomes.

The initial local debug run wrote the 67,108,864-byte object in 27.03 seconds and
completed the scenario in 48.17 seconds. This is a functional boundary check on
one host, not sustained large-object bandwidth, a whole-operation deadline,
or multi-host failure isolation.

A separate 51-process run on commit
`974817c5e174e8275ceed577cb34d4ca4a9a1a12` verified all 1,280 one-KiB objects after
leader loss and total restart in 163.60 seconds. Its initial 1,024 writes took
96.07 seconds, post-failover 256 writes took 20.12 seconds, and verified reads took
12.18 and 25.45 seconds. These observations follow the nonblocking peer-receive
change but are not a controlled performance comparison.

## Release-build recovery measurement

On October 29, 2026, commit
`84006f35c7de6c7fc4753115badc0415b290a70d` passed the complete-object
51-process scenario in release mode on a 16-logical-CPU WSL2 host with
24,032 MiB of reported memory and Rust 1.98.0. The test runs three metadata
voters and sixteen three-voter data groups, with eight concurrent clients and
sixteen metadata request workers. Objects contain 1 KiB across four 256-byte
pages. It stops the metadata leader and each data-group leader, verifies the
initial objects, writes more objects, then restarts all servers and verifies
all 1,280 objects.

| Phase | Objects | Elapsed | Writes/second | Write p50 / p95 / p99 |
|---|---:|---:|---:|---|
| Initial writes | 1,024 | 76.164 s | 13.4 | 595.931 / 922.363 / 1,004.253 ms |
| Reads after leader failures | 1,024 | 11.325 s | - | - |
| Writes after leader failures | 256 | 17.851 s | 14.3 | 530.055 / 855.554 / 1,003.744 ms |
| Reads after full restart | 1,280 | 17.755 s | - | - |

The complete test passed in 134.10 seconds. Reproduce with:

```sh
cargo test --release --locked --all-features --test network_chunk_consensus coupled_groups::sixteen_groups_preserve_object_writes_and_reads_through_recovery -- --exact --ignored --nocapture --test-threads=1
```

These are observations from one local run, with all servers sharing one host,
network stack and storage device. They establish recovery for this workload;
they do not establish independent failure domains, a sustained throughput target,
peak resource use, or production latency. No controlled comparison with earlier
runs was performed. The measured write throughput remains modest and requires
profiling before making performance commitments.

The connection-count regression uses 32 alternating extents across two groups,
including a partial final page. It requires three total write-side connections
(including preflight), two read-side connections, and exactly one metadata lookup
per group per operation. It validates the original manifest order and complete
reconstructed bytes. Real Raft process tests separately exercise durability,
leader loss, damaged extents and conditional publication.

## Striped object load and recovery

The complete-object workload also supports two data groups per object. Four
256-byte extents alternate between the selected groups, exercising repeated-group
connection reuse while preserving the 1 KiB object bytes and generation. The
workload verifies that every configured group holds data. Physical usage counts
only that group’s extent bytes; committed accounting retains the complete object
length on each selected group, as required by current placement accounting.

CI runs `coupled_groups::striped_objects_preserve_pages_across_group_failover_and_restart`
with three data groups, three voters per group and three metadata voters. It
writes 96 objects, kills every group leader and the metadata leader, checks reads,
writes 24 more objects, restarts all 12 processes and checks all 120 objects.

Run the larger case explicitly:

```sh
cargo test --release --locked --all-features --test network_chunk_consensus coupled_groups::sixteen_groups_preserve_striped_objects_through_recovery -- --exact --ignored --nocapture --test-threads=1
```

A local release run used 16 three-voter data groups, three metadata voters, eight
writer clients and 16 metadata executors. It completed in 48.72 seconds: 1,024
initial object writes took 29,682 ms, reads after 17 leader losses took 1,439 ms,
256 subsequent writes took 6,933 ms, and all 1,280 objects were read after full
restart in 2,117 ms. Every phase retained the 180-second workload bound and made
no automatic mutation retries. The existing one-group-per-object case remains
separate. These local results are recovery evidence, not a controlled performance
comparison, sustained throughput target or multihost qualification.

### An unavailable group during metadata failover

The CI-pinned `coupled_groups::unavailable_group_preserves_striped_objects_and_other_group_writes`
starts three three-voter data groups and three metadata voters. It writes 48
objects striped over two groups, stops two voters in one data group and waits for
metadata to mark that group unavailable. It then kills the metadata leader. The
existing metadata client must follow the new leader, retain the exact manifest,
and keep the minority data group excluded from placement.

A read spanning the unavailable group fails; a conditional write through its lone
voter also fails. Objects entirely on the other groups remain readable, and 24 new
objects use only those groups. Restoring one stopped voter recovers quorum and
makes all 72 objects readable while the third voter remains stopped. Restoring the
last voter preserves physical usage, committed accounting and the original
manifest. This tests an overlapping control-plane and data-plane failure locally;
it does not establish multihost partition behavior.
