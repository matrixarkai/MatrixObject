# Compact append planning

`RemoteMetaClient::plan_append_routes_compact` selects placement against an
exact live immutable manifest. `append_staged_object_compact` now uses it before
writing suffix pages. The request binds bucket, key, generation and canonical
manifest SHA-256. The metadata server checks the borrowed current manifest at a
current-term quorum read barrier, then returns the plan, selected identity and
routes. Neither direction carries the prefix extent list.

Opcode 95 uses canonical version 1 binary protobuf messages. Requests are limited
to 8 KiB and replies to the 8-MiB transport bound. The selected group count is
1–4,096. Plans of up to 32 groups include every route; wider plans omit inline
routes and the SDK fetches them in batches of at most 32. These additional route
reads have their own metadata barriers. Each route is checked against the
selected group and current membership fence before data access. Group count
selects placement groups; it does not change a Star group's replication thresholds.

Upgrade metadata voters before calling the new planner. The compact append
helper already requires version 11 publication support; it now also requires
opcode 95. Unsupported peers are refused without a full-manifest fallback.
Existing ordinary planning and write methods retain their existing protocols.
Planning is read-only and may retry across metadata leader changes. It grants
neither ownership nor publication authority: staged ownership, exact-current
publication, lease expiry, capacity and immutable-prefix checks still apply.
A concurrent publication after planning can therefore still refuse the append.

Client and server compute canonical manifest hashes without allocating a full
serialized manifest. Hashing remains linear in the prefix extent count. Building
the returned object and validating/persisting publication still involve owned
manifest copies. [Catalog snapshots and undo](manifest-catalog-sharing.md) now
share immutable entries. Compact planning bounds traffic for a fixed placement and avoids
cloning the prefix into a planning reply; it does not establish constant append
CPU, memory cost, physical I/O or native latency.

The library tests exercise widths 1, 32 and 33, compare wire lengths at 1 and
4,095 prefix extents, reject malformed/different identities, enforce route
membership and verify state remains unchanged. SDK tests verify the wider route
batches and reject a reply for a different selected identity. The WSL process
workload checks stale-digest refusal before data writes, leader-loss recovery,
minority refusal, full restart and each replica's resulting bytes.

The [append measurement](star-append-amplification.md) records the complete
cursor/reservation/planning/publication API window, alongside metadata and data
journal growth. Its compact path asserts total metadata requests below 1 KiB and
replies below 2 KiB at 1, 128 and 4,095 prior extents for one group and one fixed
suffix extent. These workload bounds depend on identifier lengths and placement
width; they are not limits for every valid request.

The release run observed 353–360 total metadata request bytes and exactly 544
metadata reply bytes across those prefix sizes, with unchanged 12,552-byte
three-replica data journal growth. At 4,095 prior extents, the initial compact
publication workflow had still returned 302,923 metadata reply bytes.
