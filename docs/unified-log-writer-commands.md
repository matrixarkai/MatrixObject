# Unified LOG writer commands

`LogChunkRequest::unified()` selects request version 2 for grants, switches and
reads. `RemoteMetaClient` exposes `grant_chunk_writer`, `switch_chunk_writer` and
`read_chunk_writer`. Replies use version 2 and `chunk_open: Option<ChunkExtent>`;
the earlier `open` field is absent. Clients require the reply version they asked
for. Older command readers reject version 2.

These commands require explicit activation of `FEATURE_UNIFIED_LOG_WRITERS`
after the complete metadata reader set declares support. Nothing raises it by
default. They operate only on the unified chunk catalog. Both catalog APIs reject
a grant to a key already owned by the other catalog, including retained
tombstones. This prevents a grant from silently changing storage authority or
restarting a generation counter.

A grant opens one empty LOG tail, or takes over the current tail after the prior
lease expires. A live holder's retry costs no metadata entry. A switch preserves
the object's attributes, seals its prior tail with the supplied length and footer,
and opens the next tail in the same outcome as its lease renewal. New chunk
identities derive from the committed metadata index. Object and image admission
limits apply during leader preparation; committed consumers use structural bounds.

Renewals and cleanup sweeps retain request version 1 and the shared lease table.
The leader's proposal draft saves only touched unified entries and their catalog
revision, restoring them before proposals can commit. This avoids cloning or
walking the whole catalog. Legacy-only batches allocate no unified-entry undo list.

`unified_log_writer_process_probe` exercises metadata leases and identified native
records with separate processes. It is a test fixture, not a gateway or a production
object writer. It does not establish gateway outcome settlement, default LOG
routing, write quorum policy, or performance results.
