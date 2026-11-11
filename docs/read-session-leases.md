# Bounded distributed read sessions

`open_read_session(bucket, key, generation, ttl_ms)` atomically selects and
durably protects a live manifest generation. Use `None` for the current
generation or an exact expected generation to refuse a concurrent change.
Tombstones cannot open sessions. TTLs range from 1,000 to 3,600,000 milliseconds.
Standalone metadata requires durable storage; replicated metadata requires quorum.

The returned `RetainedManifest` has an absolute metadata-server deadline.
Its generation and extents remain unchanged after overwrite or delete.
`read_retained_manifest` and `read_retained_object` can inspect and read a live
session. A relative remaining window in the metadata reply bounds data reads;
clients do not subtract their wall clock from the server's timestamp.
Expired sessions refuse reads even before cleanup removes their records.

`renew_read_session(&mut session, ttl_ms)` compares the exact previous deadline
and must extend a live session. Renewal remains possible after the current object
changes. A stale renewal or cleanup request cannot replace a newer deadline.
Inspect the handle after an uncertain renewal; never assume its deadline extended.

## Automatic acquisition and release

`read_object_in_session(bucket, key, max_bytes, options, ttl_ms)` acquires protection,
authenticates its extents, then releases after bytes have been copied into the
result. Star and optional Raft groups share this path. One operation deadline
covers acquisition, routing, reads and release. It is bounded by the caller's
deadline and a conservative monotonic lease window starting before acquisition,
with a 100-ms margin. A final check refuses success when cleanup consumed the
remaining operation window.

Acquisition is not replayed after an uncertain transport result. A client failure,
panic or uncertain cleanup may leave a leased handle; automatic expiry bounds
its lifetime. Explicit `release_manifest` remains idempotent after removal.

Ordinary `read_object`, range reads and batched reads retain their existing
behavior. Creation and release currently journal a full manifest record, and
the helper adds metadata work. Reduce that amplification, preserve batched
selection semantics, and measure concurrent-reader admission before migrating
those defaults.

## Bounded cleanup and recovery

Sessions and permanent handles share the 128-handle and 64-MiB catalog body
bounds. Renewal works at capacity and accounts for encoded deadline growth.
`expire_read_session` removes only the exact expired deadline; permanent handles
cannot be converted or removed through expiry.

The one-second cleanup pass shares at most four commands between staged ownership
and readers. Both receive two slots when both have work; unused slots can serve
the other class. Replicated cleanup requires the applied current-term leader
barrier and consensus commit, preserves foreground proposal capacity and avoids
duplicate queued cleanup. Standalone cleanup journals the same decisions.
Empty sweeps append nothing.

Outage and quorum loss can delay removal. Replay applies committed decisions
without consulting the recovering clock. Metadata clocks must remain synchronized;
these are not clock-skew-tolerant data-node leases. Physical reclamation stays
disabled pending complete reader adoption, data fence propagation, stale-node
readmission and interrupted-deletion recovery.

## Wire format and measured bounds

Metadata opcode 92 request version 2 adds open, renew and expire actions 5–7,
TTL field 8 and expected-deadline field 9. Retained records add deadline field 3;
zero preserves permanent retention. Version-3 replies carry a relative lease
window. Version-2 replies acknowledge renewal with only ID and deadline.

Creation and removal use existing binary protobuf retention outcomes. Renewal
uses placement outcome version 8 and field 11, with no manifest copy in the
payload or speculative rollback record. Tests with one and 4,096 extents keep
the renewal outcome below 128 bytes and reply below 32 bytes. This byte bound
does not establish constant CPU cost or end-to-end throughput.

Upgrade all metadata voters, clients and snapshot consumers before opening leased
handles. Permanent records and version-1 requests retain their encoding. Local
WSL checks cover deletion during a paused session, deadline exhaustion, journal
restart, cleanup fairness, full handle capacity, lagging-voter checkpoint transfer,
leader loss, total restart, quorum loss and real Star/Raft object reads.
