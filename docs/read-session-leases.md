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

In a catalog with publication fencing enabled, ordinary whole-object, range and
batch reads acquire 60-second sessions for the exact selected generations before
any payload I/O. All grants in a batch must succeed first. Duplicate object entries
share a grant; conflicting generations refuse the whole operation. A concurrent
overwrite before acquisition refuses the read rather than selecting another
generation. Successful grants preserve their generations across overwrite or delete.

One monotonic deadline covers all grants, payload reads and cleanup, with a
100-ms lease margin and the caller's earlier deadline. Failure releases every
known grant; lost acquisition replies are never replayed. Leases left by an
uncertain result expire automatically. The existing 128-handle/64-MiB catalog
bounds apply to concurrent readers; exhaustion refuses admission.

Unfenced catalogs retain legacy ordinary-read behavior. Batched metadata
selection always requires native batch support: older servers refuse it instead
of substituting separate confirmations. Manifest-route replies carry the
publication fence in field 4 and use version 2 when it is positive; zero retains
version-1 bytes. Every batch entry must carry the same policy. Once observed,
the client preserves the fence across reconnects and refuses a missing or
different policy. Older SDKs reject the version-2 response.

Upgrade metadata voters together before enabling this policy. Drain readers
started before activation and upgrade all reader clients before any future
reclamation rollout. This only protects supported SDK data-read methods; raw
manifest retrieval and direct data-node reads do not acquire sessions.

Automatic grants bind the selected manifest's chain digest and return a
compact acknowledgement. Metadata request version 3 action 8 adds digest field 10;
reply version 4 carries handle ID, absolute deadline, relative lease window and
digest field 8, with no retained manifest. Generation and digest must match the
current live object before admission. The client refuses a changed digest, lease
window or reply kind. Upgrade serving metadata endpoints before deploying this
SDK path; older endpoints refuse the new request without automatic fallback.

The reply payload is at most 63 bytes for all permitted IDs, timestamps and TTLs.
A local one-extent fixture measured 87 bytes for the full grant versus 44 bytes
for the compact grant. At 4,096 extents, the corresponding payloads were 233,253
and 44 bytes; each selected-grant request was 59 bytes with the same short names.
These counts exclude transport framing, initial manifest lookup, release,
checkpoints and data traffic. Public explicit session APIs retain their full
record replies and existing request versions.

Creation persists exactly the same binary protobuf version-9 outcome as a full
grant. No new durable outcome or checkpoint format is introduced. Raft proposal
rollback accounts for compact grants before any acknowledgement; speculative
handles must disappear until commit.

Two metadata mutations per distinct object, retained-manifest catalog storage,
hashing and initial manifest transfer remain. Measure concurrent-reader admission,
CPU, latency and total transferred bytes before increasing limits. Batched grant
admission and reduced catalog work remain performance follow-ups.

## Lease-free short reads and the reclaim grace window

A fenced catalog makes every ordinary read hold a lease, which costs two durable metadata
writes per read (an open and a release, each a quorum commit). That is the price of a protection
nothing yet needs, because physical reclamation is disabled. The cluster feature
`FEATURE_READ_GRACE` (bit 3, declared by each voter and raised by an operator through the
existing feature mechanism, like the other bits) replaces it for short reads with a rule the
future reclaimer must keep.

### The rule

Let `T` be the reclaim grace window, 60 s (`READ_GRACE`).

1. **Reclaimer.** Whatever frees the pages of a generation of an object (a later generation, a
   deletion, or a copy no manifest names) frees them only when `now >= eligible_at`, where
   `eligible_at = commit time of the transition that superseded or deleted the generation + T`.
   `eligible_at` is part of the committed decision, not read from a clock when the decision is
   applied, so replay after an outage, a leader change or a restart reclaims nothing earlier than
   the schedule the first application made. A generation named by an explicit retained handle or
   an open session is not eligible while that lease is live, exactly as now. The first reclaimer
   must also keep the existing conditions (no manifest names the pages, no staged ownership
   covers them, the inventory and ownership records agree); the grace is an extra condition, not a
   replacement.
2. **Reader.** With the feature raised, a read whose operation deadline is no later than `T / 2`
   (`SHORT_READ`, 30 s) after it starts takes no lease. It must finish within `T / 2` of the moment
   it began to learn the manifest, measured on the reader's monotonic clock (the half leaves room
   for clock rate error and for the time between a supersession and its commit). A read that runs
   past that fails with a retryable error and returns no bytes. A read with no deadline, or a later
   one (a whole large object, a batch), takes a lease as before.
3. **Integrity.** An extent of a format 2 or later manifest carries a published SHA-256, which
   the data reader checks on every page it reads, together with the extent's length and
   `write_sequence`. The sequence does NOT tell a reused page from the original: a page's counter
   restarts at zero when its state is dropped, and compact-append extents all carry sequence 1.
   The digest is what does, so only reads of generations whose extents all carry one go without a
   lease (a format 1 generation keeps its lease). A page freed and reused for other data therefore
   makes a late read fail with corruption ("object extent does not match manifest"); it cannot
   return the other data. A lease-free read that fails with corruption is retried once after reading the
   manifest again (the new attempt names the current generation); real corruption fails again.
   The grace makes this rare; the integrity check makes it safe.
4. **Compatibility.** No voter may declare the feature unless its reclaimer, or the absence of
   one, keeps rule 1. A client takes no lease only when the cluster has raised the bit (it reads
   the feature set and believes a yes permanently, a no for 30 s), so a mixed or older cluster
   keeps taking leases. Raised bits are never lowered.

### What the first reclaimer must do

- Record `eligible_at` in the same committed outcome that supersedes or deletes a generation, or
  in a committed marking decision that is later than it, and never derive eligibility from the
  wall clock at apply time.
- Honour the lease table first (explicit handles and sessions), then the grace, then the existing
  ownership checks, and free nothing on a failure of any of them.
- Not shorten `T` while the feature is raised. A longer `T` is safe at once; a shorter one only
  after `T_old` has passed with the shorter value in force for new supersessions.
- Treat a page that is reused after its grace as lost to a late lease-free reader (the reader gets
  corruption, retries, and reads the new generation), and test that case.
- Keep the explicit lease path working for long reads, batches and operator tools.

### What the client does today

`with_read_leases` asks for the feature set once (and again after 30 s while the answer is no),
and takes no lease for a short read when the bit is raised. A read that overruns half the window
fails with `PreconditionFailed` ("a read ran past half the reclaim grace window; retry it"). The
gateway's read deadline is its operation timeout, so its reads are short reads. A gateway also keeps
a small cache of the leases it opens (, 16 by default, shared by its connections):
a read of a generation and digest it already holds a lease for, with at least 5 s of the 60 left,
reuses it, and a lease in use is never released for another read. The feature is raised with
 for each voter, then
.

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
disabled pending reader migration and drain, data fence propagation, stale-node
readmission and interrupted-deletion recovery.

## Wire format and measured bounds

Metadata opcode 92 request version 2 adds open, renew and expire actions 5–7,
TTL field 8 and expected-deadline field 9. Retained records add deadline field 3;
zero preserves permanent retention. Version-3 replies carry a relative lease
window. Version-2 replies acknowledge renewal with only ID and deadline.

Creation uses binary protobuf placement outcome version 9 and field 12. It binds
the handle ID, object name, generation, deadline and chain digest of the canonical
manifest protobuf. Replay verifies the current generation and digest before
retaining its manifest. Removal uses version 10 and field 13, binding the immutable
handle ID and exact prior deadline. A stale removal cannot erase a renewal.
Permanent handles retain their version-3 creation/removal format. Earlier
full-record session outcomes still replay.

Checkpoints retain complete manifests. Raft rollback bookkeeping shares immutable
retained records instead of copying their extent vectors. Deadline mutation
preserves captured snapshots through copy-on-write. Hashing streams the protobuf
encoder into a fixed scratch buffer instead of allocating a complete temporary
payload; validation and hashing still perform work proportional to manifest size.

Renewal remains version 8 and field 11. Tests with one and 4,096 extents keep its
outcome below 128 bytes and reply below 32 bytes.

| Extents | Full-record creation + removal journal bytes | Compact creation + removal journal bytes |
|---:|---:|---:|
| 1 | 252 | 150 |
| 4,096 | 466,588 | 150 |

These are actual standalone metadata journal file deltas for fixed object names, including frame
overhead. They exclude renewal, replies, checkpoints and filesystem/device
journaling. They do not establish constant CPU cost or end-to-end throughput.
The test `compact_read_session_journal_size_is_independent_of_extent_count_and_recovers_mixed_history`
pins these figures and reopens mixed old/new histories with active protection.

Use a coordinated metadata stop/upgrade/restart: automatic expiry also writes new
decisions. Upgrade every voter and snapshot consumer before restarting cleanup;
earlier binaries cannot apply versions 9 and 10. Client request/reply formats,
checkpoint records, permanent records and
version-1 requests retain their encoding. Local
WSL checks cover deletion during a paused session, deadline exhaustion, journal
restart, cleanup fairness, full handle capacity, lagging-voter checkpoint transfer,
leader loss, total restart, quorum loss and real Star/Raft object reads.
