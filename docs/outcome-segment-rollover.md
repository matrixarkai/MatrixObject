# Managed outcome segments

SegmentedOutcomeLog manages the immutable files used by
[stored outcome records](stored-outcome-records.md). It preserves physical record
positions across file rollover and bounds the cache of sealed file readers.
The running chunkserver still uses its existing journal and full-image
checkpoints. This API supplies rollover needed by its future journal/catalog
integration; it does not change live server routing.

## Capacity and durable ordering

Open a dedicated anchor path with a per-segment capacity. The usual capacity is
64 MiB; supported capacities are sector multiples from 24 through 512 MiB,
excluding each file's 4-KiB header. The minimum accommodates any valid bounded
outcome batch, including its metadata. Batches retain the stored-record limits
of 64 decisions and 8 MiB of original encoded outcomes.

An ordinary batch synchronizes one selected segment. A pre-write capacity
refusal starts rollover: initialize a new empty segment, mark the old segment
sealed with a 128-byte chained manifest record and synchronize that record before
writing any mutation to the new segment. The initial manifest is created through
atomic replacement and directory synchronization.
Rollover therefore has additional initialization and manifest barriers; it is
not a one-barrier operation overall. A batch never spans files.

The anchor and its stable identity lock remain exclusively owned. A published
manifest binds its anchor identity, capacity, ordered segment identities and the
exact length of every sealed segment. Each segment retains its own file identity
and lock. Sealing downgrades its locks to shared locks so that an in-flight reader
can outlive cache eviction without preventing another reader from opening it.
A configured file limit starts rollover; an operating-system write or sync error
fences the manager and retains the original error. Invalid batches
do not cause rollover or manifest replacement.

## Binary metadata and reads

The anchor uses payload-file profile 3, distinct from raw extents and individual
outcome logs. Its final header byte records durable manifest initialization.
The initial manifest has an MOSEGM01 envelope, a big-endian protobuf-body length
and a SHA-256 body digest. Its version-1 protobuf body binds the anchor identity,
capacity and first segment. It is bounded at 1 KiB. Further rollovers append one
fixed 128-byte MOSEGL01 record, without rewriting earlier manifest bytes.

| Bytes | Rollover record value |
|---|---|
| 0..8 | MOSEGL01 |
| 8..16 | Sequential new segment number, big endian |
| 16..32 | Anchor identity |
| 32..48 | New segment identity |
| 48..56 | Previous segment's sealed length, big endian |
| 56..88 | Previous link digest, initially the protobuf body digest |
| 88..120 | SHA-256 of bytes 0..88 |
| 120..127 | Reserved zeros |
| 127 | Pending zero or published A5 marker |

Sequence, hash chaining, identities, reserved bytes and sealed lengths are
checked during recovery. Decode limits are 256 MiB and one million segments.
A named incomplete link can be removed only when the corresponding next file
is verified as an initialized empty segment that has never held records. Complete
valid pending links can be adopted. Unknown or damaged links are refused.

Ordered indexes avoid full-vector copying and hash-table rehashing on rollover.
Index operations take logarithmic work; the manifest write remains 128 bytes
regardless of the existing segment count. Startup reconstructs the segment
index and checks file headers. Explicit metadata scans traverse their captured
prefix; they are not part of an ordinary append.

Record positions and payload descriptors retain their individual segment
identities. Lookup uses an identity index rather than searching earlier files.
At most 16 sealed readers are cached, in addition to the active writer and
readers temporarily retained by concurrent calls. A cache miss checks the
selected existing file's identity, header and sealed length without scanning its
record prefix. Old reads do not take the append lock or wait for writer sync.

Metadata scans capture a committed cut. Appends, including those issued by a
scan callback, do not extend that scan. An optional last-applied position is
validated before scanning or replaying later records. Metadata inspection checks
record integrity; read_outcomes and replay_after additionally verify payloads
and logical outcome digests. Startup state must be discarded on replay error.

## Recovery and remaining integration

Published missing or mismatched segment files and identity markers are refused
without recreating them. Sealed files are opened read-only and their tails are
never repaired. Only the active file can apply the existing conservative pending
tail recovery. A missing initialized manifest is refused. A nonempty unlisted
next segment is refused; an empty file left by interrupted rollover can be reused.
Unpublished partial headers can be rebuilt through their stable identity protocol;
an initialized truncated file or a file that has held records is refused.
Retained catalog positions and payload roots still need validation before
serving state, as described in the stored-record recovery contract.

Tests cover binary encoding, ordered replay across rollover, old ranges,
bounded reader caching, cache eviction with retained readers, reads during sync,
publication failures and killed writer processes. Allocation tests cover
4-/32-/128-MiB prefixes that cross multiple files. These are local WSL storage
tests, not multi-host or network-quorum evidence.

Files are not reclaimed by this manager. Metadata-only catalog checkpoints,
retained read generations, safe reclamation, legacy journal recovery, repair,
state installation and the live Star journal adapter remain required. The
existing full-image checkpoint pause is still present.
