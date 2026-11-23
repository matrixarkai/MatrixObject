# Local publication storage writes

Explicit local kernel methods complement the publication readers. They do not
activate a distributed wire emitter, cluster capability or default object backend.
The caller must establish authoritative allocation, object incarnation and writer
ownership. A successful local method is not a quorum decision.

## Initialization

`initialize_publication` writes a 64-byte MOL3 initialization control, kind4,
only to a fresh file. It has zero payload and published length, a nonzero writer
epoch and an opaque 16-byte initialization identity. The control is not a payload
extent. Initialization syncs the file and, on Unix, its parent directory before
returning. Directory-sync failure makes the handle unavailable. Store directory creation also
syncs newly created directory ancestry on Unix before the store can serve requests.
This work runs at store creation, not per append or sealed read. An exact retry
with the same identity and current epoch resyncs without changing bytes or data;
a changed identity or epoch is refused.

Existing legacy records, old publication records without initialization, and an
already used file cannot be implicitly initialized. A missing member of an
existing group needs verified history transfer. File freshness alone is not
permission to replace lost replica history with an empty initialized member.
Metadata authorization and ready qualification remain integration work.

## Appends and fences

`append_published` accepts exact identified records after initialization. A record
either retains the prior published prefix or advances it to its physical end.
Records may arrive out of order within the existing bounded reorder window;
waiting records retain their publication fields. Retries must preserve complete
record boundaries, bytes, identity and publication phase. A changed phase cannot
turn an earlier partial fragment into a completed request.

Data appends remain durable only after `commit`. A captured earlier sync cannot
publish later records. The caller must mark only the original request's final
record as complete and confirm the required replicas before returning an object
outcome. Epoch changes cannot complete an unfinished group directly.

`fence_publication` chooses this replica's own complete prefix under its storage
lock. It discards only the incomplete physical suffix and waiting records, retains
initialization identity and advances the epoch. No client-selected shorter
published length is accepted. Its result requires `commit` for durability. An
exact repeat does not add another control record. Selecting and verifying a shared
history across replicas still needs the distributed recovery protocol.

`seal_publication` requires all applied bytes to be complete and no waiting
records. It syncs the seal and optional digest, then drops resident payload bytes.
The old append/truncate/seal APIs continue refusing publication-aware files.
Existing wire mutations still refuse these files; version3 remains read-only.

## Validation and deployment

Twenty-three focused kernel tests and a store-directory test cover initialization layout and cold replay,
idempotence, directory-sync failure, invalid formats, exact retries, reorder,
publication boundaries, preserving fences, stale epochs, seals, every torn
initialization header, complete-header corruption, captured sync lengths and a
replica-server read round trip. The extent index size is unchanged.

The initialization shape must be readable throughout a deployment before any
cluster writer uses it. Read-only version3 support alone does not authorize
emission. Genesis qualification, writer lease handoff, quorum write-back,
replacement catch-up, metadata route checks, atomic PUT/CAS/copy/delete and
configured multi-chunk requests still require integration and fault validation
before changing the default object path.
