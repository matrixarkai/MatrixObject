# Log chunk recovery and acknowledgement

Log chunks derive their contents, epoch and seal from ordered records. Appending
records does not acknowledge durability: callers must wait for a successful commit
or seal before acknowledging the corresponding changes.

Recovery keeps every complete valid record. A final short header with a recognizable
record prefix, or a valid header whose payload is incomplete, is removed and that
removal is synced. A complete header with invalid checksums or shape, or a complete
payload with an invalid checksum, returns corruption without modifying the file.
This includes zero-filled payloads and the last append, truncate or seal record.
A damaged complete record may already have been acknowledged; its position at EOF
is insufficient evidence that it was an interrupted write.

A full-size torn write that fails integrity checks is also refused. Repair must use
a verified replica rather than guessing a shorter acknowledged prefix. The existing
record encoding is unchanged; these checks add no record bytes or sync barriers.
Physical removal of bytes from a previously acknowledged file cannot be detected
by this encoding alone and requires replica-level reconciliation.

After a write, seek or sync error, the handle rejects every subsequent mutation and
commit with Unavailable. The original error is returned to the first caller. A sync
failure does not advance the durable length. Reopen and recover before retrying;
a failed operation may have reached storage despite returning an error. Inspecting
in-memory bytes after failure does not make them acknowledged or safe to serve.

The module remains separate from live replica journal routing. These storage-level
checks do not prove end-to-end Star acknowledgement, checkpoint latency, or QPS.

Validation covers complete-record corruption without file mutation, every byte cut
through a final append, zero-data and control-record headers, reorder/retry/seal
behavior, and Linux write/sync failures using local devices. Run the log_chunk_tests
library group in WSL; the log_tail_tests group exercises replica tail selection.


## Identified append records

Readers accept legacy 40-byte MOL1 headers and 56-byte MOL2 append headers in the
same file. MOL2 keeps fields through byte 36 unchanged, stores an opaque 16-byte
attempt tag in bytes 36..52, and stores the CRC-32C of bytes 0..52 in bytes 52..56.
Only append records use MOL2. The payload has its own checksum. Production writers
continue emitting MOL1 while compatible readers are deployed.

`append_record_identity` examines a complete durable record starting at an exact
logical offset. It rereads and checks the header against the replay index. An
interior offset, an incomplete durable range or a truncated record supplies no
complete identity. Legacy records return their range with no attempt tag.
The extent index retains its existing 32-byte size; tags are read from storage.

A tag is opaque storage metadata, not an acknowledgement by itself. The caller
must bind it to the object and range, fence pending writers, and establish replica
commitment before resolving an attempt. Matching payload bytes cannot establish
ownership. This reader deployment does not change gateway append routing, provide
a durable end-to-end receipt, or remove the need to return OutcomeUnknown when
ownership cannot be established.

## Explicit identified writes

`append_identified` stores the supplied tag with the complete append record. A retry
must match the tag, exact record range and payload. Equal bytes with another tag,
partial-record retries and overlapping extensions are refused. Legacy retries
cannot claim an identified record, and identified retries cannot claim a legacy
record. An exact retry adds no file record, including after reopening the file.

The existing bounded reorder window retains each waiting record's tag and epoch.
A pending identified attempt fences lower epochs in the live handle. To change
writer epoch with pending identified records, truncate to the selected tail and
commit the fence before appending. A gap fill cannot silently promote another
writer's pending attempts or discard their identities. Crash recovery still needs
the external writer lease and replica reconciliation; a volatile pending record
alone is not a durable fence.

This API uses the ordinary record write and commit path: the identified header
adds sixteen record bytes, with no new metadata receipt table or sync barrier.
Existing `append` callers keep emitting legacy records. Replica RPC dispatch and
gateway append routing do not yet call the identified API, so this storage-level
change alone does not settle gateway outcomes after restart.

## Replica protobuf operations

Version one log requests retain their byte encoding and legacy append behavior.
Version two adds identified appends and an exact-offset identity query. The tag is
represented by two fixed64 protobuf fields containing its first and second eight
bytes in little-endian order. Zero-valued words are omitted canonically; an all-zero
tag remains distinguishable from a legacy record by the operation version and
reply presence flags. Parsing the tag needs no heap buffer, and owned decoding
continues reusing the payload allocation for append data.

A query never creates a missing chunk or claims its directory. It reports only a
complete durable record, including its offset, length, and optional tag. Deferred
records are invisible to identity queries until their sync is completed and noted.
An interior position, removed record or missing chunk supplies no complete record.
Replies validate identity presence and the applied and durable range bounds.

The replica dispatcher uses the identified append API for version two requests,
including grouped writes. A replica restart reconstructs tags from log records.
This does not grant a writer lease, establish quorum, fence pending remote writes,
or prove absence globally. Gateway routing remains unchanged while the leased LOG
writer and outcome settlement are integrated.

## Native log directory ownership

One store owns a native log directory at a time. An operation that opens or creates a chunk,
including an existing-chunk read or tail query, takes a nonblocking exclusive lock on
`.writer.lock` before opening or caching any chunk. Competing stores receive a
precondition refusal; they cannot replace an active owner's acknowledged bytes.
The store and every pending sync handle retain ownership until their file handles
are released. After handoff, a new owner reopens and recovers the persisted state.

Queries for missing chunks return empty without creating files or claiming storage.
Constructing a page-only replica does not claim native storage, so page replicas
with distinct journals may still share a parent directory. Native replicas must
use distinct log directories. Place each server journal in its own parent directory;
native chunks use that parent's `logchunks` subdirectory. The lock is cooperative: stop older binaries that
do not implement directory ownership before accessing the same directory, and
retain the lock file while its owner is active. Ownership adds no per-record bytes
or additional sync barrier.

## Separate-process recovery fixture

Build `identified_log_process_probe` alongside the cluster binaries with
`cargo build --release --features metadata-consensus --bins --example star_catalog --example identified_log_process_probe`.
Use a fresh disposable cluster directory and three chunk-server endpoints.

Run `identified_log_process_probe seed EP,EP,EP READY_FILE`. After READY_FILE appears,
the replicas have committed a legacy prefix and an identified record, and the
client waits without printing a successful result. A controller can kill that
client and all three chunk-server processes, then restart the servers against their
same journals and native log directories. A fresh client running
`recover EP,EP,EP FENCE_READY_FILE` checks durable identity, rejects a different
attempt with equal bytes, retries the exact attempt, commits a higher-epoch tail
fence, and rejects a late old-epoch append. At FENCE_READY_FILE, kill that waiting
client and all three data processes again. After restarting them, a fresh client
running `append-second EP,EP,EP` must observe the persisted epoch fence and reject
the old epoch before appending equal bytes as a distinct record. Restart the three
servers a third time and run `verify EP,EP,EP` from another fresh client.

The `old-probe EP,EP,EP` mode uses fresh older replicas: version-two queries and
appends must be definitively refused while version-one writes remain usable.
This fixture inspects storage and protocol behavior. It does not exercise gateway
outcome settlement, acquire a metadata lease, inject a crash during publication,
or measure latency or amplification.
