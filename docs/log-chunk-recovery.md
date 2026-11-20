# Log chunk recovery and acknowledgement

Log chunks derive their contents, epoch and seal from ordered MOL1 records. Appending
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
