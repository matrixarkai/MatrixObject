# Durable payload extents

The Unix payload storage API separates immutable data bytes from the binary
protobuf descriptors needed by a metadata catalog. It is a building block for
metadata-only checkpoints. Chunkserver journal routing and checkpoint integration
remain to be implemented; the current server still uses its existing journal.

## API and ordering

The module is available with either the replica-server or meta-server feature.
Open a dedicated file with PayloadExtentStore::open(path, capacity_bytes), then
use append or append_batch. A successful result follows payload synchronization.
The caller must durably publish the returned descriptors before acknowledging an
object mutation. Descriptor publication and object ownership belong to the
metadata/outcome layer.

Batches contain at most 64 extents and 8 MiB of logical input. Each extent is
1 byte through 1 MiB. There is one data synchronization per nonempty batch.
Capacity and argument refusals happen before writes. Persistence failures return
no descriptors and fence the handle; a poisoned writer also refuses reads and
writes. Known older extents can be verified by a separate read while the writer
is waiting for synchronization. Positional reads do not share the writer cursor.

The default file payload capacity is 64 MiB; capacity is an explicit sector
multiple through 512 MiB, excluding a 4-KiB file header. Stored capacity is part
of the file identity and must match on reopen. The runtime checks use local WSL.
File initialization requires Unix directory synchronization.

## Binary layout and integrity

The file starts with a 4-KiB header. Its first eight bytes identify the format;
a big-endian body length and SHA-256 checksum precede a version-1 protobuf body.
The body binds a random 16-byte store identity, sector size and file capacity.
Unused header bytes are zero and are validated on open.

Payloads follow at sector-aligned offsets. Every extent is padded with zeros to
a 4-KiB boundary. Appending writes only new sectors. A PayloadExtent protobuf
stores its version, store identity, byte offset, logical length, whole-payload
SHA-256 and concatenated SHA-256 checksums of its padded sectors. It carries no
payload bytes or machine-local path. Binary-format golden tests pin the encoded
header and descriptor. Unknown versions and malformed descriptors are refused.

Range reads verify only the sectors they touch. A complete read also verifies
the whole logical payload digest. At most two boundary sectors are read beyond
the requested byte range; unrelated sectors are not scanned. Padding is included
in sector integrity checks. A damaged requested sector fails the read.

A stable lock file holds the same store identity and its checksum. Initializing
the identity and data header precedes a durable ready marker. Interrupted
unpublished headers can be rebuilt; a missing or replaced initialized payload
file is refused. Exclusive ownership spans the data and stable lock files.

## Recovery roots

Ordinary open preserves all existing aligned payload bytes, including complete
unpublished extents. It refuses an unaligned interrupted tail. Explicit
open_recovered requires an existing identity and the complete published and
retained extent roots for that file. Identical roots share validation; conflicting
or overlapping roots are refused. All descriptors and unique payloads are checked
before any truncation. Only bytes after the highest published extent are removed;
interior holes remain allocated. An empty root set explicitly declares that no
payload is published. This API does not discover catalog ownership or perform
online garbage collection.

Process tests persist descriptors as binary protobuf, kill their writer without
running destructors, then reopen and validate the payload. They also inject
partial and complete unpublished tails and require invalid roots to preserve the
original file. Allocation checks cover a 4-KiB append and a 4-KiB read through a
1-MiB extent at file prefixes of 4, 32 and 128 MiB.

## Remaining server work

Integrate extent descriptors with versioned mutation outcomes, preserve legacy
journal recovery, and maintain a metadata catalog whose frozen base and updates
can be checkpointed independently of payloads. Repair, retained read generations,
file rollover, bounded caching and safe reclamation must use the same ownership
and persistence rules. Repeat distributed checkpoint/read/repair interference
measurements after that routing is complete; the existing checkpoint pause is
still present today.
