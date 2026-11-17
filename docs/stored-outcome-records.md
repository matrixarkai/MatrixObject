# Stored outcome records

DurableOutcomeLog stores decided mutations and immutable payload sectors in one
append-only file. Each successful batch uses one data synchronization for both
the binary protobuf metadata and its payloads. This is a storage API; the live
chunkserver still uses its existing journal and full-image checkpoints. Switching
server routing, metadata-only checkpoints and legacy recovery remains server work.

## API and limits

The API is durable_payload::outcome_log. Open a dedicated file with
DurableOutcomeLog::open and append already decided PageOutcome or
NamespaceFenceOutcome values using append_page or append_decided_batch. A batch
contains 1 through 64 decisions and at most 8 MiB of original encoded outcomes.
The caller validates the decided state before persistence. A batch is a durability
group, not a transaction across its logical mutations.

Patch positions and lengths remain in metadata; patch bytes are packed into one
stream split into extents of at most 1 MiB. Small sparse patches share sectors.
The default capacity is 64 MiB, configurable in sector multiples through 512 MiB,
excluding the 4-KiB file header. Argument and capacity errors occur before writing.
I/O failure or writer poisoning fences the handle and returns no committed
position. Success follows synchronization and publication of the readable end.

read_metadata and next_metadata read only the record header. read_payload_range
verifies the touched payload sectors. read_outcomes reconstructs logical outcomes
and verifies their original encoded length and SHA-256 digest. replay_from applies
those verified decisions in order through the existing logical outcome appliers.
The caller must discard a partially replayed service if replay fails; it must not
serve partial recovery. Older committed reads do not wait for writer sync.

## Binary file profile

The file header uses payload profile 2. Profile 1 raw extent files and profile 2
outcome files refuse each other's readers. There is no in-place conversion.
The stable identity lock, directory synchronization and capacity checks follow
[durable payload extents](durable-payload-extents.md). Header byte 4095 records
whether the file has held records, preventing a lost first record from reopening
as a new empty log.

Every record begins on a 4-KiB boundary with this 128-byte envelope. Integer fields
are big endian; the protobuf body follows the envelope and header padding is zero.

| Bytes | Value |
|---|---|
| 0..8 | MOEXREC1 |
| 8..12 | Padded header span |
| 12..16 | Protobuf body length |
| 16..24 | Complete record span |
| 24..56 | SHA-256 of the protobuf body |
| 56..88 | SHA-256 of bytes 0..56 followed by bytes 88..112 |
| 88..96 | Absolute record offset |
| 96..112 | File identity |
| 112..127 | Reserved zeros |
| 127 | Record marker, zero while pending and A5 when published |

StoredOutcomeBatch version 1 holds stripped page outcomes or namespace fences,
patch lengths, stream offsets, original logical outcome digests and lengths, and
PayloadExtent descriptors. StoredOutcomePosition identifies a node-local file and
record offset and length. These physical descriptors are not global object
manifest extents and contain no paths. Metadata-only decisions carry no payload
extent. File identity and absolute offset binding also protect these records
against moving them within a file or copying them between files.

The decoder bounds the header at 16 MiB and preflights protobuf message counts
before decoding. Canonical encoding, versions, field shapes, reserved bytes,
checksums, extent geometry and contiguous stream coverage are checked before
metadata is accepted. Full payload validation is a separate read/replay step;
metadata inspection alone does not establish that payload bytes are intact.

## Recovery and evidence

Recovery scans bounded metadata headers. Truncated records retaining their
published marker, unknown tails and damaged published metadata are refused
without truncating the file.
An identifiable incomplete pending tail can be removed after the valid prefix.
A complete pending record is fully verified before adoption. A mutation whose
caller never received success can therefore replay after recovery; callers need
logical retry/fencing rules, as with the existing journal. An intact prefix alone
does not prove that no complete trailing record was removed. Catalog recovery
must validate its retained positions and payload roots before serving state;
opening the log is not a substitute for those ownership checks.

Golden tests pin binary encoding. Tests cover one synchronization per batch,
namespace ordering, metadata-only transitions, sparse packing, refusal before
writes, I/O fencing, corruption, file/offset binding and concurrent reads during
sync. Local WSL process tests keep three independent writers alive together,
kill them, and replay each file. They also kill writers with partial tails and
before synchronization returns. These are storage-engine process tests, not
network replication or metadata-quorum validation.

Allocation tests measure a 4-KiB append and metadata read through 4-, 32- and
128-MiB prefixes. They bound allocation growth rather than proving distributed
throughput. [Managed outcome segments](outcome-segment-rollover.md) add file rollover and
bounded reader caching. Retained read generations, safe reclamation and
metadata-only catalog/checkpoint integration remain required
before this API changes the running server's performance.
