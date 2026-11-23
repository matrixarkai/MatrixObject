# Identified native writes

`ChunkLogWriter::open_identified` checks version 2 identity-read capability on all
three distinct endpoints before opening or fencing the chunk. Identified writes
retain the existing all-three durable acknowledgement policy. The legacy writer
API remains available explicitly; an identified request is never downgraded.

`append_identified` takes the expected physical offset, fragment logical offset,
payload and `AppendIdentity`. The identity supplies the object incarnation, original
client attempt ID and complete original request range. The caller acquires the
object's writer lease separately and serializes its requests. The native API does
not replace the metadata lease or implement gateway default selection.

The writer retains one unsettled piece at a time. Larger pieces split at the
existing record limit with global boundaries shared by all replicas. An exact
pending retry retains its context and bytes; a different request waits for
settlement. The existing chunk and record limits bound the retained body and
fragment descriptors. Descriptor capacity is reused between settled requests.

A correlated successful append reply confirms that record without another query.
After an uncertain reply, the returning replica's length alone is insufficient:
the writer checks the exact durable record tag and boundary, then validates its
bytes against the retained payload. Both replies must remain at the writer epoch,
and the read must retain the durable prefix. A foreign tag, legacy record, changed
payload or boundary stops the writer with `AttemptMismatch`. Equal bytes alone
never establish ownership.

Tags are the first sixteen bytes of SHA-256 over `MOSRS-LOG-ATTEMPT1`, the two
sixteen-byte incarnation and attempt IDs, original offset and length, fragment
logical offset, volume, chunk, generation, physical offset and record length.
Integers are little endian; generation and record length are32bits, the other
integers64bits. The pinned encoding test guards this durable derivation. Epoch is
excluded so a later lease holder can recognize an earlier request. The storage
record already includes the tag in its CRC-covered binary framing.

The process probe exercises the public writer through metadata leases and three
native replicas. Gateway settlement, object overwrite/delete lease fencing,
default LOG routing and full object/SDK integration remain separate work. These
correctness checks do not establish scale or performance results.
