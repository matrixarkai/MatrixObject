# LOG publication record readers

This change introduces readers for `MOL3` append records. Existing writer APIs
continue emitting `MOL1` or `MOL2`. No RPC or capability enables emission yet.
The default object backend remains on its existing publication path.

## Durable header

The fixed header is 64 bytes. Bytes 0 through 35 retain the append header fields:
magic, kind, flags, reserved bytes, generation, epoch, physical offset, payload
length and payload CRC32C. Bytes 36 through 51 hold the opaque 16-byte attempt
tag. Bytes 52 through 59 hold the published chunk length as a little-endian u64.
Bytes 60 through 63 hold CRC32C of bytes 0 through 59. The payload follows.
Append records and an explicit first-record initialization control are valid
in this extended format; flags and reserved bytes must be zero. The initialization
control has kind4, zero offset/length/data CRC/published length, a nonzero epoch
and an opaque initialization identity. It is valid only as the first file record. `MOL1` and `MOL2` byte encodings are unchanged.

A record carries either the prior published length or its own physical end.
The former retains an unfinished request; the latter publishes all accumulated
fragments. Intermediate physical prefixes therefore remain distinct from complete
published requests. An unfinished group cannot continue under a different epoch
without truncating to a published boundary first. These checks supplement exact
attempt identity; matching payloads never establish ownership.

The next writer implementation must place its final marker only on the final
record of the original request, require the configured durable replicas, and
prove the original attempt on recovery. The record boundary primitive alone
does not implement complete distributed object settlement.

## Replay and visibility

Replay derives both physical and published lengths. `published_length` reports
the complete applied prefix. `durable_published_length` reports a published prefix
covered by the captured durable physical length. For a partial sync it finds the
last complete durable record and validates that record's indexed header. This
avoids interpreting a durable fragment end as a complete object append. The
legacy-only query remains a scalar operation. Extent memory size is unchanged.

A torn final header or payload is removed while preserving the prior published
length. Complete damaged headers or data are corruption. A changed version marker
cannot erase a complete older header by making it appear to be a short new one.

Existing seal and truncate control records remain readable. A seal requires all
physical bytes to be published. A truncation of publication-aware bytes must land
on a published boundary. Truncation does not clear publication mode. Legacy append
records following publication-aware records are refused, including after a complete
group. Current append, truncate and seal APIs refuse publication-aware files
without writing; an explicit emitting protocol will be needed to mutate them.

Current wire versions refuse both reads and mutations of publication-aware files,
including a file truncated to empty. Three additional store tests verify read,
identity, synchronous and deferred mutation refusal without changing chunk bytes.
An explicit new wire protocol must be introduced before these files are served.

Local raw chunk byte reads retain their existing contract. Future object readers must
use the durable published boundary when selecting visible bytes. The gateway,
metadata switch recovery, larger configured request bodies and overwrite/delete
fencing still require integration before changing the default object path.

## Tests

Twenty-one focused tests exercise exact header fields, old encodings, incomplete and
complete groups, every torn final-record boundary, damaged publication checksums,
invalid recomputed boundaries, epoch changes, mutation refusal, seal and truncate
rules, partial durable prefixes, indexed-header corruption, cold sealed reads,
version-marker corruption, portable fixed record bytes, recomputed indexed
publication boundaries and the existing extent-memory bound.
