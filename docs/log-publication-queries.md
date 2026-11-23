# Read-only publication queries

Version 3 of the LOG operation protocol accepts only tail, read and identity
queries. It enables inspection of publication-aware records without enabling an
emitter, writer lease or new default object backend. Mutation operations at this
version are refused before opening or creating a chunk.

`LogOpRequest::publication_tail` returns the complete durable local prefix and
its final complete record identity. If later physical records remain incomplete,
the returned identity still names the published prefix. A missing chunk reports
explicit protocol support without reserving storage. An old empty response is
not a capability acknowledgement.

`publication_read` returns bytes only below that complete durable prefix, including
an empty response beyond it. `publication_identity` can inspect a complete durable
physical record even when the original request is incomplete. An identity alone
therefore does not establish object publication or attempt settlement.

Reply protobuf fields 14 through 19 carry the published length, publication mode,
the observed record's published length, explicit protocol support, explicit
record publication presence and chunk presence. The presence bit distinguishes a record publishing
zero bytes from a legacy record. Old requests leave all six fields omitted, so
their reply bytes remain unchanged. Borrowed and owned decoders validate ordered
unique fields, minimal encoding, widths and boundary relationships. The owned
decoder continues reusing the existing data buffer.

`LogOpReply::has_publication_observation` validates the extended storage envelope.
`published_prefix` additionally requires a final identified publication record for
any nonempty prefix. Absent or uninitialized legacy members cannot supply an empty
prefix vote. A member with lost durable state must complete catch-up before
participating; capability support alone is not membership readiness. It refuses a nonempty legacy prefix or an incomplete record
that merely carries an earlier boundary. These are local observations, not a
replicated commit certificate. Callers must bind them to the correct object
incarnation, route and chunk generation and establish safe quorum visibility.

Older wire versions continue refusing publication-aware files, even after a
version 3 query or a truncation to empty. Local raw byte APIs retain their existing
contract. No request can silently upgrade legacy data into atomic object history.

Sixteen focused tests cover request and reply encodings, unsupported mutations,
missing chunks, incomplete groups, final record identity, bounded reads, legacy
separation, canonical rejection, owned-buffer reuse, protocol floors and the
replica server wire path. Distributed read repair, fencing, writer recovery,
large configured requests and end-to-end object publication remain integration
work before changing the default object path.
