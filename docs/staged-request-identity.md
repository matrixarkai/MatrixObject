# Recoverable staged allocation requests

Enable the durable publication fence, then call
`prepare_staged_allocation(bucket, key, expected_generation, page_count, append, ttl_ms)`.
Zero TTL creates permanent ownership; a leased request accepts 1,000 through
3,600,000 milliseconds. Preparation performs a quorum-confirmed metadata read
and generates a random 32-byte nonce. It does not reserve pages or its position.

Durably save the returned `PreparedStagedAllocation::to_bytes()` before sending
`allocate_prepared_staged_object(&prepared)`. Restore the exact handle with
`PreparedStagedAllocation::from_bytes` after a client restart. Keep it bound to
the same retained metadata history and pinned metadata catalog.

The handle binds the target, expected generation, append mode, page count,
initial TTL, nonce and the next durable metadata position. Only that position
can create its namespace. Once created, retries recover the matching active
record even after unrelated metadata mutations. Another nonce or altered request
cannot adopt that record. Renewals preserve the original request identity, and
a retry returns the currently renewed record without extending its deadline.
An expired request is refused even before automatic cleanup removes it.

When publication, abort or expiry removes ownership, its position remains
consumed. The same handle cannot allocate again. No permanent retry-receipt
table or additional retention policy is needed. Active ownership remains
bounded to 128 records and shares the existing checkpoint byte budget.

## Contention and uncertainty

Another mutation may consume the prepared position before submission. The
request is then refused; it cannot move to a replacement position automatically.
Preparation adds one metadata read per reservation, and contending preparers
can require another round trip. This tradeoff must be measured under concurrent
allocation load before changing caller defaults.

A transport failure is uncertain. Retry the exact persisted handle. A refusal
after retirement does not establish whether an earlier publication succeeded:
inspect the saved publication candidate and manifest using the normal resume
protocol. Do not create a replacement request solely because ownership is absent.
The legacy allocation APIs remain available; their uncertain replies still
require operator reconciliation.

## Persistence and compatibility

Metadata opcode 93 request version 3 uses action 10 to read the next position,
with a version-2 reply carrying `allocation_cursor` in field 7. Allocation uses
action 1, `allocation_id` and nonce field 14. Version-3 ownership records retain
the nonce in field 9 and immutable initial TTL in field 10. Existing version-1
and version-2 records and requests retain their encoding.

The existing binary protobuf allocation/removal and renewal outcomes carry these
records in journals and Raft. Fenced catalog checkpoints preserve the identity.
Upgrade all metadata voters, clients and snapshot consumers before using these
records; older consumers reject version 3. Restore the complete placement state
and metadata state together, including the monotonic placement index.

Local WSL tests cover a successful reply dropped in transit, competing nonces,
altered request fields, renewal, expiry, publication, abort, speculative batch
rollback, checkpoint transfer to a lagging voter, leader loss, full restart and
quorum loss. This protocol does not authorize data deletion; automatic reader
protection and metadata-driven data fencing remain prerequisites for reclamation.
