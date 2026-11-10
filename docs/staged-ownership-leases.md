# Staged ownership leases

Enable the durable publication fence before using leased allocation. The API
`allocate_leased_staged_object` reserves the same fresh page namespace as ordinary
owned allocation, with an absolute metadata-server deadline. TTLs must be from
1,000 through 3,600,000 milliseconds. The returned record is version 2 and includes
`expires_at_ms`. Existing version-1 reservations remain permanent until publication
or explicit abort.

`renew_staged_allocation(&record, ttl_ms)` compares the exact prior deadline and
returns the new record. It must extend a live deadline; shortening a lease,
renewing an expired lease and renewing against a replaced deadline are refused.
Keep the returned record for subsequent object writes and resume calls. A stale
record does not match renewed ownership during client preflight. A lost renewal
reply can be inspected through `read_staged_allocation`; do not blindly repeat
the old deadline or adopt another reservation.

Publication checks the live deadline during metadata admission. At or after the
deadline, new publication and renewal are refused even if the expired reservation
has not yet been removed. An already prepared publication may commit after its
deadline: consensus order, rather than replay-time wall-clock checks, determines
the decided result.

## Explicit expiry and recovery

`expire_staged_allocation(&record)` removes that exact expired deadline. It is
idempotent once the allocation is absent, refuses future identities and never
removes a renewed deadline. Active deadlines and permanent reservations cannot
be expired through this API. An expired allocation ID is never reused.

Expiry is an explicit controller operation. Expired records remain listed and
count toward the shared 128-reservation capacity until expired or aborted.
Controllers can enumerate at most 32 IDs per page, read their records and submit
expiry against each recorded deadline. Concurrent renewal causes a refusal,
requiring a fresh read. Repeated empty expiry does not advance placement state.
This change does not install an automatic sweeper or allocation request identity.

Deadlines use the admitting metadata server's Unix clock. Deployments must keep
metadata clocks synchronized; this is not a clock-skew-tolerant data-node lease.
Renew well before expiry. No client-provided current time is trusted. Decided
binary protobuf outcomes contain the resulting deadline or removal, and replay
never expires, renews or revalidates a decision using the recovering server's
clock. Speculative proposal preparation restores prior ownership before commit.

## Wire format and scope

Metadata opcode 93 request version 2 uses `ttl_ms` (field 12) and
`expected_expiry_ms` (field 13). Action 1 allocates a lease, action 8 renews and
action 9 expires. Version-1 requests retain their existing encoding. Reservation
record version 2 stores deadline field 8. Outcome version 7 records renewal;
allocation, publication and removal use existing staged transitions with the
version-2 reservation. Upgrade all metadata voters and readers before use:
older implementations reject these records and renewal outcomes.

The durable publication fence prevents expired owned pages from becoming
visible through ordinary unowned publication. It does not stop raw chunk
writes, cancel writes already in flight, authenticate allocation ownership or
authorize reclamation. Data-node fencing, automatic reader protection and
interruption-safe physical deletion remain unfinished. Keep reclamation disabled.

Validation covers exact deadline boundaries, TTL bounds, renewal at full
reservation capacity, stale cleanup, forged renewal identity, snapshot and
journal recovery, batch rollback, metadata leader loss, total restart, quorum
loss, and leased object writes through both Star and Raft data groups.
