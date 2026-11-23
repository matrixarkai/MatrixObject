# Guarded publication wire operations

LOG wire version 4 separates initialization-aware observations from the older
read-only version 3 capability. A node must explicitly opt into publication writes
through `ReplicaNodeOptions::publication_writes`; the default is false. No
production CLI or default object backend enables it. In-memory replicas cannot
opt in. Writable nodes require at least 512 payload bytes and advertise the
actual frame limit so a writer can choose stable record sizes across members.
Maintenance nodes do not advertise writable capability.

## Operations and identity

Version4 adds explicit initialization (operation 7) and preserving fence
(operation 8), plus identified published append and seal operations. It rejects
the old client-selected truncate operation. Initialization is only for an
authoritatively granted fresh allocation. It is not missing-member recovery.

Append, fence, seal and bound read/identity requests carry the expected 16-byte
initialization identity. The server checks it before proceeding. Only a preserving
fence can advance an existing member's epoch. Append and seal require the current
fenced epoch. Missing members are not implicitly created by these operations or
bound reads. They require verified history transfer under the upper protocol.

The fence retains the member's own complete local prefix and drops only its
incomplete suffix. The caller cannot supply a shorter length. Initialization and
seal sync internally; append/fence use ordinary synchronous or deferred group
completion. Deferred mutation replies capture the applied publication boundary,
and may be delivered only after successful group sync. Queries continue reporting
the actual durable prefix while that sync is outstanding.

## Protobuf observations

Request fields 13..15 carry the published length and expected initialization
identity. Reply fields 20..25 carry initialization identity/presence, explicit
protocol 4 support, whether writes were opted in, and the frame payload limit.
Old request versions omit these new fields in replies. Both canonical decoders check protocol, presence,
identity and boundary shapes; owned decoding retains its payload allocation.

`has_publication_write_protocol` establishes supported enabled wire behavior,
not creation permission, ready membership or a metadata lease. A bound prefix
observation additionally checks the expected initialization identity. Read-only
version 3 support alone must never authorize emission of initialization controls.

## Integration requirements

The option is a low-level integration guard. Before production use, implement
reader-first metadata activation/floors, authoritative staging/allocation,
qualified member catch-up, preserving lease handoff and verified quorum write-back.
Same-end conflicting histories and stale metadata routes must be resolved safely.
All-three write acknowledgements remain the integration policy; this change does
not reduce durability requirements or enable a new object default.

Twenty-one focused tests cover disabled/default guards, canonical request/reply
encodings, identity and epoch checks, missing-member refusal, deferred durability,
legacy separation, maintenance and in-memory modes, restart, actual network
operations, preserving fences on three replica servers and a failed sync. These
are protocol/storage tests, not complete atomic-object or scale qualification.

## Separate-process storage check

Run `cargo run --release --example guarded_publication_process_probe -- <fresh-directory>`
to exercise three child replica processes with different frame limits. The probe
chooses a common record capacity, loses one final record, fences while preserving
complete prefixes, transfers records with their original identities, checks reads
with one replica unavailable and restarts every replica.

This uses synthetic allocation and epoch context. It validates storage protocol
behavior only; it does not exercise metadata authorization, automatic repair,
object publication or performance.
