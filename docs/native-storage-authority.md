# Physical storage authority

Log wire protocol 5 binds every initialized member operation to a persistent
16-byte physical storage incarnation. The storage probe returns that incarnation;
callers must retain the admitted value in their authoritative membership state.
A replacement creates a new value and refuses delayed requests naming the old one.
The native protocol never falls back to protocol 4.

A checksummed binary protobuf registry reserves each namespace before creating its
LOG file. After durable initialization, a second transition marks it ready. An
atomic checksummed anchor commits the exact registry length, sequence and history
digest before acknowledgement. Recovery rejects missing files, truncated committed
history, unknown versions, noncanonical records and checksum damage. An unanchored
suffix cannot authorize a member and is truncated during recovery.

A ready namespace with missing data cannot be initialized as an empty member.
Existing unowned files cannot be adopted. Older wire requests cannot access an
owned native namespace, including after restart. Pending creation can be retried
only with its original allocation, generation, epoch and initialization value.
The registry has one resident entry per chunk; startup replays this index. Steady
appends and reads perform indexed checks and add no registry records or fsyncs.
Retired entries retain their ownership; reclamation and index compaction remain
part of the retirement protocol.

This storage primitive does not grant object ownership or metadata authority.
The metadata membership representation, actual pinned reader, safe replacement
catchup, full handoff and native object RPC routing still need their coupled
implementation. Feature bit 7 and the default object backend remain unchanged.
