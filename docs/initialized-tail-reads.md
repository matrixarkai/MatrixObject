# Initialized active-tail reads

`ChunkLogReader::stabilize_publication` selects the largest complete prefix among
qualified members, rather than reducing it to the median length. When only one
member holds that prefix, the client copies its original identified records to a
second qualified member and verifies durable publication before returning.
`read_published` then reads one payload copy, bounded by the captured prefix.
The ordinary write acknowledgment policy remains all three replicas.

The caller must pin an authoritative metadata route and writer epoch throughout
the operation. The supplied initialization identities bind each ready storage
incarnation to its configured endpoint. A missing, uninitialized, old-epoch or
wrong-incarnation member cannot vote as an empty member. Replacement storage must
remain unqualified until caught up. The validation callback checks an existing
pin; it does not acquire one or authorize creation. These methods must not be
used with arbitrary endpoint discovery or an unpinned object route.

Readers never initialize, fence or truncate storage. Same-boundary conflicting
identities and conflicting shorter complete tips stop the read. A repair replays
exact original records, including matching partial records, so the destination
checks both identity and bytes on retries. Repair requires explicitly enabled
publication writes. Unknown mutation transport outcomes do not count as durable
copies. An invalidated route check aborts without trying to turn that check into
a later success.

Healthy reads exchange bounded prefix observations and fetch one payload copy.
Repair currently replays the selected chunk from offset zero, with one bounded
record in memory, to validate existing records as well as append missing ones.
Repair therefore costs bytes proportional to that chunk; it does not establish
flat end-to-end object append latency. Probes currently visit members serially.

The default object backend is unchanged. Metadata pin acquisition, allocation,
replacement admission and atomic object publication must be integrated before
these storage operations can serve native object APIs. The separate-process
probe uses synthetic allocation and a fixed route; it validates storage repair,
source loss and restart, not production metadata integration.
