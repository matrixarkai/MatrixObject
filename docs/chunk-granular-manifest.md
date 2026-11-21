# Chunk-granular manifests and log objects

A page-mode object records one extent per written page range, publishes a new
manifest generation for every append, and refuses an append once the manifest
holds 4,096 extents. That is the right shape for objects that are replaced or
appended to occasionally. It is the wrong shape for an object that is only ever
appended to, such as a write-ahead log segment: each append costs a metadata
publication, the manifest grows with the number of appends, and the cap is
reached within minutes at a high append rate.

A **log object** is the other shape. Its bytes live in a chain of **log chunks** of
up to 64 MiB. The metadata plane records one entry per chunk, not one per page
range, and an append goes straight to the three replicas of the open chunk
without any metadata command. The metadata plane is touched only when a writer
takes the object, at each chunk boundary, and when a lease is renewed.

This page describes the model, the formats, the replica verb, the replica client,
the metadata commands, the log object writer and reader, the feature gate and the
operator procedure. It also states which parts are in the main tree today and what
has not been measured or tested.

## What is in the tree and what is not

| Part | State |
|---|---|
| Log chunk file format, integrity checks, recovery | In the tree |
| Writer lease table and the tail-length rule | In the tree |
| Format 3 manifests: readers | In the tree. Ordinary publication still refuses to write one |
| Cluster feature set; declare, raise and read commands | In the tree |
| Replica verb `LogOp` (code 28) and the per-node chunk store | In the tree |
| Group commit for log chunk writes, with the sync outside the replica lock | In the tree |
| Metadata readers: outcome version 15, snapshot version 22, and the writer lease table restore in the proposal draft | In the tree |
| Sealed chunk reads from disk through a bounded cache, with a handle limit and an open tail budget | In the tree |
| Leader commands (grant, switch, renew, sweep), batched renewal (outcome version 16) and the feature gate wiring (feature bit 4) | In the tree, behind the two cluster features |
| Replica client and quorum log writer and reader | In the tree. A reader repeats a failed read-only call once on a fresh connection when a connection it kept was closed by the node |
| Log object API (open, append, read, take over) | In the tree, behind the two cluster features |
| Reclaiming the log chunk files of a deleted log object | Not implemented |
| Repair of a log chunk left under-replicated | Not implemented |
| End-to-end harness: ignored tests and scripts that grow a log object past 4 GiB on a private cluster | In the tree. Not run by CI |
| End-to-end latency and metadata cost at multi-gigabyte object sizes | Measured on a private cluster, single runs, one writer, 64 KiB appends: see [What the end-to-end runs measured](#what-the-end-to-end-runs-measured) |

With neither cluster feature raised, a grant or a switch is refused ("chunk-granular
manifests are not enabled for this cluster"), so no lease exists and no format 3
manifest or version 15 or 16 outcome is emitted. A renew reports every lease it
names as lost and a sweep drops none, and neither commits an entry. Nothing raises
the features by default. Raising them is an explicit decision of the owner of a
cluster and cannot be undone; see the irreversibility statement and the operator
procedure, which is written for private test clusters only.

## Which objects use log chunks

Log chunks are for append-heavy log objects: a few hundred objects per cluster,
each growing to gigabytes, written by one writer at a time. They are not for a
corpus of many small objects. A hundred thousand objects of 4 KiB each must not
become a hundred thousand open tail chunks and files on every chunk server, so
they stay in page mode.

The choice is made when the object is created and is recorded in its manifest:
the first grant of the log object writer creates a new object as a log object.
A reader never guesses: it follows the kind of each extent it finds. An append to
a page-mode object that grows large stays in page mode. There is no automatic
conversion. When a page-mode object reaches the 4,096-extent limit its append is
refused, and the caller's remedy is to create a log object instead.

## The model

**Chunks.** A chunk is one file on each of three replicas, written up to 64 MiB.
An object of `size` bytes holds `K = ceil(size / 64 MiB)` chunk entries, at most
4,096. That is about 16 entries per GiB, against one entry per written page
range in page mode.

**Manifest format 3.** A format 3 manifest may hold extents of kind 1 (log chunk)
after any page extents. Each log chunk extent covers a whole chunk. A sealed
entry carries its final length, a CRC-32C over the sealed bytes and, if enabled,
a SHA-256 of them. The last entry may instead be marked as the tail: still being
written, its recorded length only a lower bound. Only the last entry may be the
tail. Readers older than this format reject the new fields.

**Record format.** A chunk file is a sequence of records, each a 40-byte header
followed by its data. The header holds the magic `MOL1`, the record kind (append,
seal or truncate), the chunk generation, the writer epoch, the offset, the data
length, a CRC-32C of the data and a CRC-32C of the header. A record carries at
most 4 MiB. No length is stored anywhere else: the length of a chunk is derived
when its file is replayed, so a write costs one append and one sync.

**Integrity and recovery.** Recovery keeps every complete valid record. A final
incomplete header, or a valid header whose data is incomplete, is removed. A
complete record with a damaged header or data is corruption and is refused
without changing the file, because it may already have been acknowledged. After a
write or sync error a chunk handle refuses every later change until it is
reopened and recovered.

**Writer lease and epoch.** One writer holds an object at a time. The metadata
plane grants a lease whose epoch is the raft index of the granting entry, so
epochs only go up. A replica refuses any write whose epoch is below the highest
it has seen for that chunk. That moves the single-writer and offset checks onto
the chunk owner and off the metadata plane.

**Tail length at open.** Recovery keeps the earlier two-copy rule so previously
acknowledged data is not shortened to an older third replica. With three answers,
the recovered length is the second longest durable length; with two answers it is
the shorter of the two. One answer is insufficient. Nodes holding a longer tail
are truncated at the new epoch. New appends and seals require all three durable
replicas; an under-replicated recovered prefix needs repair before they succeed.

**Chunk identity.** A new tail chunk is named after the raft index of the entry
that opens it, with the same function a page allocation uses to name its chunk.
An entry is an allocation or a grant, never both, so a chunk id is the index of
exactly one entry. It is unique, never reused after a lease ends or a chunk
seals, and never equal to a page chunk id. The cost is that the next chunk id is
not known until the switch commits. That affects writer latency, not correctness.

## The replica verb

`LogOp` is one replica request verb, code 28, with an operation field: append,
seal, truncate, tail and read. One verb keeps the wire surface small. A node that
does not know it answers with the unknown-verb status for the whole family, and
that answer is the capability probe a client uses before it relies on log chunks.
A tail or a read of a chunk the node never held answers as an empty chunk and
creates nothing, so the probe leaves no trace on a node that speaks the verb.

Requests are strict: the version, the operation, the epoch (zero is refused for
writes), the data size (at most 4 MiB) and the identity are all validated before a
request reaches a chunk. A node without durable storage refuses log chunk
operations. In maintenance mode a node refuses appends, seals and truncates and
still answers tails and reads. A node keeps at most 256 sealed chunk files open
and at most 64 open tails (both configurable); see the memory model below.

Appends may arrive in any order within a bounded window (16 records or 16 MiB
waiting for an earlier one). The reply reports the contiguous length applied and
the length made durable, so a client acknowledges by contiguous length and
retries a lost offset.

## Where the sync happens

A log chunk write joins the replica group commit. Under the replica lock the
mutation is applied and written to the chunk file without a sync, and the chunk is
marked dirty. After the group's journal sync, the committer takes one cloned file
handle per dirty chunk, releases the replica lock, syncs the handles, and takes
the lock again only briefly to record the new durable lengths, or to mark the
node failed if a sync failed. Only then does any caller get an answer. A group
therefore costs one sync per dirty chunk however many records it holds, and a slow
sync does not block the heartbeat that reads the replica.

A read never returns a byte above the chunk's durable length, so a byte that is
applied but not yet covered by a sync is invisible. A failed sync fails the node
and no acknowledgement leaves it. A seal still syncs under the lock, once per
64 MiB chunk. A caller that bypasses the group commit syncs under the lock on
every call, so log appends must be sent through it.

## The replica client

A log chunk lives on three nodes. The client side has a writer, which appends to
the three nodes of one chunk, and a reader.

**Writing.** A writer holding the chunk's epoch sends every record to all three
nodes. A record is acknowledged only after all three report it durable (`N=3, W=3`).
Each node reports where the bytes landed (its applied and durable lengths), so the
writer never guesses: a node that lost an offset, answered late or came back after
a restart is brought up from the bytes the writer still holds, starting at the
length that node reports. The writer sends records of at most 1 MiB and retains
pending bytes until all three replicas report them durable. The 64 MiB chunk limit
bounds that buffer. A replica behind the recovered prefix needs repair; its absence
cannot release pending bytes or complete a write.

**The quorum-durable length.** It is the second largest durable length among the
three nodes. Recovery and reads retain this rule for data produced by older
two-copy clients; changing it to the shortest replica could discard previously
acknowledged data. New writes wait for the shortest of all three durable lengths.
A byte held by one node only is outside the recovery prefix and may be discarded.
If a replica is behind that recovered prefix, new writes fail until it is repaired;
opening the writer preserves the existing data rather than shortening it.

**Sealing.** A seal is final, so a node is sealed only once it holds every byte the
writer has. The writer first waits for the nodes that lag to be given the bytes
they lack, bounded by the connection's read timeout, and then seals only the nodes
that hold exactly the chunk's length. A node whose appends are still in flight is
not sealed at the shorter length it happens to hold. The footer (length, CRC-32C
and, when asked for, SHA-256) is returned only when all three nodes sealed and
all of them report the same footer; otherwise the seal fails with an error.

- A node that answers a seal with a length other than the one it was asked to hold
  has sealed something wrong, and that cannot be undone. That is an error: no
  footer is returned, the writer is finished, and the chunk must not be recorded as
  sealed.
- A node that is down, or that does not catch up within the read timeout, is not
  sealed. It is left open, the seal reports an error, and no metadata switch is
  published. A reader can still use the nodes holding a recoverable prefix. Repair of log chunks is a separate path and is not
  implemented (see the limits below).

**Sealed reads.** A sealed extent already carries its final length, CRC-32C and
optional SHA-256 in committed metadata. The log-object reader sends one read per
piece to a replica, validates that its immutable footer matches those values, and
reuses that replica for later pieces. A refused, incomplete or mismatched response
tries another replica. This path does not query the three tails, and remains usable
with one matching live replica. New readers distribute their initial choice among
replicas and keep a successful peer for connection reuse. Connections are pooled
per group across chunks. A cached manifest is validated once and ranged reads
locate their first extent by binary search, then visit only the requested extents.
Open-tail reads still resolve their safe length.

**Reading an open tail.** The reader asks each node for its durable length and its writer epoch.
A node whose epoch is below the highest any node reports takes no part in the
quorum length and serves no read: it missed a change of writer, so what it holds
past the point the new writer started from may be bytes that were never
acknowledged and that the new writer's bytes replaced elsewhere. With fewer than
two nodes at the highest epoch the read fails. A read ends at the quorum-durable
length. It is served in pieces of at most 1 MiB, and a piece is accepted only when
a node returns exactly the bytes asked for; if a node returns fewer, the reader
tries the next, and if no node can serve the piece the read fails with an error
instead of returning less. When every node answers the unknown-verb status, the
reader reports that the nodes do not speak log chunks.

## The metadata commands

The leader commands, the outcomes they produce (version 15 for grant and switch,
version 16 for renew and sweep), the snapshot that holds the writer lease table
(version 22) and the restore of that table when the leader discards a draft are in
the tree. All of them are behind the feature gate described below: a grant or a
switch is refused unless both features are raised.

- **Grant** takes the writer lease of an object and, if no chunk is open, adds one
  empty tail chunk to its manifest, in one commit. A holder that asks again while
  its lease is live and a chunk is open changes nothing and costs no entry. A grant
  is refused, with a precondition failure "the object is not a log object", when
  the object holds any extent that is not a log chunk, so a page-mode object stays
  a page-mode object. A new object, an existing log object and an object that holds
  no extent are not affected. A grant for a deleted key builds on the key's
  tombstone: the generation goes on from the tombstone's, none of the old object's
  chunks survive, and the new lease has a later epoch, so the old writer is fenced.
- **Switch** seals the open chunk at its final length and footer and opens the
  next one, in one commit, by the holder of the live lease. It also renews the
  lease: the expiry becomes the later of the old expiry and now plus the term,
  and the holder and epoch stay. A switch is refused in the same way for an object
  that holds an extent that is not a log chunk.
- **Renew** extends the leases a writer holds on up to 1,024 objects in one
  commit (outcome version 16). Epochs stay and expiry only moves later. The
  intended policy is a term of 30 to 60 seconds, renewed at half of it. A term is
  between one second and one hour, and a switch that names no term renews for 30
  seconds.
- **Sweep** drops up to 256 expired leases in one commit (version 16), proposed by
  the leader's cleanup loop. Names that are no longer expired are filtered out
  when the decision is made.
- **Read** returns the lease and the open chunk of one object from committed state.

Grant and switch are outcome version 15. Snapshot version 22 holds the writer
lease table and is written only while at least one lease exists. The commands use
a new metadata request verb (code 97).

**Entry cap.** An object holds at most 4,096 entries. A grant or a switch that
would add a 4,097th is refused and the writer may still fill the open chunk.

**Cost.** A writer pays one grant when it opens an object, one switch per chunk
boundary, and one renewal per half term for all the objects it holds. An append
between those costs no metadata entry. A test asserts these counts (see the log
object section below); a measurement at multi-gigabyte sizes is pending (see the
limits).

## The log object

A log object is written through `LogObject`, by one leaseholder, and read through
`LogObjectReader`. Both sit on the replica client and the metadata commands above,
and everything here is behind the two cluster features: with them not raised, the
metadata plane refuses the grant and a writer cannot open an object.

**Options.** A writer is named by its holder name and the chunk group that holds the
object's chunks. The defaults are a chunk of 64 MiB, a lease term of 30 seconds,
no SHA-256 in the seal footer, and at most 4,096 entries per object. A chunk size
up to the 64 MiB limit and the other values can be set.

**Opening for append.** `open_for_append`:

1. asks the metadata plane for the nodes of the chunk group, and refuses a group
   that does not have three;
2. probes each of the three nodes with a tail request of a chunk it never held. A
   node that answers unknown-verb stops the writer with a "does not speak log
   chunks" error before any grant is sent, because a grant is a metadata commit and
   cannot be taken back. A node that cannot be reached is skipped, and fewer than
   three reachable nodes is an error;
3. sends one grant, which takes the lease and, if the object is new, creates it as a
   log object, and opens the chunk being written at the lease's epoch;
4. returns the object's length, which is where the next byte goes. After a writer
   died, the tail is decided from what the nodes hold.

**Appending.** An append is cut at chunk boundaries and goes to the three nodes of
the open chunk. No metadata command is sent for it. It returns the object length
once all three nodes hold every byte durably; if any required replica is missing,
the bytes are kept and `settle` retries them. An empty append also settles pending
bytes instead of returning a shorter durable length. `settle` and empty appends
refuse to resend bytes after the handle's lease expires. A handle stops writing, with a lost-lease error,
when its own view of the lease has run out. That view is measured from the moment
the request that granted or renewed the lease was sent, so a writer never believes
in a lease longer than the metadata plane does.

**The chunk boundary.** The object moves to its next chunk when the next byte needs
room. The entry cap is checked first: an object at its cap keeps its open chunk,
which can still be filled, and refuses the append that does not fit. Then the chunk
is sealed on the nodes, and only if the seal returned a footer, ONE switch carrying
the sealed length and the footer is sent to the metadata plane. The switch also
renews the lease. The next chunk must start where the sealed chunk ends, or the
writer reports corruption.

**When a seal does not complete.** The metadata switch is never sent without a
footer.

- If the seal reached no node, the chunk is open everywhere, no switch is sent, and
  the handle is kept open.
- If it reached a node but returned no footer, or fails for another reason (a node
  sealed at another length, a newer epoch), the handle is closed and the writer must
  open the object again.
- If the switch fails after the chunk was sealed, the handle is closed, and the next
  open finishes the switch.

**Recovering an interrupted switch.** A writer can die after it sealed the chunk and
before the switch reached the metadata plane, so that the manifest still names the
sealed chunk as the open one. The next writer, when it opens, finds the chunk sealed
and reads the tail of every reachable node. It completes the seal at its own
epoch on every unsealed node holding exactly the sealed length. Only three
matching sealed footers permit the metadata switch. A node at another length is
left for repair; mismatched footers or fewer than three sealed replicas are errors.

**Renewal.** An append that finds the lease past half its term renews it with one
command. A writer that holds many objects renews all their leases in one command.
A handle whose lease the metadata plane no longer holds for this writer is closed.

**Errors.** The writer reports typed errors: a node does not speak log chunks, the
object is not open for append, the object is leased to another writer, the writer's
lease was lost, the object holds the most entries it may, a chunk error (including
no quorum), a metadata error, and an invalid request.

**Reading.** `LogObjectReader` follows the object's manifest. It reads only objects
whose extents are all log chunks, and refuses any other with "the object is not a log
object". A sealed chunk is read from its nodes up to its recorded length, and a
short answer for a sealed chunk is an error, never partial data. The open chunk, the
live tail, is read only up to the length two of its nodes hold durably. A read is
therefore cut short at the end of the object: the end of the last sealed chunk plus
the durable length of the open chunk. When the open chunk answers short, the reader
reads the manifest once more, because the object may have moved to a later chunk.
The length of an object is computed the same way.

**What the test asserts about metadata cost.** One test runs three real metadata
voter processes and three real chunk nodes and reads the metadata group's commit
index before and after each step. It asserts that:

- with the features not raised, opening the object fails and the commit index does
  not move;
- after the features are declared and raised, a grant costs one entry;
- an append inside a chunk costs none, and an append that crosses N chunk boundaries
  costs N entries, plus one renewal if the lease was past half its term;
- renewing one lease costs one entry, and renewing the leases of three objects in one
  command costs one entry;
- reads cost none;
- an append is unaffected by a follower being down, and after every voter is
  restarted a second writer takes the object over once the first writer's lease has
  run out, starts at the length the nodes acknowledged, loses no acknowledged byte,
  and the first writer is then refused.

These are what the test asserts. There is no end-to-end latency or object-size
measurement yet; it is pending.

## The feature gate

Two cluster features gate everything that writes a log chunk manifest:

- **Chunk manifest** (bit 2): manifests whose extents name log chunks.
- **Log chunk leases** (bit 4): outcome versions 15 and 16 and the lease commands.

Each metadata voter declares the feature bits its build can read. The group raises
a bit only through an explicit raise command, and the leader accepts it only when
every voter the caller names has declared every bit being raised. A raised bit is
never lowered. An entry that is already committed is always applied: a node that
cannot read it fails loudly instead of skipping it. The leader checks both bits
for a grant and for a switch and refuses with "chunk-granular manifests are not
enabled for this cluster" unless both are raised; one is not enough. A read
answers from committed state. Of the bits in use, bit 2 (chunk manifest) and bit 4
(log chunk leases) belong to this feature, and bit 3 is the read grace feature of
the gateway side. Bits 8 to 15 are held for other records.

The leader does not know what each chunk server supports. A writer must probe the
replica verb on the three nodes before it asks for a grant, so that a node that
does not speak log chunks stops it before any metadata command is sent. The log
object writer that does this is written and is not in the tree yet.

### Irreversibility

**With neither bit raised, the running system writes nothing in the new format: no version 15 or 16 outcome, no snapshot version 22 and no format 3 manifest is produced, and nothing already written is changed. The code that reads and applies those records is merged and inert until a raise. Nothing raises the bits by default; only the explicit raise command does, and a grant or a switch needs BOTH the chunk manifest bit (bit 2) and the log chunk leases bit (bit 4). The raise command is accepted only for bits that every voter the caller names has declared; it does not check the gateways, the clients or the chunk servers, so before a raise every metadata voter, gateway and client that will read these objects must run a build that reads them, and the chunk servers must answer the log chunk verb. RAISING BOTH BITS IS THE IRREVERSIBLE STEP, and a raise cannot be rolled back. Once both bits are raised and the first log chunk grant commits: (a) the metadata Raft log and its snapshots contain outcome versions 15 and 16, snapshot version 22 and objects with manifest format 3 and log chunk extents; a build that lacks these readers FAILS to apply those entries (strict canonical re-encode, no skip), so no metadata voter can be run on an older build afterwards and an older gateway or client cannot read those objects; (b) a raised feature bit is never lowered: no command un-raises it; (c) an object written as log chunks stays chunked: going back to page extents means rewriting the object. Raising the features on a cluster that is not a private test cluster is an explicit decision of that cluster's owner or operator.**

## Operator procedure

**Warning. Raising the two features is not a routine step, and it is not
recommended on any cluster that is not a private test cluster.** It is
irreversible: there is no downgrade, no command lowers a raised bit, and a metadata
voter on an older build cannot be run afterwards. On any cluster other than a
private test cluster that you can afford to rebuild, raising them is an explicit
decision of that cluster's owner or operator, taken with the consequences in the
irreversibility statement above in view. The steps below are the procedure for a
private test cluster.

1. Run a build that reads format 3 manifests, outcome versions 15 and 16 and
   snapshot version 22 on every metadata voter, every gateway and every client
   that will read the objects. A voter on an older build cannot apply the first
   grant.
2. Run chunk servers that answer the log chunk verb. Probe each node with a tail
   request of a chunk it never held: the unknown-verb status means the node does
   not speak log chunks.
3. Have each metadata voter declare the bits it reads, with the declare call of
   the metadata client (`declare_voter_features`), and read the set back with
   `read_cluster_features`. Check that every voter appears in `declared` with both
   bits before going further.
4. Raise the bits with `raise_cluster_features`, naming the whole voter set in
   ascending order. The leader refuses the raise if any named voter has not
   declared every bit. Read the set back and check that `raised` holds both bits.
5. Open each new object that will be a log object for append with the log object
   writer: its first grant creates it as a log object. Page-mode objects are
   unaffected.

To check at any time: `read_cluster_features` returns the raised bits and every
voter's declaration; a sealed chunk entry in a manifest carries its footer CRC.
There is no command to lower a bit or to convert a log object back to pages.

## Memory model

A chunk server keeps memory flat in the size of what it stores. Chunks that can
still change and chunks that are sealed are treated differently:

- **Open tails are resident.** A chunk that can still change keeps all its bytes
  in memory, up to 64 MiB, and answers reads from memory. A node holds at most 64
  open tails by default. Past that budget, opening one more tail is refused with a
  retryable error ("too many log chunk tails are open on this replica; retry"). A
  tail is never closed to make room, and it frees its slot when it is sealed.
- **Sealed chunks are read from their file.** A sealed chunk keeps no payload in
  memory. Opening one verifies its checksum chain by reading the file in 256 KiB
  pieces, so the open needs no buffer the size of the chunk. A read takes whole
  records from the file, checks each record against the checksum written with it,
  and keeps the record in a cache shared by every chunk on the node and bounded in
  bytes (64 MiB by default). A sealed file never changes, so a cache entry never
  goes stale and dropping one only costs reading and checking the record again.
- **Sealed chunk files are limited apart from tails.** A node keeps at most 256
  sealed chunk files open. Past that, the least recently used sealed chunk is
  closed and reopened when something reads it again, and nothing is refused. A
  chunk with a sync still to come is never closed.

The open tails and the record cache together bound the heap. The three limits
(sealed files open, open tails, cache bytes) are set when the store is opened.
Appends and the replica group commit behave as described above.

Measured by tests of the sealed read change: the peak heap while opening a sealed
64 MiB chunk fell from 71,303,204 bytes to 524,924 bytes, and reading all 64 MiB
through an 8 MiB cache peaked at 13,632,156 bytes.

## What the end-to-end runs measured

The ignored tests in `tests/node_regime/log_object_end_to_end.rs`, run by
`scripts/log_object_e2e/run.sh`, grow a log object on a private cluster of three
metadata voters and three chunk servers on one machine. Both cluster features are
raised on that cluster only, and the metadata servers run with the default Raft
checkpoint interval. One writer appends 64 KiB at a time and waits for each append
to be acknowledged. The object is grown to 4,362,076,160 bytes (65 chunks of 64 MiB),
a voter is killed and restarted at 1.62 GiB and a chunk server at 2.84 GiB, the whole
object is read back and compared with the bytes that were appended, and a second
writer takes the object over.

The measurements below used the earlier two-copy completion policy (`N=3, W=2`).
They do not measure the current three-copy client. Current three-copy throughput,
latency and recovery measurements must be collected separately; a stopped member
blocks success until it returns or is repaired.

These are single runs (four log object runs in all, across fixes of the harness and of
the reader), on a machine that was shared with other builds: the load average reached
10 to 16 and 2 to 7 GB of swap were in use for part of the time. They show the
behaviour of that earlier client at this size. They are not a benchmark, and the tails in
particular are not clean.

| Measured | Result |
|---|---|
| Append latency per 256 MiB window | Median 4.5 to 11.1 ms and 99th percentile 13.5 to 116 ms in every window of every run. No trend with object size from 0.25 GiB to 4.06 GiB. The slowest single append in a window ranged from 0.08 s to 6.5 s |
| Metadata commit entries per 256 MiB window | 4 in almost every window: the four chunk switches. A window also holds the extra entries of a grant (at the start, and again when a writer reopens the object) or of batched renewals that landed in it, up to three renewals in one window. None are written per append: 0.001 entries per append |
| Manifest size | 144 bytes at 4 chunks, 2,280 bytes at 65 chunks (about 35 bytes per chunk) |
| Metadata write-ahead log, summed over the three voters | 7.8 KB after the first 256 MiB, about 514 KB at 4.06 GiB. It did not reach a checkpoint |
| Voter killed at 1.62 GiB | The append in flight failed, the writer reopened the object at the same offset with no extra bytes, recovery 2.9 to 4.4 s; the voter was back in 3.1 to 4.8 s |
| Chunk server killed at 2.84 GiB | Down 1.0 to 4.8 s; the 201 appends made while it was down were acknowledged by the other two replicas, the slowest taking 20 to 288 ms |
| Sealed range read of 16 MiB | 0.67 to 1.6 s cold, 0.07 to 0.32 s warm, bytes correct |
| Read back and compare the whole 4.06 GiB object | 0 mismatches and no acknowledged byte lost, in 72 to 194 s |
| Tail rule | The second writer opened the object at exactly the acknowledged length (4,362,076,160 bytes, equal to what the nodes durably held); the first writer was fenced; takeover 0.6 to 0.84 s |
| Chunk server resident memory | 72 to 81 MB during growth; up to 185 MB on the node that served the whole-object read |
| Metadata voter resident memory | 6 to 8 MB per voter at 4 GiB, read in the first run only |
| Processes at the end | All three voters and all three chunk servers were still running |

The log object is not faster in single-writer bytes per second. Each append here is
64 KiB and is acknowledged by two of three replicas before the next is sent; the runs
moved 4.06 GiB in 563 to 753 seconds, about 5.5 to 7.4 MiB per second (a run that
was paused for seven minutes by the machine's other load is left out). What the runs
show is that the cost of an append does not depend on the size of the object and that
the metadata plane is touched once per chunk, not once per append.

**A defect the runs found, and its fix.** The first read of the open chunk during the
whole-object verification failed with no quorum, although all three nodes were up, in
the two runs that reported it (chunk index 64, position 4,294,967,296) and succeeded on
a retry. A log object reader keeps a connection to each node between calls; a node
closes a connection that has sent nothing for its read timeout (30 seconds), and the
reader treated the first failed request on such a connection as a node that had not
answered. The verification took 72 to 144 seconds to reach that chunk. The reader now
repeats a failed tail query or read once on a fresh connection when the connection that
failed had been used before (only read-only calls: a mutation whose outcome is unknown
is never sent twice). A run with the change shows no failed read.

**Not shown.** One writer only; appends of 64 KiB only; one run of each kind, so no
variance; no reader running while the object grows; one voter or one chunk server down
at a time; no gateway or S3 path writes a log object yet, so nothing here exercises
one; and no run on a cluster that carries other traffic.

## Limits and what is not measured

- The metadata cost per GiB appended is about 16 entries for a log object (one per
  64 MiB chunk) and one entry per page range in page mode. The runs above measured 16
  per GiB for the log object at 64 KiB appends.
- Append latency against object size was measured up to 4.06 GiB at one append size,
  with one writer using the earlier two-copy completion rule (see above). The cost of a ranged read was measured for one 16 MiB
  range and for reading the whole object once per run, and a restart and recovery
  at that size once per kind of node per run. Other append
  sizes, several writers, readers during growth and objects beyond 4.06 GiB are not
  measured.
- Chunk server memory against corpus size was measured only for the one object above;
  metadata server memory per GiB appended only as the 6 to 8 MB per voter at 4 GiB.
- A node refuses to open a new tail past its tail budget, and the metadata plane
  refuses a 4,097th entry. Both are errors the caller must handle.
- New log writes require all three replicas. A missing replica prevents success,
  and an older two-copy prefix remains readable while its third copy needs repair.
- A chunk can be left unsealed on a lagging node: a node that is down or does not
  catch up keeps the seal incomplete, and there is no automatic repair for log chunks yet. When a writer completes an interrupted
  switch it does not touch a node at any other length.
- Not tested at the log object level: a writer that keeps appending on the same
  handle after a seal that reached no node (the tests show the handle is kept open
  and that a new writer then continues); and a node that seals a chunk at another
  length (the writer treats it as an error that closes the handle, and no test
  exercises it).
- The planted-defect runs for the log object (deliberately breaking the code to
  check that its tests fail) and the instruction-count cost probes have not been
  run.
- Nothing reclaims the log chunk files of a deleted log object: this is not
  implemented. A search of the source found no code in object deletion, retention
  or reclaim that refers to log chunks, no removal of a log chunk file in the chunk
  store, and no replica operation that drops a chunk (the verb has append, seal,
  truncate, tail and read). The log chunk files stay on the chunk servers until an
  operator removes them.
- The ordinary object read path refuses an object that holds log chunk extents
  ("object holds log-chunk extents this reader cannot read"). A log object is read
  through the log object reader.
- The guard that keeps a page-mode object a page-mode object is in the leader
  commands only. The manifest rules still accept a manifest that holds page extents
  followed by log chunks (page extents first, then log chunks, and only the last
  extent unsealed), and the validators of the version 15 outcome check the kind
  only of the extents they change. Whether to gate the validators as well is an
  owner decision.
- A delete does not clear the writer lease of the deleted object: a search of the
  source found no delete path that touches the lease table. A grant by another
  holder is refused while that lease is live.
- A grant for an object that holds no extent is not refused, because such an object
  has not chosen a kind. An empty format 2 object becoming a log object is not
  tested; the tests cover a new object, an existing log object, a refused page
  object and a deleted key.
- Format 3 objects cannot be read by older builds, and a raised feature cannot be
  lowered. See the irreversibility statement above.
- **Trust assumption: cluster clients are trusted for page addressing.** A chunk manifest names the
  pages it holds by address (group, chunk, page range) and a publication is checked for its generation
  and its own validity, not for whether the publisher wrote those pages. A client that can publish can
  therefore name pages that belong to another object (an alias). The digest check on a read is
  **integrity, not access control**: it proves that the bytes are the ones the manifest committed, not
  that the publisher was entitled to name them. Publishing is not authorization. What an alias cannot
  do is make the reclaim sweep free a live page: a page any manifest names stays live. Deployments that
  do not trust every cluster client must put that boundary in front of the metadata plane.

## Appending to a paged object through the gateway

A paged object is appended to by writing the new bytes as new extents after the last one (data pages, then
the digest page of a multi-page extent) and publishing the manifest one generation later, with the
generation check: an append that read a stale manifest is refused and changes nothing. The gateway answers
the append with the offset it landed at, and settles an append whose outcome is not known with the
extents the attempt published as its identity, as it does for page-granular objects.

Known limits: an append adds at least one extent, and a manifest names at most 4,096, so an object that has
reached the bound refuses the append whole and is rewritten; a log chunk writer (several appends in one
chunk, no extent per append) is not part of this path.
