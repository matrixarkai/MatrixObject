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

This page describes the model, the formats, the replica verb, the metadata
commands, the feature gate and the operator procedure. It also states which parts
are in the main tree today, which are in review or not yet opened for review, and
what has not been measured.

## What is in the tree and what is not

| Part | State |
|---|---|
| Log chunk file format, integrity checks, recovery | In the tree |
| Writer lease table and the tail-length rule | In the tree |
| Format 3 manifests: readers | In the tree. Ordinary publication still refuses to write one |
| Cluster feature set; declare, raise and read commands | In the tree |
| Replica verb `LogOp` (code 28) and the per-node chunk store | In the tree |
| Group commit for log chunk writes, with the sync outside the replica lock | In the tree |
| Metadata readers: outcome version 15, snapshot version 22, and the writer lease table restore in the proposal draft | In review |
| Leader commands (grant, switch, renew, sweep), batched renewal (outcome version 16) and the feature gate wiring (feature bit 4) | In review (draft), stacked on the metadata readers |
| Replica client and quorum log writer and reader | In review (draft) |
| Sealed chunk reads from disk through a bounded cache | In review (draft) |
| Log object API (open, append, read, take over) | Written, stacked on the leader commands and the replica client; not yet opened for review |
| End-to-end latency and metadata cost at multi-gigabyte object sizes | Not measured yet |

Nothing writes a log chunk manifest unless an operator raises two cluster
features. Nothing raises them by default.

## Which objects use log chunks

Log chunks are for append-heavy log objects: a few hundred objects per cluster,
each growing to gigabytes, written by one writer at a time. They are not for a
corpus of many small objects. A hundred thousand objects of 4 KiB each must not
become a hundred thousand open tail chunks and files on every chunk server, so
they stay in page mode.

The choice is made when the object is created and is recorded in its manifest.
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

**Tail length at open.** A writer that opens a chunk decides where it ends from
what the replicas hold. When the metadata plane recorded a length at a chunk
switch it wins. Otherwise, with strong writes (all three replicas hold every
acknowledged byte), the tail is the shortest replica length. With quorum writes
(two of three), the tail is the second longest length when all three answer, the
longest of two when two answer, and the rule refuses when only one answers.
Replicas that hold more are truncated to the decided length, at the new epoch,
before any append. A sealed replica fixes the chunk at its sealed length.

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
still answers tails and reads. In the current tree a node keeps at most 256 chunks
open and refuses to open another.

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

## The metadata commands

These commands are in review and are not in the main tree yet. They are
behind the feature gate described below.

- **Grant** takes the writer lease of an object and, if no chunk is open, adds one
  empty tail chunk to its manifest, in one commit. A holder that asks again while
  its lease is live and a chunk is open changes nothing and costs no entry.
- **Switch** seals the open chunk at its final length and footer and opens the
  next one, in one commit, by the holder of the live lease. It also renews the
  lease: the expiry becomes the later of the old expiry and now plus the term,
  and the holder and epoch stay.
- **Renew** extends the leases a writer holds on up to 1,024 objects in one
  commit (outcome version 16). Epochs stay and expiry only moves later. The
  intended policy is a term of 30 to 60 seconds, renewed at half of it.
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
between those costs no metadata entry. The count of metadata entries per append
between chunk switches is the figure to verify on a running cluster (see the
limits below).

## The feature gate

Two cluster features gate everything that writes a log chunk manifest:

- **Chunk manifest** (bit 2): manifests whose extents name log chunks.
- **Log chunk leases** (bit 4): outcome versions 15 and 16 and the lease commands.

Each metadata voter declares the feature bits its build can read. The group raises
a bit only through an explicit raise command, and the leader accepts it only when
every voter the caller names has declared every bit being raised. A raised bit is
never lowered. An entry that is already committed is always applied: a node that
cannot read it fails loudly instead of skipping it. Both bits are required for a
grant or a switch; one is not enough. Bits 8 to 15 are held for other records.

The leader does not know what each chunk server supports. The writer probes the
replica verb on the three nodes before it asks for a grant, and a node that does
not speak log chunks stops the writer before any metadata command is sent.

### Irreversibility

**IRREVERSIBLE ONCE ENABLED. Merging this change is reversible: nothing emits a version 15 or 16 outcome or a format 3 manifest until an operator raises BOTH cluster features (chunk manifest, bit 2, and log chunk leases, bit 4) with the explicit raise command, and nothing raises them by default. After they are raised and the first log chunk grant commits: (a) the metadata Raft log and snapshots contain outcome versions 15 and 16, snapshot version 22 and objects with manifest format 3 and log chunk extents; a build that lacks these readers FAILS to apply those entries (strict canonical re-encode, no skip), so no metadata voter can be run on an older build afterwards and an older gateway or client cannot read those objects; (b) a raised feature bit is never lowered: no command un-raises it; (c) an object written as log chunks stays chunked: going back to page extents means rewriting the object. Raising the features on a cluster that is not private is an explicit operator decision.**

## Operator procedure

Do this only on a private cluster that you can afford to rebuild. Raising the
features on a cluster that holds data you must keep readable by older builds is a
decision for the operator of that cluster, and it cannot be taken back.

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
5. Create the objects that will be log objects with the log object flag at create
   time. Page-mode objects are unaffected.

To check at any time: `read_cluster_features` returns the raised bits and every
voter's declaration; a sealed chunk entry in a manifest carries its footer CRC.
There is no command to lower a bit or to convert a log object back to pages.

## Memory model

In the current tree a chunk server holds an opened chunk entirely in memory: the
whole file, up to 64 MiB, is read when the chunk is opened. A node may keep 256
chunks open and refuses to open more. That is the same resident model as page
payloads, so it does not yet allow a corpus larger than memory.

The change that is in review, and not yet in the tree, makes memory flat in corpus
size:

- Only the open tail chunk of an object keeps its bytes in memory. A sealed chunk
  keeps no payload. Opening it verifies the checksum chain by streaming, and
  reads come from the file through a node-wide cache bounded in bytes (64 MiB by
  default), with the checksum of each record verified when it is read.
- The limit of 256 open chunks becomes a least-recently-used set of file handles:
  a sealed chunk's handle may be closed and reopened. An open tail, or a chunk
  with a sync still to come, is never closed.
- Open tails have their own budget (64 by default). Past it a new tail is refused
  with a retryable error, and nothing is closed to make room. Memory is then
  bounded by the tails times 64 MiB plus the cache.

Measured by tests of that change: the peak heap while opening a sealed 64 MiB
chunk fell from 71,303,204 bytes to 524,924 bytes, and reading all 64 MiB through
an 8 MiB cache peaked at 13,632,156 bytes.

## Limits and what is not measured

- The metadata cost per GiB appended is about 16 entries for a log object and one
  entry per page range in page mode. That is a count from the format, not a
  measurement of a running cluster.
- Append latency against object size up to 4 GiB, the number of metadata entries
  per append between chunk switches (zero by construction), the cost of a ranged
  read at that size, and a restart and recovery run at that size are pending
  measurement on a private cluster with both bits raised.
- Chunk server memory against corpus size, and metadata server memory per GiB
  appended, are pending measurement.
- A node refuses to open a new tail past its tail budget, and the metadata plane
  refuses a 4,097th entry. Both are errors the caller must handle.
- Quorum writes acknowledge at two of three replicas. A third replica that falls
  behind must catch up or be replaced before it serves a write again.
- Format 3 objects cannot be read by older builds, and a raised feature cannot be
  lowered. See the irreversibility statement above.
