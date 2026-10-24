# MatrixObject

A distributed object store, in Rust. **Apache-2.0** open source: public, forkable,
and open to issues and pull requests from anyone.

Objects are stored across replicated nodes with quorum writes, quorum reads,
per-page checksums and background repair.

```rust
use matrixobjectstore_rs::{MatrixObjectStore, StoreOptions};

let mut store = MatrixObjectStore::new(StoreOptions::default())?;
store.put_object("photos", "cat.jpg", bytes, "image/jpeg", now_ms, opts)?;
let object = store.get_object("photos", "cat.jpg")?;
```

## How storage is laid out

| tier | default size | what it is |
|---|---|---|
| **chunk** | 64 MiB | the append-only container objects are allocated from |
| **page** | 1 MiB | the replicated unit: what a quorum votes on and what repair rebuilds |
| **fragment** | derived | the pieces an erasure code splits one page into, plus its redundancy piece |

An object becomes one or more **extents** — a `(chunk, offset, length)` range,
each at most `max_extent_bytes` — and an extent maps onto the pages covering
it by `page_index = absolute_offset / page_size`. So a ranged read fetches
only the pages it needs instead of scanning a chunk, and a repair rebuilds one
page rather than a whole container.

**shard** is used in the distributed sense only -- metadata partitioned by key
range across services. The pieces inside one page are **fragments**: they never
leave the page they came from. The service coordinating a chunk's replicas is the
**chunk service**; the one holding a single replica is the **replica service**.

Two classes of name are still spelled the old way on purpose, because the text
itself is load-bearing rather than cosmetic:

| still named the old way | why it stays |
|---|---|
| **148 durable format constants** (36 contain `BLOCK`, 3 `SEGMENT`) | they are hashed into proofs, so changing the text changes every checksum computed from it |
| **154 control-field and report keys** | they are observable output -- someone's dashboard reads them |

Both are a versioned migration rather than a rename, and
`every_durable_format_constant_is_accounted_for` pins all 148 by name in both
directions: a renamed constant fails it, and so does a new one. The second count
is distinct lower-case literals containing `block` or `segment`; counted another
way it differs, which is why the definition is here and not only the figure.

The directories `src/services/page` and `src/services/chunk` hold the chunk
service and the replica service respectively, and move in a later change; the
types and fields are already `ChunkId`, `chunk_size`, `PageId`, `page_size`.

## Services

| service | responsibility |
|---|---|
| **meta** | object metadata, leases, key-range shards, transactions, the change feed |
| **page** | allocation, placement, the write and read quorums, repair and scrub |
| **chunk** | the data itself: one replica of each chunk, with its checksum and discard map |

### What a write promises

**A put either completes or it did not happen.** There is no partial object and no stated
size below which that holds and above which it does not: the guarantee is the same for a
one-page object and a five-page one.

A put is split into pages by `max_extent_bytes` and written one page at a time, so a
failure part way through is the case that matters. Both shapes are pinned:

| the failure | the guard |
|---|---|
| refused *after* mutating — the expected-checksum check fires once counters have moved | `a_put_that_fails_after_mutating_leaves_the_store_unchanged` |
| refused *inside* the write loop — four pages written, the fifth refused | `a_write_that_fails_part_way_through_its_pages_did_not_happen` |

Each asserts the store is unchanged in four views: the object is absent, its neighbour is
intact, the metadata fingerprint is unchanged, the chunk ledger is byte-identical and the
**snapshot bytes are identical**. One audit key is allowed to fall and never to rise — a
rollback clones the receipt maps and a clone allocates exactly, so a restored receipt holds
the same text in a tighter allocation.

What the store does **not** promise: a partial write does not report how far it got. The
error names why it was refused, not how many pages had landed.

### What a compaction gives back

A superseded object leaves its bytes behind, and the space ratio says so:
`space_amplification_ppm` is the logical bytes a chunk occupies over the bytes still live
in it. Overwrite one object in ten and it reads **1,100,000** — a tenth of the payload left
behind. Overwrite half and it reads 1,500,000.

`compact_all(min_stale_bytes)` moves every live object out of the chunks above that
threshold and retires them, which takes the ratio back to **1,000,000** and then finds
nothing to do: a second pass compacts zero chunks. Two things make that true, and both were
wrong before:

- **the chunk being drained is out of the allocator** for the whole rewrite, or the rewrite
  lands back in its own source — and the compaction then marks it stale, including the bytes
  it has just written there;
- **a drained chunk is retired**, or its logical bytes keep counting against the ratio and
  each pass makes the figure worse than the last.

A **retained version refuses** the compaction of its chunk. Compaction rewrites current
objects and has no path to move a version, and a version's bytes are never marked stale, so
compacting underneath one would declare them stale and offer them to the next writer.

`reuse_stale_chunk_space` is the other way to get the space back — the next write takes it
instead of growing the chunk — and it holds the ratio at 1,000,000 at any overwrite rate.
**It is on by default**, because the alternative is to accumulate without bound: nothing
here drives compaction, so a store nobody maintains keeps every superseded copy. Overwrite
one object nine times and the ratio reads 10,000,000.

Set it to `false` to get the other behaviour — the garbage accumulates where the collector
can see it, and `compact_all(min_stale_bytes)` takes it back on demand. Either way a
retained version's bytes are never stale, so neither route can take them.

### What a lease closes, and what it does not

A metadata lease is held over a key prefix by an owner at an epoch. While it is active, a
mutation of a key under that prefix must carry the owner and the epoch, and every object
door checks it: put, append, copy, delete, tags, retention, legal hold, restore, and all
four multipart doors -- create, upload-part, complete and abort.

A **multipart upload is fenced on the credentials recorded when it was created**, not on
anything the caller passes later. An upload created under a lease keeps working for the
writer holding it; a second writer is refused. The consequence: an upload created with no
credentials cannot be continued or aborted while a lease covers its key, until the lease
expires.

A **batch or prefix delete does not fail** when a lease protects one of its objects. It
reports that item as failed -- `deleted=0, failed=1, error_code=PreconditionFailed` -- and
leaves the object alone, so one protected key does not abandon the rest of the batch. Read
the report, not the return value.

The same doors also check the **metadata shard fence**, which refuses a mutation whose shard
is draining or owned elsewhere.

### A replica that missed a write does not get a vote

A page carries a write sequence on each replica that holds it, and a read takes a majority
over those. That compares the replicas with **each other**, so a replica that missed a
committed write and came back is only ever *outvoted* -- and when it is not outvoted, it
wins. With a read quorum of one it won, and served its superseded bytes as current.

So the page service keeps its own record, which the replicas cannot write to: the replicas
it will not believe. A replica goes in when it stops being able to receive writes --
quarantined, frozen, or taken out of the endpoint set -- and comes out only through
`check_in_replica`, which reads its pages and refuses while any of them trails a replica
that is believed, naming the page and both sequences.

| | |
|---|---|
| a believable replica | counts toward the read quorum |
| a replica in the record | still readable, still holds its bytes, still a repair target -- but it does not count |
| a read that cannot reach a quorum of believable replicas | **refused**, and the refusal says staleness was why |
| a read served without one | reported as degraded |

Three consequences worth stating plainly, because each is a choice:

- **A repair is not a check-in.** Repairing a page brings the bytes across; it does not
  establish that the replica holds every page it should, which is a different statement and
  the one the record is about.
- **Nothing is deleted.** A stale copy is kept -- it may be the last copy of something --
  which is also why a refused check-in can cite the evidence rather than having thrown it
  away.
- **A restart does not forget.** The record is in the page service snapshot, at format
  version 24; a snapshot written before that version has its record reconstructed from the
  quarantine and endpoint sets it does carry, because a replica that was not receiving
  writes is not believable whatever wrote the snapshot.

It costs one `BTreeSet` field on the page service, which is empty on a healthy store and
so allocates nothing: staleness is a property of a replica rather than of a page, so there
is no per-page term at all.

### What a scrub can be asked to do

A scrub reads every stored byte of every active replica, verifies it against its checksum
and, when asked, repairs from quorum. It is the one maintenance pass meant to run at a
throttled rate over days -- which means it has to be able to stop and carry on, and
`scrub_page_replicas(repair)` cannot: it does the whole store in one call.

That is the right shape for a store small enough to finish and the wrong one for a store
large enough to need throttling, so the bounded form is additive:
`scrub_page_replicas_bounded(after_page_id, max_chunks, repair)` scans at most `max_chunks`
pages and says where it stopped.

| field | what it carries |
|---|---|
| `marker` | the cursor the call was given |
| `next_marker` | the last page the call scanned |
| `has_more` | whether there is more to do -- **this is the field to loop on** |

A `max_chunks` of zero is refused rather than taken to mean "no bound", and all three
fields carry the meanings they already carry on `audit_page_replicas`, which bounds and
resumes over these same pages. Two calls over one set of pages, taking the same argument
names, should not mean different things -- so a test asserts the pair agree, rather than a
comment claiming they do.

## What it does

- **Quorum writes and reads.** A write needs `write_quorum` replicas to accept.
  A read votes on `(write_sequence, checksum, etag)` across replicas, so a
  replica that has fallen behind cannot answer with stale bytes.
- **Repair.** `repair_chunk_from_quorum` rebuilds one chunk on one replica from
  the others, so a diverged replica costs one chunk of recopying.
- **Erasure coding.** A page with no peers keeps a redundancy fragment, so a
  partial corruption is still recoverable from the single copy it has. The
  fragment is local to its page and is empty whenever the page has peers, which
  is why the data term measures exactly 3.00x at three replicas.
- **Striped payload, if you ask for it.** `stripe_redundancy_units` spreads a
  page across its replicas as fixed-size units instead of copying it to each, so
  the store holds `replica_count / (replica_count - stripe_redundancy_units)` of
  the payload -- **1.5x against replication's 3.0x** at three replicas and one
  unit of redundancy. Replication is the default. The trade is durability: one
  loss survived where three-way replication survives two. A striped write needs
  every replica to accept it, so `write_quorum` must equal `replica_count`.
  Two limits, named rather than implied: a replica of a striped service cannot be
  quarantined (the guard refuses dropping below the write quorum), and
  `repair_chunk_replicas` reports a striped page with a lost replica as
  unrepaired instead of rebuilding its units -- reads still serve it by
  reconstructing on demand, so it is readable and one loss from unreadable at the
  same time. [Footprint](#footprint) has the whole-store figures.
- **Space reuse.** Overwriting an object normally leaves its previous version
  resident until collection unless `reuse_stale_chunk_space` is left on, when the next write takes
  that space directly, which measures about 9% off resident footprint on an
  overwrite-heavy workload.
- **Idempotent mutations.** Allocation, write, discard and repair carry
  receipts, so a retried request is not applied twice.
- **Object semantics.** Buckets, versioning, multipart upload, object lock and
  retention, lifecycle policies, tagging, CORS and bucket policies, quotas, and
  a change feed.

## Footprint

At three replicas with the metadata counted, measured at 26-character keys:

| object size | objects | amplification | data-only figure |
|---:|---:|---:|---:|
| 64 KiB | 1,000 | **3.01x** | 3.00x |
| 4 KiB | 4,000 | **3.09x** | 3.00x |
| 1 KiB | 8,000 | **3.36x** | 3.02x |

The data term is exactly **3.00x** -- the replication floor for a store that
copies every page to every replica -- so everything above it is metadata, which
is **330 bytes an object plus 164,240 fixed** whatever the object size.

Those bytes are not all in one place. **218 of them are a storage node's own** -- the
object's entry in its map, and its key in the payload index -- and **112 are the metadata
service's**, which keeps its own record of every object with its own versioning, deletion
markers and change log. This crate models both services in one process, so the audit counts
both; a deployment runs them on different machines, where a storage node carries 218 bytes
an object rather than 330.

The metadata service's record is **a name, two stamps and a digest**, and the collection
holding it is ordered by that name rather than keeping a second copy of it. It does not restate the
object: it holds an eight-byte digest of the object's rendered metadata, so it can prove a
record matches the object without carrying a second copy of it. What that costs is a restore
that is no longer self-sufficient -- a snapshot carries the digest, not the characters, so
reproducing a value needs the object.
`every_metadata_leg_is_a_line_and_the_intercepts_are_named` measures both shares and fails
if either stops being what this file publishes.
`footprint_audit()` names every resident byte and its breakdown sums to its own
total with no remainder.

Those two numbers are a line, fitted from measurements at three object counts by
`the_published_metadata_footprint_line_is_the_one_this_tree_produces`, which fails
if this file disagrees with the tree.

A store that stripes moves the data term itself, to **1.5x** at three replicas and
one unit of redundancy -- which is the least any layout can hold and still survive a
loss. Every replica holds one unit of each stripe group, so keeping `p` units of
redundancy over `R` replicas costs `R / (R - p)`, which is smallest at one unit; with
no redundancy nothing is recoverable, and the only way further down is more replicas,
which is more hardware rather than less space.
`one_unit_of_redundancy_is_the_least_a_layout_can_hold` sweeps every layout from three
replicas to eight and fails if any of them undercuts that.

Counted with the metadata, a thousand objects of 1 KiB measure
**2.01x striped against 3.50x replicated**, both read out of `footprint_audit()` on a
store nothing has read back yet. What separates the total from the 1.5x data term is the
metadata line above, which striping does not change: the same 330 bytes an object either
way. Striping moves the payload and nothing else.

Both numbers are for the same object count, so the comparison is between two
stores rather than across two measurements. It is a smaller point than the rows
above because measuring both shapes at 8,000 objects takes longer than the whole
suite, and a figure only a test nobody runs would check is a figure that goes
stale.

```
cargo test --lib a_store_holds_one_and_a_half_copies_of_its_payload_when_it_stripes -- --nocapture
cargo test --lib the_published_striped_amplification_is_the_one_this_tree_produces -- --nocapture
```

```
MEASURE_OBJECTS=8000 MEASURE_SIZE=1024 cargo test --release --lib measure_the_metadata_footprint -- --ignored --nocapture
```

[docs/footprint.md](docs/footprint.md) has the per-leg working: which total to
divide by, why lowering `replica_count` does not reach 1.0x, what is bounded by
a setting rather than by your data, and what the change feeds cost.

### What a mutation holds while it runs

Every figure above is the store **at rest**. A mutation also holds a copy of what
it may have to put back, from the moment it starts until it commits or is
refused, so peak resident is the store plus that copy — and a copy taken and
dropped inside one call is invisible to a figure taken at rest.

Two copies exist, and they are reported separately because they are bounded by
different things.

**The store's own undo**, reported by `ObjectMutationRollback::footprint_audit`,
at 1 KiB objects:

| stored objects | undo |
|---:|---:|
| 250 | 1,225 B |
| 1,000 | 1,225 B |
| 2,000 | **1,225 B** |

Flat, because a put, a delete, an append and a copy each write one object key,
one metadata record and one change record, and the undo carries exactly that --
not the payload index, not the store's containers, and not the bytes of a
multipart upload in flight.

**The page service's mutation batch** holds a second copy, boxed, so the undo
above can leave the page service alone. It separates what it copies from what it
only names:

| stored objects | copied | named |
|---:|---:|---:|
| 250 | 2,485 B | 768,312 B |
| 1,000 | **2,485 B** | 3,072,312 B |

Also flat in the object count, because a chunk's payload is shared rather than
copied and so are the receipt maps. The copy is 847 bytes of page service state,
1,398 bytes of replica state, 168 bytes of shared receipt handles, and 24 bytes
for each chunk — so what moves it is the chunk count, which the object SIZE
drives rather than the object count.

The receipt maps are shared until the batch writes one, which is what keeps them
at the 168 bytes counted above rather than the whole of what the receipt table
reaches at its bound.
`the_published_mutation_copy_figures_are_the_ones_this_tree_produces` reads this
table and fails if either figure stops being what the tree produces.

```
cargo test --release --lib measure_what_a_mutation_undo_costs -- --ignored --nocapture
cargo test --release --lib measure_what_a_mutation_batch_snapshot_costs -- --ignored --nocapture
```

#### A delete does not reclaim, it trades

Writing 500 objects of 1 KiB and then deleting every one leaves the store
**26.1% larger** than when it held them:

| | bytes |
|---|---:|
| holding 500 objects | 1,791,633 |
| after deleting all 500 | 2,260,646 |

The payload index is given back in full. What replaces it is **822 bytes of
idempotency receipt per delete** across three replicas, bounded only by
`receipt_retention`, which defaults to 100,000; plus 106 bytes of change feed and
57 of metadata change log per delete, both bounded by their retention constants.
The object's own entry does not go away either — a delete files a delete marker,
so the 210 bytes of its `objects` entry stay, which is what the change feed and
the versioned paths read.

Those receipts are also the term that makes the mutation batch grow: after N
deletes, every subsequent mutation's batch copies **615 bytes per retained
receipt**, which at the default bound is about 61.5 MB copied per mutation.
Issue #20 has the measurement and what narrowing it requires.

So a deployment sizing from the figures above should add the undo and the batch
copy to the resting total, and a workload that deletes should expect the receipt
retention to dominate both.

## Reaching the store over a network

An S3-compatible HTTP front door and a binary that serves it are built in, so a
store can be reached from another machine rather than only in process.

```
MATRIXOBJECT_SNAPSHOT_PATH=/var/lib/matrixobject/store.snapshot \
MATRIXOBJECT_BUCKET=mybucket \
cargo run --release --bin object_server
```

| variable | default | meaning |
|---|---|---|
| `MATRIXOBJECT_HTTP_ADDR` | `127.0.0.1:17200` | address to bind |
| `MATRIXOBJECT_BUCKET` | `default` | bucket created at startup, idempotently |
| `MATRIXOBJECT_SNAPSHOT_PATH` | *(unset)* | durable snapshot file; **in memory when unset** |
| `MATRIXOBJECT_MAX_BODY_BYTES` | 64 MiB | largest object this door accepts |
| `MATRIXOBJECT_THREADS` | 4 | accept threads |

The verbs, where `{bucket}` is the first path segment and `{key}` is everything after it:

| request | does |
|---|---|
| `PUT /{bucket}/{key}` | write an object; honours `If-None-Match: *` and `If-Match: "etag"` |
| `GET /{bucket}/{key}` | read it; honours `Range: bytes=` and `If-None-Match` |
| `HEAD /{bucket}/{key}` | its headers, and the length the matching `GET` would send |
| `DELETE /{bucket}/{key}` | remove it; idempotent, so a retried delete answers 204 |
| `PUT /{bucket}` | `CreateBucket`, idempotently; answers a `Location` and no body |
| `HEAD /{bucket}` | `HeadBucket`: 200 if it exists, `NoSuchBucket` if not |
| `DELETE /{bucket}` | `DeleteBucket`; `BucketNotEmpty` with 409 while it still holds objects |
| `GET /{bucket}?prefix=&delimiter=&continuation-token=&start-after=&max-keys=` | `ListObjectsV2`, with the delimiter rolled up into `CommonPrefixes` |
| `GET /` | `ListBuckets` |
| `POST /{bucket}/{key}?uploads` | `CreateMultipartUpload` |
| `PUT /{bucket}/{key}?partNumber=&uploadId=` | `UploadPart`; answers the `ETag` the completion must name |
| `POST /{bucket}/{key}?uploadId=` | `CompleteMultipartUpload`; the part list in the body is validated against what was uploaded |
| `DELETE /{bucket}/{key}?uploadId=` | `AbortMultipartUpload` |
| `GET /{bucket}/{key}?uploadId=` | `ListParts` |
| `GET /{bucket}?uploads` | `ListMultipartUploads` |
| `POST /{bucket}?delete` | `DeleteObjects`; 200 with a per-key report, because one key failing is not the batch failing |
| `PUT /{bucket}/{key}` + `x-amz-copy-source` | `CopyObject`, honouring `x-amz-metadata-directive` |
| `GET /{bucket}?location` | `GetBucketLocation` |
| `GET /{bucket}?versioning` | `GetBucketVersioning` |
| `PUT /{bucket}?versioning` | `PutBucketVersioning` |
| `GET /{bucket}?versions` | `ListObjectVersions`, with delete markers reported as such |
| `GET` / `HEAD /{bucket}/{key}?versionId=` | read one named version |
| `DELETE /{bucket}/{key}?versionId=` | remove one named version |
| `GET` / `PUT` / `DELETE /{bucket}/{key}?tagging` | object tagging |
| `GET` / `PUT` / `DELETE /{bucket}?cors` | bucket CORS |
| `GET /healthz` | readiness, which takes the store lock rather than only reporting the listener |

A write answers the object's `ETag`, `Content-Type`, `Last-Modified`, its version and length, and
any `x-amz-meta-*` headers it was given back again. A listing reports `<IsTruncated>` and, when it
is truncated, the `<NextContinuationToken>` to resume from -- a client cannot tell a full page from
a truncated one by counting, and a token on a final page would make it ask again for nothing.

Every failure is an `<Error>` document carrying the `<Code>` a client branches on, and the codes
are S3's: `NoSuchBucket` and `NoSuchKey` are distinguished rather than both reported as a 404 with
prose, because one tells a client to create the bucket and retry and the other tells it to stop.
`NoSuchUpload` is a third, for the same reason. A V1 listing client that sends `marker` and no
`list-type` is served the same body.

`CompleteMultipartUpload` is the one request whose body is read. The parser recognises exactly that
document and refuses everything else -- including a document type declaration, a part list under
another root element, and trailing or missing elements -- because accepting more of XML buys nothing
a client needs. The part list is then checked against what was actually uploaded, and a mismatch
answers `InvalidPart` or `InvalidPartOrder`: the store completes whatever parts it holds and takes
no list, so a mismatch not caught here is not caught at all, and the client would be handed an
object assembled from something other than what it sent.

### Three things to know before putting data in it

**It is unauthenticated.** No signing, no token, no TLS. Anything that can open a socket to the
port can read, overwrite and delete every object. That is why the default bind is loopback; moving
it off loopback puts the whole store behind whatever else protects that network. The binary says so
on startup when the address is not loopback.

**Without a snapshot path nothing survives the process.** The default store is entirely in memory.
The startup line always reports which of the two you have -- `snapshot=none` included -- so it is
visible then rather than at the next restart.

**Requests are serialised.** The store keeps interior mutability for its caches and durable stamps,
so it is `Send` but not `Sync`: it can move between threads but cannot be shared by them, and the
server holds it behind one lock. Extra accept threads stop a slow client stalling others; they do
not add parallelism. This door exists so data can reach the store from another machine, not so it
can reach it faster.

### What it is not

An `Authorization` header is accepted and ignored rather than refused, so a signing
client works against this surface -- it is simply not verified. There is no SigV4.

Not implemented, and answered with `NotImplemented` or `MethodNotAllowed` rather than with an empty
success: object ACLs, bucket policies, lifecycle verbs, object lock.

Five places the model is narrower than S3's, each answered as itself rather than
approximated:

| | this store | why |
|---|---|---|
| **CORS** | one flat policy per bucket; a second rule is **refused**, not flattened | flattening would union the rules, so an origin one rule allowed only `GET` would come back allowed whatever another permitted -- access the request never asked for. One rule round-trips exactly |
| **The 5 MiB minimum** for a non-final part | any part size is accepted | the check would only ever refuse an upload this store can serve |
| **Versioning** (`Enabled`, `Suspended`, never configured) | one flag, so `GetBucketVersioning` omits `<Status>` for `false` | that is what S3 sends for a never-configured bucket, and a client acts on it identically. Telling suspended from never-configured needs the third state |
| **Version ids** (opaque strings in S3) | a number, reported as `x-amz-version-id`; `versionId=null` is **refused** | a client must treat an id as opaque, so it cannot tell. Every object here carries a version from the start, so S3's pre-versioning `null` names no version -- serving the current object would hand back a different one |
| **`GetBucketLocation`** | an empty `<LocationConstraint/>` | S3's own value for its default region, and the only answer true of a store with no region. Answered at all because SDKs call it before their first request |

`GET /healthz` is deliberately not S3: it answers plain text, because a container
probe and an operator read it and an `<Error>` document would help neither.

Anything a caller might reasonably expect and not get answers a status rather than
a plausible-looking wrong answer: `chunked` framing is refused with 501 rather than
read as a body of the wrong length, a `PUT` with no `Content-Length` is refused with
411 rather than stored as empty, and a multi-range request falls back to the whole
object rather than returning one range under a 206 claiming to be all of it.

There is no deduplicating write: an object's `etag` is derived from its identity --
bucket, key, version, length, content type -- not from a hash of its content, so
comparing etags cannot tell the same bytes from the same shape. The store's real
conditional writes are applied inside the mutation it guards.

### Embedding it

```rust
use matrixobjectstore_rs::net::{ObjectHttpServer, ServerOptions};

let store = std::sync::Arc::new(std::sync::Mutex::new(store));
let server = ObjectHttpServer::bind("127.0.0.1:17200", store, ServerOptions::default())?;
let handle = server.spawn()?;          // returns at once; `local_addr` reports the bound port
// ... later ...
handle.shutdown();                     // stops the accept loops and waits for them
```

The listener is non-blocking so the accept loops can be stopped, which is what makes the server
usable as a component and testable in-process; the cost is one poll per idle interval.

It adds **no dependencies**. The HTTP reading and writing is written on the standard library
alone -- see the comment at the top of `src/net/http.rs` for why that is a deliberate constraint
for a crate that other people's data sits inside, and for the list of framings it refuses rather
than guesses at.

## Building

```
cargo build --release
cargo test
```

Nothing external is required to build or run the tests.

## Configuration

`StoreOptions` carries the knobs. The ones most worth knowing:

| option | default | meaning |
|---|---|---|
| `chunk_size` | 64 MiB | how large a chunk grows before a new one is opened |
| `page_size` | 1 MiB | the replicated unit a write lands in — see the sizing note below |
| `max_extent_bytes` | 4 MiB | the largest contiguous range one extent may cover |
| `volume_count` | 1 | volumes to spread chunks across |
| `replica_count` | 3 | copies kept of each chunk |
| `write_quorum` | 2 | replicas that must accept a write |
| `stripe_redundancy_units` | 0 | units of redundancy in a stripe group; 0 copies every page to every replica |
| `stripe_unit_bytes` | 64 KiB | bytes in one stripe unit, when striping |
| `reuse_stale_chunk_space` | true | take the space a superseded object left instead of growing the chunk; set it false to let the collector see the garbage first |

Every default above is `StoreOptions::default()`, and
`every_option_in_the_table_is_a_field_with_that_default` reads this table and
fails if a row names a field that does not exist or states a default the tree
does not have.

### The control plane

The control plane is the **metaserver**: one metadata service, embedded by default.
`control_plane_mode` takes `auto` (the default) or `metaserver`, and both select it --
`auto` unconditionally, `metaserver` using whatever `metaserver_catalog_endpoints` names
and the embedded catalog when it names nothing.

Two earlier modes, `globalcatalog` and `rootserver`, selected a separate root catalog
instead. Both are retired: the spellings are refused by name, a root catalog endpoint is
refused at construction, in the environment and on snapshot load, and a snapshot written in
`rootserver` mode **with no endpoint loads as a metaserver**, because that is what it already
was. One written with an endpoint is refused rather than silently repointed.

### Picking the two that matter

**`page_size` is the one worth setting deliberately.** It is the replicated
unit, so an object smaller than a page still occupies a page's worth of
replication, and an object much larger than a page is split across more pages
than a ranged read needs. Sizing `page_size` near your median object size is
what keeps both from happening.

**`receipt_retention` is not in the options table** -- it is configured on a
live store with `configure_chunk_receipt_retention`, and it bounds what a
mutation's idempotency receipts cost. Set `receipt_retention` rather than
leaving it at its 100,000 default if your workload deletes heavily.

[docs/configuring.md](docs/configuring.md) has the sizing table for both, and
the measurements behind them.

## Contributing

Anyone can contribute: fork, commit, open a pull request. Issues are open, and
`main` carries no branch protection, so an outside pull request can land without a
review you cannot obtain. [CODEOWNERS](.github/CODEOWNERS) requests a reviewer; it
gates nothing.

[CONTRIBUTING.md](CONTRIBUTING.md) is the guide -- what CI runs and what it does not,
the 21 recorded test failures and how to tell yours from them, and the shape a change
is expected to take. [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) applies. There is a
pull-request template and two issue templates; a security or durability report goes
privately through [SECURITY.md](SECURITY.md) instead of an issue.

`the_repository_carries_what_an_outside_contributor_needs` checks those files are
here, that the contributing guide is this project's, and that every path it sends a
reader to exists -- the guide was a sibling library's for a while, and a check on mere
existence scored it present.

Two invariants worth knowing before you start:

- **A durable format is a contract.** Snapshot and proof encodings are hashed
  into checksums, so changing the bytes a format emits — including the text of a
  format constant — changes every checksum computed from it. That is a
  migration, not a rename.
- **A measurement needs its denominator.** Performance claims here are expected
  to say what they are per: per put, per live byte, per stored object. "Faster"
  without one is not a result.

## Continuous integration

On pushes to `main` and on every pull request, two jobs:

- **build & docs** -- `cargo build --all-targets` and `cargo doc`, each for the
  default features and for `--all-features`, with warnings denied.
- **format and rollback guards** -- each guard run on its own with `--exact`, and
  its output checked for `test result: ok. 1 passed`, because `cargo test` exits 0
  when a filter matches nothing.

CI does not run the whole library suite. `tools/suite_gate.sh` does, locally: it
compares the failures by name against the 21 recorded in
`tools/known_failures.txt`, and fails in **both** directions -- a failure that is
new, and a recorded one that has started passing and should come off the list.

## Licence

Apache License 2.0 -- see [LICENSE](LICENSE) and [NOTICE](NOTICE).

Every Rust source opens with the two lines that say so:

```text
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
```

`every_rust_source_carries_the_licence_header` walks the tree and fails if a source
is missing either line, if the walk finds fewer than a thousand sources, or if the
`LICENSE` and `NOTICE` stop saying what the header claims.
