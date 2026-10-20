# MatrixObject

A distributed object store, in Rust. Apache-2.0.

MatrixObject stores objects across replicated storage nodes with quorum writes,
quorum reads, per-unit checksums and background repair. It is open source and
free for anyone in the world to use, fork, deploy and contribute to.

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

Two words are used deliberately narrowly:

- **shard** means the distributed sense only: metadata partitioned by key range
  across services. The pieces inside a single page are **fragments**, because
  they never leave the page they came from.
- **page** is the replicated unit of storage: `PageId`, `PageState`, `page_size`.
  It is 1 MiB by default and it is what a quorum votes on and what repair
  rebuilds. The service that coordinates one chunk's replicas is the **chunk
  service**; the service that holds a single replica is the **replica
  service**.

The directory names still say `src/services/page` and `src/services/chunk` for
the chunk service and the replica service; those move in a later change. The
types and fields are renamed: `ChunkId`, `chunk_size`, `PageId`, `page_size`. What is still spelled the old way is spelled that way
deliberately, and it is worth knowing which before you reach for a tidy-up:

| still named the old way | why it stays |
|---|---|
| **148 durable format constants** (36 contain `BLOCK`, 3 `SEGMENT`) | they are hashed into proofs, so changing the text changes every checksum computed from it |
| **154 control-field and report keys** | they are observable output -- someone's dashboard reads them |

The key count is distinct lower-case string literals containing `block` or
`segment`; counted another way it comes out differently, which is why the
definition is here and not just the figure.

Both classes are a versioned migration rather than a rename, and
`every_durable_format_constant_is_accounted_for` pins all 148 constants by name
in both directions: a renamed constant fails it, and so does a new one.

## Services

| service | responsibility |
|---|---|
| **meta** | object metadata, leases, key-range shards, transactions, the change feed |
| **page** | allocation, placement, the write and read quorums, repair and scrub |
| **chunk** | the data itself: one replica of each chunk, with its checksum and discard map |

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
- **Space reuse.** Overwriting an object normally leaves its previous version
  resident until collection. With `reuse_stale_chunk_space` the next write takes
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
| 4 KiB | 4,000 | **3.18x** | 3.00x |
| 1 KiB | 8,000 | **3.69x** | 3.02x |

The data term is exactly **3.00x** -- the replication floor -- so everything
above it is metadata, which is **661 bytes an object plus 172,456 fixed**
whatever the object size. `footprint_audit()` names every resident byte and its
breakdown sums to its own total with no remainder.

```
MEASURE_OBJECTS=8000 MEASURE_SIZE=1024 cargo test --release --lib   measure_the_metadata_footprint -- --ignored --nocapture
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
one metadata record and one change record, and the undo carries exactly that.
Three terms used to make it grow instead: the payload index at 40 bytes per
stored object, every container in the store for an append or a copy, and — the
largest — every byte of every multipart upload in flight, which made an
unrelated put's undo 8,391,068 bytes while an 8 MiB upload was open.

**The page service's mutation batch** holds a second copy, boxed, so the undo
above can leave the page service alone. It separates what it copies from what it
only names:

| stored objects | copied | named |
|---:|---:|---:|
| 250 | 2,485 B | 768,288 B |
| 1,000 | **2,485 B** | 3,072,288 B |

Also flat in the object count, because a chunk's payload is shared rather than
copied and so are the receipt maps. The copy is 823 bytes of page service state,
1,398 bytes of replica state, 168 bytes of shared receipt handles, and 32 bytes
for each chunk — so what moves it is the chunk count, which the object SIZE
drives rather than the object count.

The receipt maps used to be the term that grew. They were copied outright, so an
open batch held the whole of what the receipts table above reaches at its bound;
sharing them until the batch writes one took that to the 168 bytes counted here.
`the_published_mutation_copy_figures_are_the_ones_this_tree_produces` reads this
table and fails if either figure stops being what the tree produces.

```
cargo test --release --lib measure_what_a_mutation_undo_costs -- --ignored --nocapture
cargo test --release --lib measure_what_a_mutation_batch_snapshot_costs -- --ignored --nocapture
```

#### A delete does not reclaim, it trades

Writing 500 objects of 1 KiB and then deleting every one leaves the store
**22.3% larger** than when it held them:

| | bytes |
|---|---:|
| holding 500 objects | 1,949,253 |
| after deleting all 500 | 2,384,394 |

The payload index is given back in full. What replaces it is **822 bytes of
idempotency receipt per delete** across three replicas, bounded only by
`receipt_retention`, which defaults to 100,000; plus 114 bytes of change feed and
57 of metadata change log per delete, both bounded by their retention constants.
The object's own entry does not go away either — a delete files a delete marker,
so 226 of the 282 bytes an object cost stay, which is what the change feed and
the versioned paths read.

Those receipts are also the term that makes the mutation batch grow: after N
deletes, every subsequent mutation's batch copies **615 bytes per retained
receipt**, which at the default bound is about 61.5 MB copied per mutation.
Issue #20 has the measurement and what narrowing it requires.

So a deployment sizing from the figures above should add the undo and the batch
copy to the resting total, and a workload that deletes should expect the receipt
retention to dominate both.


## Reaching the store over a network

Every way into the store used to be an in-process call, so a store was only ever as shared as the
process holding it. `--features http-server` adds an HTTP front door and a binary that serves it.

```
MATRIXOBJECT_SNAPSHOT_PATH=/var/lib/matrixobject/store.snapshot \
MATRIXOBJECT_BUCKET=mybucket \
cargo run --release --features http-server --bin object_server
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
| `PUT /{bucket}` | create the bucket, idempotently |
| `GET /{bucket}?prefix=&marker=&max-keys=` | newline-delimited keys |
| `GET /healthz` | readiness, which takes the store lock rather than only reporting the listener |

A write answers the object's `ETag`, `Content-Type`, `Last-Modified`, its version and length, and
any `x-amz-meta-*` headers it was given back again. A listing says whether it was truncated and,
if it was, the marker to continue from -- a client cannot tell a full page from a truncated one by
counting.

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

Not an S3-compatible endpoint. The bucket and key come from the path, the conditional headers are
the ones the store enforces internally, and user metadata travels as `x-amz-meta-*` -- but there is
no request signing, no multipart upload, no versioning or tagging verb, and a listing answers
`text/plain` keys rather than the XML body an S3 client parses. Anything a caller might reasonably
expect and not get answers a status rather than a plausible-looking wrong answer: `chunked` framing
is refused with 501 rather than read as a body of the wrong length, a `PUT` with no
`Content-Length` is refused with 411 rather than stored as empty, and a multi-range request falls
back to the whole object rather than returning one range under a 206 that claims to be all of it.

There is also no deduplicating write. An earlier sketch of this surface answered a write whose
bytes were already stored with `208 Already Reported`; that is not implementable here, because an
object's `etag` is derived from its identity -- bucket, key, version, length, content type -- and
not from a hash of its content. Comparing etags cannot tell the same bytes from the same shape, and
the honest alternative would read the stored object back on every write. What the surface does
support is the store's real conditional writes, applied inside the mutation the store guards rather
than checked out here and then written.

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
| `reuse_stale_chunk_space` | false | take the space a superseded object left instead of growing the chunk |

Every default above is `StoreOptions::default()`, and
`every_option_in_the_table_is_a_field_with_that_default` reads this table and
fails if a row names a field that does not exist or states a default the tree
does not have. Four of the six rows this replaces were wrong: `slab_size` and
`reuse_stale_slab_space` were not fields at all, `chunk_size` was given as 1 MiB
when it is 64, and `page_size` — the option the next section calls the one worth
setting deliberately — was missing.

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

Anyone can contribute: fork, commit, open a pull request. Issues are open.
See [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

Two invariants worth knowing before you start:

- **A durable format is a contract.** Snapshot and proof encodings are hashed
  into checksums, so changing the bytes a format emits — including the text of a
  format constant — changes every checksum computed from it. That is a
  migration, not a rename.
- **A measurement needs its denominator.** Performance claims here are expected
  to say what they are per: per put, per live byte, per stored object. "Faster"
  without one is not a result.

## Continuous integration

`cargo build`, the library suite, and the measurement suite, on every push.
`tools/known_failures.txt` lists the tests known to fail, and a change is
gated by the failures matching that list name for name rather than by a count.

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to run the same gates locally.

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
