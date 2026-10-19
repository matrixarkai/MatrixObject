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

## What the footprint audit reports, and which total to divide by

`footprint_audit()` returns two totals, and the difference between them is the
whole point:

- **`byte_buffers`** is payload buffers, replica data, redundancy fragments and
  checksums. An amplification derived from it alone is **data-only**, because
  everything else the store holds used to be reported as an entry count and
  never converted to bytes.
- **`total_bytes`** adds the metadata the store keeps to describe that data, and
  is the one to divide by `logical.written`.

Measured at three replicas, 26-character keys, with the metadata counted:

| object size | objects | amplification | data-only figure |
|---:|---:|---:|---:|
| 64 KiB | 1,000 | **3.01x** | 3.00x |
| 4 KiB | 4,000 | **3.18x** | 3.00x |
| 1 KiB | 8,000 | **3.69x** | 3.02x |

Reproduce any row with the measurement harness:

```
MEASURE_OBJECTS=8000 MEASURE_SIZE=1024 cargo test --release --lib \
  measure_the_metadata_footprint -- --ignored --nocapture
```

**Lowering `replica_count` does not reach 1.0x.** A page with no peers keeps a
redundancy fragment so that a partial corruption is still recoverable from the
single copy it has, and a page with peers does not — which is why the data term
above is exactly 3.00 rather than something above it. Measured at one replica:

| bytes in the page | fragment | split | overhead |
|---:|---:|---:|---:|
| 20,480 | 10,240 | 2 | 50% |
| 204,800 | 12,800 | 16 | **6.25%** |

The split is `clamp(bytes / 8192, 2, 16)` and the fragment is one more piece, so
the overhead is 1/N and falls as a page fills: **a single replica costs 1.5x for
a nearly empty page and 1.0625x past 128 KiB.**
`a_page_with_no_peers_carries_a_redundancy_fragment` asserts both rows, and
`no_page_carries_a_redundancy_fragment_while_it_has_peers` asserts the
suppression that makes the 3.00 exact.

### Metadata is per object, and the per-object figure depends on how many there are

At a fixed object count the metadata total barely moves with what the objects
contain: 2,851,246 bytes for 4,000 objects at both 1 KiB and 4 KiB. So it
disappears into a large object and dominates a small one, which is the whole
reason the two totals are reported separately.

It is not quite constant, and the reason is worth knowing: an object's length is
stored as **decimal text**, so the per-object cost rises by one byte for every
decimal digit in the size. At 4,000 objects the metadata total is 2,847,246 at
256 B, 2,851,246 at both 1 KiB and 4 KiB, and 2,855,606 at 64 KiB — a byte an
object per digit, which is why the 1 KiB and 4 KiB figures agree exactly and the
others do not.

The per-object figure falls as the store grows, because two of the structures
are bounded rather than per object:

| objects | 100 | 1,000 | 4,000 | 8,000 |
|---:|---:|---:|---:|---:|
| metadata per object | 914 B | 867 B | 712 B | **686 B** |

Both change feeds cap at 1,024 records, so together they are 27.2% of metadata
at a hundred objects and 3.8% at eight thousand. Everything else is exactly
linear, which makes the total a line once the feeds are full:

```
metadata = 661 bytes an object + 207,246 bytes fixed      (remainder: zero)
```

measured at 4,000 and 8,000 objects of 1 KiB, and reproducing both totals
exactly. The fixed term is measured rather than derived, which matters because
deriving it from the feeds' own per-record costs gives 206,848 and loses 398
bytes that are not in the feeds at all:

| | bytes | |
|---|---:|---|
| object change feed | 149,504 | 1,024 records at its bound |
| metadata change log | 57,344 | 1,024 records at its bound |
| inside `objects` | 86 | the content-type and storage-class text, shared by every object |
| inside `meta.records` | 108 | one record more than there are objects — the bucket's |
| `meta.entities` | 84 | |
| `segments` | 120 | |
| **fixed total** | **207,246** | |

`every_metadata_leg_is_a_line_and_the_intercepts_are_named` fits each leg over
three store sizes and fails unless every one is a line and the fixed terms sum
to the total's — so a leg that starts depending on the object count, or a fixed
byte that stops being named, is caught rather than absorbed.

That 661 is now more than half one item:

| | per object | share |
|---|---:|---:|
| `meta.records` — the metadata projection record | 355 B | 53.7% |
| `objects` — the structured metadata | 282 B | 42.7% |
| payload index keys | 24 B | 3.6% |

Those two halves hold the **same facts twice**. `objects` is a binary struct —
152 bytes inline, with the object id, length, etag, content type, storage class
and extent list in fixed-width fields. `meta.records` is a text projection of
the same object, and it is the largest single item in the store per object: 227
bytes of value, **34% of everything the store holds per object**.

Measured against what that text actually distinguishes:

| | per record | share | |
|---|---:|---:|---|
| field names and separators | 151 B | 70% | `retention_until_ms`, `metadata_count`, … spelled out for every object |
| values that never differ | 24 B | 11% | 9 of the 12 fields have **one** distinct value across every object |
| values that differ | 42 B | 19% | etag, length, content type |

So **175 of 217 bytes carry no information that varies between objects** — 81%,
and 26% of all per-object metadata. Issue #40 has the measurement and what
closing it would require; it is a durable format, so the cost is a migration
rather than an edit. The item is unchanged in absolute terms while everything
around it has shrunk, which is why its share keeps rising.

So if you store small objects, three replicas do not cost 3x. Any amplification
figure published for this store before the metadata was counted was the
data-only column, including in this file; and any per-object figure quoted
without its object count was measured at a hundred objects, where the bounded
feeds have not yet amortised.

`byte_buffers` also counts what the caches hold: each replica's read cache and
the per-page hashes beside it, and the cached encoded snapshots the store, the
page service and every replica keep. A read cache is bounded by a configured
size per replica rather than by anything the data implies, so a deployment
serving reads can hold as much there as it holds data. All of those read zero in
the rows above, which write and never read.

Neither total includes B-tree node slack or allocator overhead, which is what
the remainder against process RSS is. Capacity is counted rather than length,
because the slack is resident and showing slack is what the audit is for. Text
shared between holders is charged once per allocation — the characters and the
two counts in front of them — so `mapkey_capacity` against `mapkey_len` is that
header rather than slack.

### What is bounded by a setting rather than by your data

Every figure above scales with what you store. Several structures do not: they
are bounded by a configured limit, so they are the same size whether the store
holds a thousand objects or a million, and a deployment has to provision for
them separately.

Each per-unit cost below is measured rather than estimated. The figures come
from the ignored `measure_*` tests in the suite, which print them on demand:

```
cargo test --lib measure_ -- --ignored --nocapture
```

| structure | setting | default | per unit | at that default |
|---|---|---:|---:|---:|
| idempotency receipts | `receipt_retention` | 100,000 | 274 B | **82.2 MB** across three replicas |
| metadata route cache | `metadata_route_cache_entries` | 100,000 | 144 B | **14.4 MB** |
| raft apply receipts | `k_max_raft_apply_receipts()` | 4,096 | 280 B | 1.15 MB |
| object change feed | `UNCONSUMED_OBJECT_CHANGE_RETENTION` | 1,024 | 146 B | 146 KB |
| metadata change log | `UNCHECKPOINTED_METADATA_CHANGE_RETENTION` | 1,024 | 56 B | 56 KB |
| read cache | `chunk_read_cache_max_bytes_per_replica` | — | — | the bound **plus 5.6%** |

A change feed's per-record figure is the one **at its cap**. A smaller store
reports more per record, because a vector that has grown by doubling carries
spare slots and the audit charges capacity rather than length: at 200 records
the feed measures 177 bytes a record, and at its cap of 1,024 it measures 146.

The read cache is the odd one: its setting bounds the cached payload, and the
entries, keys, checksums, by-chunk index and eviction queue beside it are
outside that bound. A replica configured to cache 1 GiB holds about 1.056 GiB,
and the overhead's share depends on how large the cached ranges are.

For scale, the metadata for 2,000 objects of 1 KiB is about 1.8 MB. A full
route cache is eight times that, and the receipt maps at their default are
considerably more.

The route cache is the one most likely to surprise, because it is keyed by
**metadata key** — one entry per object key resolved, not one per shard — so it
fills with the number of distinct objects a deployment serves rather than with
its topology. It was 598 bytes an entry until the entries for one shard were
made to share a single route; it is 168 now, which is the key text and the
entry's own fields. Its TTL (`metadata_route_cache_ttl_ms`, default 1000) makes
an entry stale after a second but does not free it: a burst of distinct keys
leaves the cache resident behind it.

```
cargo test --release --lib measure_what_a_filled_metadata_route_cache_costs -- --ignored --nocapture
```

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
| 250 | 2,485 B | 768,456 B |
| 1,000 | **2,485 B** | 3,072,456 B |

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
**21.5% larger** than when it held them:

| | bytes |
|---|---:|
| holding 500 objects | 1,961,205 |
| after deleting all 500 | 2,384,346 |

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

### The change feeds used to grow without bound

`Vec::drain` moves the tail down and keeps the allocation, so a change feed that
once reached a peak held those slots for the life of the process — the audit
caught 4,600 records sitting in a capacity of 8,192, and compacting the front
away returned none of it. Compaction now shrinks, which costs one copy per
compaction rather than one per append.

Compaction only helps a feed somebody reads, though: it drops the prefix every
registered consumer has passed, so with no consumer at all it declined to drop
anything and the feed grew for the life of the process. Both feeds now bound
that case at 1,024 records — the object change feed when no cursor is
registered, the metadata change log when no checkpoint is. A consumer that
registers later and asks for a trimmed prefix is told to resync, which is the
read path's existing contract after any compaction.
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

### Sizing `page_size` for your object size

`page_size` is the one option worth setting deliberately before you store small
objects, because a write's undo is currently the **page** it lands in rather than
the range it wrote. Measured per put, with a deterministic allocation counter:

```
undo bytes per put  ~=  replica_count  x  0.572  x  page_size
```

The 0.572 is a page's average occupancy as it fills, and the per-replica factor
came out identical at three and five replicas. At the defaults that is about
1.7 MiB copied to store a 1 KiB object. The effect on time, 4,000 puts of 1 KiB
at three replicas:

| `page_size` | put seconds |
|---:|---:|
| 1 MiB (default) | 8.84 |
| 256 KiB | 2.12 |
| 64 KiB | 0.70 |

So pick a `page_size` near the size of the objects you actually store. The
trade is that smaller pages mean more of them to track: allocation *count* rose
from 23,091,061 to 23,764,493 across that range, while bytes and time both fell
much further.

The largest single allocation a put makes says the same thing exactly, measured
with `scale_bench`'s per-phase allocation watermark:

```
put_largest_alloc_bytes  =  min(bytes already in the page, page_size)   per replica
```

| `page_size` | 400 objects | 800 | 1,600 |
|---:|---:|---:|---:|
| 256 KiB | 261,120 **at the bound** | 261,120 | 261,120 |
| 1 MiB | 408,576 | 818,176 | 1,047,552 **at the bound** |
| 4 MiB | 408,576 | 818,176 | 1,637,376 |

The copy grows with what the page already holds and stops at `page_size`, which
makes `page_size x replica_count` the ceiling on what one write copies — about
**3 MiB at the defaults**, to store a single 1 KiB object. Reproduce with
`cargo build --release --bin scale_bench` and
`./target/release/scale_bench 1024 400 1 65536`, reading
`put_largest_alloc_bytes` — not `alloc.largest_bytes`, which is process-wide and
reports the recovery snapshot.

Note which setting this is. `chunk_size` is the 64 MiB container **above** the
page and is not the one to reduce here: lowering it below `max_extent_bytes`
(4 MiB by default) is refused outright with
`max_extent_bytes must be 1..chunk_size`. The environment names the benchmark
reads are `MATRIX_OBJECTSTORE_PAGE_SIZE` and `MATRIX_OBJECTSTORE_CHUNK_SIZE`.

This is a property of the write undo, not of the storage layout, and it is
tracked as an issue rather than accepted as the design.

### Receipt retention costs something per mutation

Allocation, write, discard and repair carry receipts so a retried request is not
applied twice. Those receipts are held per replica, bounded by
`receipt_retention` (default 100,000), and a mutation's rollback snapshot copies
them, so **the cost of a mutation grows with the number of receipts being held**.

Measured over the same workload, changing only the bound:

| | retention 100,000 | retention 8 |
|---|---:|---:|
| total allocations | 4,351,882 | 2,703,037 |
| allocations per overwrite | 2,732 | 932 |
| allocations per delete | 5,527 | 911 |
| delete seconds | 0.096 | 0.016 |

A put is unaffected, because a put writes no discard receipt.

Set `receipt_retention` to cover the retry window you actually need rather than
leaving it at the default. The trade is that an evicted receipt no longer
deduplicates its retry.

It is not a `StoreOptions` field — it is configured on a live store, because
changing it is a control-plane change that each replica has to accept:

```rust
let (receipts_trimmed, replicas_configured) =
    store.configure_chunk_receipt_retention(8)?;
```

It returns how many receipts it trimmed and how many replicas it configured,
which is the only read-back: the page service's own `receipt_retention` is a
different number and that call does not move it.

`configure_chunk_replica_receipt_retention(replica_index, retention)` sets one
replica instead of all of them.
`the_documented_way_to_set_receipt_retention_works` exercises the call above and
checks the retained receipts actually fall, so the advice cannot drift from the
API that carries it out.

This is a property of what the rollback snapshot carries rather than of
receipts themselves, and it is tracked as an issue rather than accepted as the
design.

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

Every push and pull request runs two jobs: a build of all targets and the
documentation with warnings denied, and the two guards that protect properties
nothing else protects -- that the durable format constants have not changed, and
that a mutation which fails after it has already mutated leaves the store
byte-identical.

Two things CI deliberately does **not** run, because a check that is red on
arrival teaches people to ignore the ones that matter:

- **The full test suite.** 21 tests fail on `main`, and have done since the
  first commit. `tools/suite_gate.sh` runs it and compares the failures by name
  against `tools/known_failures.txt`, so you can tell a pre-existing failure
  from one you just caused. It fails if a failure is new **and** if a recorded
  one has started passing, so the list cannot quietly rot, and it refuses
  outright if the suite did not run rather than reporting everything fixed.
  Run `cargo test` and expect `250 passed; 21 failed`.
- **`cargo fmt --check`.** The tree has never been rustfmt-clean; there were 93
  diff hunks at the first commit. Reformatting 33,000 lines of tests to turn one
  check green is a change nobody asked for, and it would bury real history.

`Cargo.lock` is committed, because this crate ships a binary and CI builds with
`--locked` so that a dependency release cannot change what is being tested. The
toolchain is **pinned** for the same reason: "it builds" should mean the same
thing in CI and on your machine. Bumping either is a deliberate change.

If you fix one of the 21, or format the tree, add the corresponding check here
in the same change.

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
