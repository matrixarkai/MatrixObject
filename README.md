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
| **slab** | 64 MiB | the append-only container objects are allocated from |
| **chunk** | 1 MiB | the replicated unit: what a quorum votes on and what repair rebuilds |
| **fragment** | derived | the pieces an erasure code splits one chunk into, plus its redundancy piece |

An object becomes one or more **extents** — a `(slab, offset, length)` range,
each at most `max_extent_bytes` — and an extent maps onto the chunks covering
it by `chunk_index = absolute_offset / chunk_size`. So a ranged read fetches
only the chunks it needs instead of scanning a slab, and a repair rebuilds one
chunk rather than a whole container.

Two words are used deliberately narrowly:

- **shard** means the distributed sense only: metadata partitioned by key range
  across services. The pieces inside a single chunk are **fragments**, because
  they never leave the chunk they came from.
- **page** names the service that coordinates replicas, not a unit of storage.

The file tree now matches those types: `src/services/page`, `chunk_page`,
`SlabId`, `slab_size`. What is still spelled the old way is spelled that way
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
- **Erasure coding.** A chunk with no peers keeps a redundancy fragment, so a
  partial corruption is still recoverable from the single copy it has.
- **Space reuse.** Overwriting an object normally leaves its previous version
  resident until collection. With `reuse_stale_slab_space` the next write takes
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

- **`byte_buffers`** is payload buffers, replica data, shards and checksums. An
  amplification derived from it alone is **data-only**, because everything else
  the store holds used to be reported as an entry count and never converted to
  bytes.
- **`total_bytes`** adds the metadata the store keeps to describe that data, and
  is the one to divide by `logical.written`.

Measured at three replicas, 26-character keys, with the metadata counted:

| object size | objects | amplification | data-only figure |
|---:|---:|---:|---:|
| 64 KiB | 1,000 | **3.02x** | 3.00x |
| 4 KiB | 4,000 | **3.20x** | 3.00x |
| 1 KiB | 8,000 | **3.76x** | 3.02x |

Reproduce any row with the measurement harness:

```
MEASURE_OBJECTS=8000 MEASURE_SIZE=1024 cargo test --release --lib \
  measure_the_metadata_footprint -- --ignored --nocapture
```

### Metadata is per object, and the per-object figure depends on how many there are

At a fixed object count the metadata total is **identical** whatever the objects
contain: 3,155,478 bytes for 4,000 objects at both 1 KiB and 4 KiB. So it
disappears into a large object and dominates a small one, which is the whole
reason the two totals are reported separately.

The per-object figure falls as the store grows, because two of the structures
are bounded rather than per object:

| objects | 100 | 1,000 | 4,000 | 8,000 |
|---:|---:|---:|---:|---:|
| metadata per object | 1,005 B | 956 B | 789 B | **761 B** |

Both change feeds cap at 1,024 records, so together they are 26.8% of metadata
at a hundred objects and 3.7% at eight thousand. Everything else is exactly
linear, which gives an asymptote of **733 bytes per object** plus 223,232 bytes
of capped feeds spread across however many objects there are.

That 733 splits almost exactly in half:

| | per object | share |
|---|---:|---:|
| `meta.records` — the metadata projection record | 363 B | 49.5% |
| `objects` — the structured metadata | 346 B | 47.2% |
| payload index keys | 24 B | 3.3% |

The largest single item inside either half is the projection record's **value**,
at 227 bytes per object — **31% of everything the store holds per object** — of
which 151 bytes are field names that nothing parses. Issue #40 has the
measurement and what closing it would require.

So if you store small objects, three replicas do not cost 3x. Any amplification
figure published for this store before the metadata was counted was the
data-only column, including in this file; and any per-object figure quoted
without its object count was measured at a hundred objects, where the bounded
feeds have not yet amortised.

Neither total includes B-tree node slack or allocator overhead, which is what
the remainder against process RSS is. Capacity is counted rather than length,
because the slack is resident and showing slack is what the audit is for. Text
shared between holders is charged once per allocation — the characters and the
two counts in front of them — so `mapkey_capacity` against `mapkey_len` is that
header rather than slack.

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
| `slab_size` | 64 MiB | how large a slab grows before a new one is opened |
| `chunk_size` | 1 MiB | the replicated unit a write lands in |
| `max_extent_bytes` | 4 MiB | the largest contiguous range one extent may cover |
| `replica_count` | 3 | copies of each chunk |
| `write_quorum` | 2 | replicas that must accept a write |
| `reuse_stale_slab_space` | false | take the space a superseded object left instead of growing the slab |

### Sizing `chunk_size` for your object size

`chunk_size` is the one option worth setting deliberately before you store small
objects, because a write's undo is currently the chunk it lands in rather than
the range it wrote. Measured per put, with a deterministic allocation counter:

```
undo bytes per put  ~=  replica_count  x  0.572  x  chunk_size
```

The 0.572 is a chunk's average occupancy as it fills, and the per-replica factor
came out identical at three and five replicas. At the defaults that is about
1.7 MiB copied to store a 1 KiB object. The effect on time, 4,000 puts of 1 KiB
at three replicas:

| `chunk_size` | put seconds |
|---:|---:|
| 1 MiB (default) | 8.84 |
| 256 KiB | 2.12 |
| 64 KiB | 0.70 |

So pick a `chunk_size` near the size of the objects you actually store. The
trade is that smaller chunks mean more of them to track: allocation *count* rose
from 23,091,061 to 23,764,493 across that range, while bytes and time both fell
much further.

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
