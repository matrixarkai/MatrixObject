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
  first commit. They are not regressions and they are not masked: the local gate
  compares failures by name against that baseline and refuses anything new.
  Run `cargo test` and expect `249 passed; 21 failed`.
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
