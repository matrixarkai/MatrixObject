# Configuring MatrixObject

The README carries the options table and the one-line advice. This is the
working behind it: how to pick `page_size` for your object size, and what
receipt retention costs per mutation.

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

