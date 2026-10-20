# What MatrixObject's footprint is, and how it was measured

Every figure here is produced by a measurement in the test suite, named
beside it, so a reader can re-run it rather than take it. The README keeps
the headline tables; this is the working.

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
| 4 KiB | 4,000 | **3.13x** | 3.00x |
| 1 KiB | 8,000 | **3.50x** | 3.02x |

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
| metadata per object | 683 B | 638 B | 515 B | **494 B** |

Both change feeds cap at 1,024 records, so together they are 30.0% of metadata
at a hundred objects and 4.1% at eight thousand. Everything else is exactly
linear, which makes the total a line once the feeds are full:

```
metadata = 474 bytes an object + 164,304 bytes fixed      (remainder: zero)
```

reproducing every total above exactly, at every count where the feeds have
reached their bound. **Below 513 objects they have not**: the feed's vector grows
by doubling and is charged at its capacity, so a store of 100 objects sits above
the line rather than on it, and so does the 710 in that table.

The fixed term is measured rather than derived, which matters because deriving it
from the two feeds alone gives 163,866 and loses 438 bytes that are not in the
feeds at all:

| | bytes | |
|---|---:|---|
| object change feed | 106,522 | 1,024 records at its bound |
| metadata change log | 57,344 | 1,024 records at its bound |
| inside `objects` | 86 | the content-type and storage-class text, shared by every object |
| inside `meta.records` | 108 | one record more than there are objects — the bucket's |
| `meta.entities` | 84 | |
| `chunks` | 120 | |
| payload index | 40 | the chunk identity, paid once per chunk rather than once per entry |
| **fixed total** | **164,304** | |

`every_metadata_leg_is_a_line_and_the_intercepts_are_named` fits each leg over
three store sizes and fails unless every one is a line and the fixed terms sum
to the total's — so a leg that starts depending on the object count, or a fixed
byte that stops being named, is caught rather than absorbed.

That 474 is two items and a rounding error:

| | per object | share |
|---|---:|---:|
| `objects` — the structured metadata | 250 B | 52.7% |
| `meta.records` — the metadata projection record | 216 B | 45.6% |
| payload index keys | 8 B | 1.7% |

Those two halves hold the **same facts twice**. `objects` is a binary struct —
144 bytes inline, with the object id, length, etag, content type, storage class
and extent list in fixed-width fields. `meta.records` is a projection of the same
object: it **spells** 227 characters and **holds** an 88-byte struct of twelve
values, rendering the text on demand.

That distinction is the whole of the saving the record has already taken, and it
is also why the format question below is still open. The text is durable — three
proofs hash it — so what the record spells cannot change without a migration,
whatever it stores. Measured against what those characters actually distinguish:

| | per record | share | |
|---|---:|---:|---|
| field names and separators | 151 B | 70% | `retention_until_ms`, `metadata_count`, … spelled out for every object |
| values that never differ | 24 B | 11% | 9 of the 12 fields have **one** distinct value across every object |
| values that differ | 42 B | 19% | etag, length, content type |

So **175 of 217 characters carry no information that varies between objects** —
81% of the text the record spells. Issue #40 has the measurement and what closing
it would require; it is a durable format, so the cost is a migration rather than
an edit.

What the record *stores* no longer pays for those characters: 88 bytes of struct
against 227 of text is the conversion already made, and it is free in the worst
case — a record decoded from a snapshot carries characters, so it holds exactly
what it spells and never more.

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

