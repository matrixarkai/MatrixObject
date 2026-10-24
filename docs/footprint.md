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
| 4 KiB | 4,000 | **3.11x** | 3.00x |
| 1 KiB | 8,000 | **3.44x** | 3.02x |

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
| metadata per object | 618 B | 574 B | 451 B | **430 B** |

Both change feeds cap at 1,024 records, so together they are 30.0% of metadata
at a hundred objects and 4.1% at eight thousand. Everything else is exactly
linear, which makes the total a line once the feeds are full:

```
metadata = 410 bytes an object + 164,256 bytes fixed      (remainder: zero)
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
| inside `objects` | 62 | the content-type and storage-class text, shared by every object |
| inside `meta.records` | 108 | one record more than there are objects — the bucket's |
| `meta.entities` | 84 | |
| `chunks` | 104 | |
| payload index | 32 | the chunk identity, paid once per chunk rather than once per entry |
| **fixed total** | **164,256** | |

`every_metadata_leg_is_a_line_and_the_intercepts_are_named` fits each leg over
three store sizes and fails unless every one is a line and the fixed terms sum
to the total's — so a leg that starts depending on the object count, or a fixed
byte that stops being named, is caught rather than absorbed.

That 410 is two items and a rounding error:

| | per object | share |
|---|---:|---:|
| `objects` — the structured metadata | 210 B | 51.2% |
| `meta.records` — the metadata projection record | 192 B | 46.8% |
| payload index keys | 8 B | 2.0% |

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

### What the 410 bytes are, and what each remaining part would cost to remove

The slope is not one thing. Every leg below is fitted from the same two object counts the
published line is fitted from, and they sum to it exactly:

```
METADATA LINE PARTS
objects.mapkey_inline          32  two shared-text holders, which is what keys the map
objects.mapkey_heap            42  the key's characters: ONE allocation, shared with the value
objects.metadata_inline       136  ObjectMetadata itself, one extent held inline
objects.extents_bytes           0  the one extent is INLINE above, so it owns no heap
meta.records.struct_inline     72  the map key's holder, plus MetadataRecord
meta.records.mapkey_capacity   56  the record key's characters: ONE allocation, shared
meta.records.value_capacity    64  the projection, its three holds behind a pointer
payloads.key_bytes              8  the payload index's key
METADATA LINE TOTAL          410
```

`the_published_metadata_decomposition_sums_to_the_line` measures each one and fails if this
block disagrees with the tree or if the parts stop adding up.

**Two of these are already one allocation each, and that is load-bearing rather than
incidental.** Both maps name the identity twice -- once as the map key and once inside the
value -- and both times the two holders share. `objects.inner_id_heap` and
`meta.records.record_key_capacity` therefore read **zero**, and
`the_identity_each_map_names_twice_is_one_allocation` pins it: unsharing either would add
back the characters plus a 16-byte header for every object, silently, with no test failing
on the behaviour.

The difference between `mapkey_capacity` and `mapkey_len` is not slack to reclaim. Shared
text has no spare capacity; the difference is the strong and weak counts in front of the
characters, sixteen bytes once per allocation.

#### The reductions that are left, and what each one costs

Each is priced against the 410. Two rows have come **off** this list, and both came off the
same way -- by being measured instead of argued. Narrowing an extent's offset and length to 32
bits was priced at 8 bytes an object and a migration, until the migration turned out not to
exist: the snapshot still writes two `u64` and only the in-memory form is narrow. Interning
content types was priced at "about twenty-four bytes" behind an unbounded table, and it is
already done -- the section after the table has the measurement. What is left is one design
decision and one representation.

| | saves | what it costs |
|---|---:|---|
| The metadata records restate the objects map | **216** | It is the whole of `meta.records`: a separate metadata service's own store, with its own versioning, deletion markers and change log, modelled in this process. Removing the restatement means collapsing two services into one, which is a design decision and not a footprint one. |
| A one-word text handle | **48**, or **32** without unsafe code | `SharedText` is `Arc<str>`, a fat pointer, so every holder spends two words to name one allocation. Eight holders an object carry it -- two keying `objects`, three inside `ObjectMetadata`, two around a metadata record and one in the projection -- so one word each is **64 bytes of inline saving**, measured by construction rather than multiplied, because a narrowed field can leave its padding behind instead of closing up. The allocation pays part of it back by carrying the length the handle stops carrying: one word per allocation, two allocations an object, **net 48**. The shape that needs no unsafe code is `Arc<Box<str>>`, which puts the length in a second allocation instead and nets **32**. Both add a dependent load to a string read, and THAT LOAD IS NOW PRICED. Measured in release over 200,000 names visited in a shuffled order -- shuffled because a constant stride is the pattern a prefetcher exists to hide -- the thin handle costs **2.6x to 3.1x on a length read** and **1.1x to 1.3x on a character read**, four to nine nanoseconds either way. The length case is the whole story: `Arc<str>` carries the length IN THE HANDLE and touches no heap at all, where `Arc<Box<str>>` must reach the box first. The comparison favours the thin handle, whose index is half the size and so has the better locality, and it still loses. `measure_what_a_one_word_text_handle_costs_to_read` prints it. So the trade is 32 bytes an object -- 7.5% of the metadata line -- against roughly tripling the cost of asking a name its length, and the saving is largest for the small objects that do the most string reads per stored byte. |

**Both figures above are measurements now, and the difference has mattered three times.**
The extent row was priced at 8 bytes and a migration, and the migration turned out not to
exist. The storage-class row was priced at "an unbounded intern table", and the set turned out
to be closed to four names. The handle row was priced at "about forty", which is neither of
the two numbers it actually has. All three were settled by MEASURING THE PROPOSED LAYOUT BY
CONSTRUCTION -- a hand-built mirror of the struct, sized by the compiler, with the unchanged
mirror as a control -- before touching any code.
`measure_what_a_one_word_text_handle_would_save` does it for the handle, and it asserts every
control equal to the type it mirrors before reading a saving off it: a mirror that has drifted
from its struct measures nothing, and nothing else in the suite would notice.

**Interning content types, which is already done.** It sat on this list reading "about
twenty-four bytes, estimated", behind an objection that a content type is arbitrary input, so
its table would be unbounded and need eviction -- a cache, with a cache's failure modes, in the
middle of object metadata. The table exists, and it is bounded by a COUNT rather than by
eviction: `MAX_SHARED_CONTENT_TYPES` is 256, and past the bound an object owns its own string,
which is what every object did before. So the behaviour degrades instead of failing and there
is no cache to go wrong. `objects_of_one_content_type_name_it_through_one_allocation` and
`past_the_bound_an_object_owns_its_content_type` pin the two halves.

What it saves is **larger** than the estimate it replaced. `objects.content_type_heap` is a
constant **40 bytes** -- one 24-character allocation and its two counts -- at every object
count from 600 to 8,000, where an unshared string would have cost those same 40 bytes **an
object**. So the row was stale and under-priced at once.

That it is measurable at all is new. The measurement leg put every object under one content
type, so it could not vary the one thing interning acts on, and the saving had to be argued
from the code instead of read off the audit. `MEASURE_CONTENT_TYPES` varies it, and the leg
moves with it -- 2,000 objects, 1 KiB each:

| distinct content types | `objects.content_type_heap` | per object |
|---:|---:|---:|
| 1 | 40 | 0.02 B |
| 2 | 62 | 0.03 B |
| 256 — the bound | 8,338 | 4.2 B |
| 2,000 — past it | 66,890 | 33.4 B |

So the saving is **40 bytes an object** on a corpus whose types repeat, and **nothing** on one
where every object carries a type of its own: past the bound each object owns its string, which
is the row's last line and now a number rather than a claim. A deployment with more than 256
live content types gets no sharing, and that is the case to size for.

```
MEASURE_OBJECTS=2000 MEASURE_CONTENT_TYPES=2000 cargo test --release --lib \
  measure_the_metadata_footprint -- --ignored --nocapture
```

Its default is 1, which is the shape every figure in this document was measured at.

**The storage class, which came off before it.** `validate_storage_class` accepts exactly
STANDARD, ARCHIVE, GLACIER and DEEP_ARCHIVE, so a `SharedText` was spending sixteen bytes to
say one of four things. As a one-byte enum the byte lands in padding both structures already
had: `ObjectMetadata` 144 to 128 and `ObjectProjection` 88 to 72, **32 bytes an object**. The
snapshot still writes the same four names, so there was no migration here either, and the
state it removes -- a class outside the set -- is one the store's own validation already
refused.

**The extent, which came off before that.** An extent's offset is a position
inside one chunk and its length is bounded by `max_extent_bytes`, which is already validated
as `1..=chunk_size`, so both fit in 32 bits for any chunk under 4 GiB. `StoreOptions` now
refuses a `chunk_size` at or above that bound rather than letting an extent wrap, and that
refusal is the whole price: a setting gained a documented ceiling 64 times above its default.

The fields are **private**, with `offset()`, `length()` and `end()` returning `u64`. A retype
alone would have left `extent.offset + extent.length` compiling as `u32 + u32` and overflowing
where it had been safe; private fields made all 101 read sites a compile error and keep every
piece of arithmetic in 64 bits.

Inlining the single extent is still **not** on the list, because it is not a saving: an enum of
one extent or many is 40 bytes inline against the 16 a boxed slice costs, so a one-extent object
saves 8 and a two-extent object loses 24. It depends on the object, which means it depends on
the workload, which is not a property a representation should have.

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

