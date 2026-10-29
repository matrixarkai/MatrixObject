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



## Standalone journal checkpoints

On Unix, a chunk process using `MATRIXOBJECT_REPLICA_JOURNAL` can enable
`MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES` with an interval from 1,024 through
536,870,912 bytes. It is disabled when absent. It requires standalone journal
storage and cannot be combined with Raft storage or an in-memory process.

After syncing the outcome that crosses the interval, the node captures a complete
service image and streams it to `<journal>.pending`. It syncs that candidate,
atomically replaces the journal, then syncs the parent directory before replying.
The stable `<journal>.lock` stays locked across replacement. Only outcome bytes
after the last checkpoint count toward the next interval. An interrupted candidate
is ignored on recovery and overwritten by the next checkpoint. Keep the journal
and its lock file together; the lock file also records initialization so a missing
or empty initialized journal is refused. Do not delete that file during operation.

The `MOSJ` envelope contains a version, stream length and checksums, followed by
the version-2 binary protobuf stream in `proto/chunk_checkpoint.proto`. Its index
and term fields are fixed at one for a standalone image; they do not denote Raft
membership. Complete page data, discarded ranges, sequences, receipts and counters
are preserved. Later mutations remain binary page outcomes. The stream supports
pages through 256 MiB and complete images through 16 GiB without constructing one
contiguous image buffer. Recovery checks configured capacity and page-count limits;
the capacity must match the saved image. Legacy journals remain readable.

Upgrade every process that may open a journal before enabling checkpoints. Older
binaries cannot read the checkpoint envelope and do not participate in the stable
lock. Checkpoint creation is Unix-only; recovery uses the shared image decoder.
Checkpointing is synchronous under the replica lock, so it can delay reads, writes
and health reports. Budget disk space for the current journal plus one pending
image. A write may be durable even when checkpoint failure loses its reply: the
node stops serving after such failure and the caller must treat the write as
uncertain. A restart recovers the synced outcome. This does not repair missed
writes on another star replica or detect removal of an entire valid journal suffix.

The regular guards exercise three-node star restart, later writes, disk failure,
locks, corruption and incomplete candidates. Run the larger checks separately:

```sh
cargo test --release --locked --all-features --lib cluster::replica_journal::checkpoint_tests::standalone_journal_streams_large_pages_and_more_than_256_mib -- --exact --ignored --nocapture
cargo test --release --locked --all-features --test replica_restart_recovery interrupted_journal_checkpoint_preserves_synced_outcomes_and_reclaims_on_retry -- --exact --ignored --nocapture
```


## Bounded page-state reads

Updated chunk clients can read complete page state with `read_chunk_state`.
Request code 10 uses the canonical binary protobuf schema in
`proto/chunk_state_range.proto`. Each response contains at most 1 MiB of raw
page bytes; pages are bounded to 256 MiB. Every range must match the complete
head checksum selected before transfer. Concurrent writes or discards cause
refusal instead of combining page versions. The client checks the assembled
payload checksum, logical length, sequence and discarded ranges. Raw state
includes bytes covered by logical discards; ordinary reads still refuse those
ranges. Derived redundancy is omitted and can be rebuilt during installation.

A star client requires validated, identical bytes from a read quorum and checks
the head quorum again before returning. It retains at most two assembled page
payloads, in addition to frame buffers and metadata. Raft nodes confirm read
leadership for these requests without adding journal records. Catalog clients
can route reads after leader loss; minority nodes refuse them.

Upgrade serving chunk nodes before using the new operation. There is no legacy
fallback or automatic restart of a changed transfer. This is a read operation,
not a remote installation or missed-write repair API. Checksums detect accidental
corruption; they do not authenticate peers. Maximum-page validation is available
with:

```sh
cargo test --release --locked --all-features --test replica_state_transfer page_state_transfer_reads_a_full_256_mib_page -- --exact --ignored --nocapture
```


## Staged standalone page installation

`RemoteReplicaClient::install_chunk_state` installs one already validated source
page on a standalone replica. The caller supplies a unique token and the expected
destination head checksum, or `None` to require absence. The binary protobuf
protocol is code 11 in `proto/chunk_state_install.proto`: begin, ordered append,
commit and abort. Low-level commands are available through `stage_chunk_state`.
No command is automatically replayed after an uncertain reply. Raft nodes refuse
this operation; their state changes must pass through consensus.

Each node permits one staging buffer through 256 MiB, with append data bounded to
1 MiB and source discard metadata bounded to 4,096 ranges. This buffer is separate
from committed byte capacity. Begin checks byte/page capacity and the destination
checksum. Commit checks capacity and the destination again, validates complete
source bytes and metadata, and refuses sources older than the destination. Equal
sequence replacement is permitted only with the exact expected destination
checksum. Partial state remains invisible. Wrong tokens and out-of-order appends
are refused; commit consumes the stage even when validation fails. A stage idle
for 60 seconds is discarded when the next request arrives; an entirely idle node
can retain its buffer until a request or process exit. Tokens are identifiers,
not authentication, and must never be reused for another operation.

In-memory nodes publish the validated candidate atomically under their service
lock. Unix journal nodes stream and sync a complete replacement checkpoint,
rename it and sync the directory before publishing in memory and acknowledging.
This works for pages larger than the normal outcome-record bound. It writes a
complete node image and can delay other requests and health reports. Budget disk
space for the existing journal plus the replacement image, and memory for the
staged page, candidate redundancy and existing state. Durable installation on
non-Unix nodes is refused.

Upgrade every process that may open the journal before using installation: this
operation writes the MOSJ checkpoint format even when periodic checkpointing is
disabled. Later normal writes still persist binary outcomes. A checkpoint failure
fences serving until restart. An error can mean the installation became durable
without its acknowledgement; reconnect and compare the complete head and payload
before recovery action. A process restart loses incomplete staging but preserves
acknowledged installations and subsequent writes.

This transport does not select a source quorum or schedule repairs. The caller
must establish source authority before beginning an install. It does not provide
cross-client transactions, authenticated peers or automatic missed-write repair.
Run the maximum-page recovery check separately:

```sh
cargo test --release --locked --all-features --test replica_restart_recovery staged_install_recovers_a_full_256_mib_page -- --exact --ignored --nocapture
```
