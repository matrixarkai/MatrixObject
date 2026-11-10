# Scheduled Star repair

`star_repaird` reads fixed Star routes from the metadata quorum and repairs
their chunk members in bounded inventory batches. It can run one named group or
discover all fixed Star groups and divide them across a bounded thread pool.
The worker starts a new scan after the full sweep cooldown, so a page skipped
because it lacked a source quorum is revisited.

```sh
MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS=127.0.0.1:17201,127.0.0.1:17202,127.0.0.1:17203 \
MATRIXOBJECT_REPAIR_GROUP=group-a \
cargo run --release --features metadata-consensus --bin star_repaird
```

For one process covering every fixed Star group in the current catalog:

```sh
MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS=127.0.0.1:17201,127.0.0.1:17202,127.0.0.1:17203 \
MATRIXOBJECT_REPAIR_ALL_GROUPS=1 MATRIXOBJECT_REPAIR_WORKERS=4 \
cargo run --release --features metadata-consensus --bin star_repaird
```

| Setting | Default | Bound |
|---|---:|---:|
| `MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS` | required | 1–7 distinct metadata API sockets |
| `MATRIXOBJECT_REPAIR_GROUP` | required in single-group mode | fixed Star group ID, 1–128 characters |
| `MATRIXOBJECT_REPAIR_ALL_GROUPS` | unset | set to 1 instead of GROUP to discover fixed Star groups |
| `MATRIXOBJECT_REPAIR_WORKERS` | 4 | 1–16 threads in all-groups mode |
| `MATRIXOBJECT_REPAIR_ACTIVE_GROUPS` | 32 | 1–128 connected groups per worker thread |
| `MATRIXOBJECT_REPAIR_BATCH_PAGES` | 16 | 1–64 pages |
| `MATRIXOBJECT_REPAIR_INTERVAL_MS` | 500 | 100–60,000 ms between batches and retries |
| `MATRIXOBJECT_REPAIR_SWEEP_INTERVAL_MS` | 300,000 | 1,000–86,400,000 ms after a completed sweep |
| `MATRIXOBJECT_REPAIR_SCRUB_INTERVAL_SECS` | 86,400 | 3,600–604,800 seconds between completed payload scrubs |

`MATRIXOBJECT_CLIENT` API TLS settings apply to both metadata and chunk
connections. The worker checks the metadata route before each batch and starts
at the beginning when membership changes. Every configured chunk member must
answer its inventory before a batch can mutate a page. A missing member or
metadata quorum causes a logged retry. Routine sweeps compare up to eight
complete page heads per request on every member. Older members use individual
head requests. Matching heads avoid payload transfer; differing or missing
heads enter the validated repair path. Routine matches report `HeadMatched`
rather than the byte-validated `Current` status. A scheduled scrub reads and
validates page payloads on every member. Its timer restarts after completion.
All-groups mode discovers group IDs at startup and shares one metadata
connection per worker thread. Restart the process after changing the catalog
inventory. Each thread retains at most `MATRIXOBJECT_REPAIR_ACTIVE_GROUPS`
groups' chunk connections, including the group currently executing a batch.
On a cache miss at the limit, the least recently used connections close before
replacement sockets open. A later batch reconnects using the freshly checked
metadata route. Eviction preserves the inventory cursor, sweep counters and
scrub state; a changed route resets the cursor. Stopped groups remain stopped
and cannot be restarted by a cache miss.

For fixed three-member groups, the default four workers retain at most 384
chunk connections plus one metadata connection per worker. Actual counts depend
on group membership and successful connections. Plaintext clients duplicate each
socket's read and write handles, so allow two file descriptors per connection.
Reduce the active-group budget
when open-file limits are tighter; increasing it can reduce reconnect work when
many groups need frequent batches. Thread count still bounds concurrent batches.
Repeated connection failures back off from the batch interval to at most 30
seconds. Healthy batches report one summary at the end of the sweep.

An unresolved page is skipped only for the current sweep, allowing later pages
to make progress. A response that leaves a repair commit unconfirmed stops its
group; inspect that destination before restarting repair for the group. In
single-group mode the process exits. In all-groups mode other groups continue,
and the process exits if every group stops. The worker does not
certify a consistent cluster snapshot, admit maintenance nodes for placement,
or reclaim old pages. Live writes can change pages already scanned, so the
next sweep revisits them. Payload scrubs can consume substantial network and
disk bandwidth; set the batch and interval limits for the available repair
budget. Routine sweeps cannot detect silent payload damage when every member
still reports the same stored head; the scheduled payload scrub handles that
case.

The local process test uses three metadata voters and three journal-backed
chunk replicas. It leaves one page without a source quorum, repairs later
pages, loses the metadata leader, completes the blocked page after a source
returns, then writes one more page while a chunk replica is stopped. The worker
repairs that missed write after restart. The explicit scale run covers 257
pages and verifies 771 persisted copies:

```sh
cargo test --features chunk-consensus,metadata-consensus \
  --test network_metadata_consensus \
  star_auto_repair::worker_repairs_256_pages_across_metadata_failover_and_chunk_restart \
  -- --exact --ignored --nocapture --test-threads=1
```

The all-groups process test runs three metadata voters and 24 chunk servers,
repairs four pages per group with one connected group per worker, loses the
metadata leader during traversal, then verifies 96 copies after chunk restart.
Every worker rotates between groups, exercising cursor preservation across
connection eviction:

```sh
cargo test --features chunk-consensus,metadata-consensus \
  --test network_metadata_consensus \
  star_auto_repair::worker_repairs_eight_star_groups_across_metadata_failover \
  -- --exact --ignored --nocapture --test-threads=1
```
