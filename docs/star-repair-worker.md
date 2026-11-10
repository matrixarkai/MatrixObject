# Scheduled Star repair

`star_repaird` reads one fixed Star group route from the metadata quorum and
repairs its chunk members in bounded inventory batches. Run one worker for each
group that needs scheduled repair. The worker starts a new scan after the full
sweep cooldown, so a page skipped because it lacked a source quorum is revisited.

```sh
MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS=127.0.0.1:17201,127.0.0.1:17202,127.0.0.1:17203 \
MATRIXOBJECT_REPAIR_GROUP=group-a \
cargo run --release --features metadata-consensus --bin star_repaird
```

| Setting | Default | Bound |
|---|---:|---:|
| `MATRIXOBJECT_REPAIR_METADATA_ENDPOINTS` | required | 1–7 distinct metadata API sockets |
| `MATRIXOBJECT_REPAIR_GROUP` | required | fixed Star group ID, 1–128 characters |
| `MATRIXOBJECT_REPAIR_BATCH_PAGES` | 16 | 1–64 pages |
| `MATRIXOBJECT_REPAIR_INTERVAL_MS` | 500 | 100–60,000 ms between batches and retries |
| `MATRIXOBJECT_REPAIR_SWEEP_INTERVAL_MS` | 300,000 | 1,000–86,400,000 ms after a completed sweep |

`MATRIXOBJECT_CLIENT` API TLS settings apply to both metadata and chunk
connections. The worker checks the metadata route before each batch and starts
at the beginning when membership changes. Every configured chunk member must
answer its inventory before a batch can mutate a page. A missing member or
metadata quorum causes a logged retry. Per-page source and destination checks
remain those of `RemoteReplicaSet::repair_chunk`.
Repeated connection failures back off from the batch interval to at most 30
seconds. Healthy batches report one summary at the end of the sweep.

An unresolved page is skipped only for the current sweep, allowing later pages
to make progress. A response that leaves a repair commit unconfirmed stops the
worker; inspect that destination before restarting it. The worker does not
certify a consistent cluster snapshot, admit maintenance nodes for placement,
or reclaim old pages. Live writes can change pages already scanned, so the
next sweep revisits them. The full scan reads and validates page data and can
consume substantial network and disk bandwidth; set the batch and interval
limits for the available repair budget.

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
