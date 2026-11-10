# Publication allocation cost

The metadata client keeps the original manifest outcome through publication.
Reply validation borrows the expected manifest from that outcome. The object
writer, resume path and ordinary copy path recover the same owned outcome after
the call instead of cloning it before transmission. An uncertain reply still
returns the original before/after outcome; no publication is automatically
replayed. Exact readback confirmation and admission classification are unchanged.

This removes a full expected-manifest copy from ordinary and owned publication,
and removes a before/after outcome copy from the object writer's publication
call. Wire formats, quorum thresholds and persisted outcomes are unchanged.

## Local measurement

The explicit allocation workload publishes an append-shaped outcome with
4,095 old extents and 4,096 resulting extents. A loopback metadata responder
returns the expected manifest. Counting starts after client connection and
outcome construction, and counts only the calling client's thread. Server
allocations and preparation of the input are excluded. Bytes are cumulative
allocation requests, including reallocations, rather than peak live memory.

Measured on local WSL with the debug test profile:

| Publication | Baseline allocations | Updated allocations | Baseline bytes | Updated bytes |
|---|---:|---:|---:|---:|
| Ordinary, one resulting extent | 32 | 27 | 2,096 | 1,933 |
| Owned, one resulting extent | 34 | 29 | 2,425 | 2,262 |
| Ordinary, 4,096 resulting extents | 38,939 | 30,744 | 5,295,431 | 4,676,923 |
| Owned, 4,096 resulting extents | 23,921 | 15,726 | 4,578,354 | 3,959,846 |

The baseline is commit 84b0c91. At full width, both publication paths avoid
8,195 allocation calls and 618,508 allocated bytes. This workload calls the
public publication API directly; it does not measure the additional outcome-copy
removal in the full object writer. It also does not establish distributed
latency, CPU cost, network amplification or physical write amplification.

Reproduce with:

```sh
cargo test --all-features --test publication_allocations \
  -- --ignored --nocapture --test-threads=1
```

The workload is intentionally opt-in. Correctness gates separately exercise
successful, mismatched and lost replies for both publication modes; typed
admission refusals; exact readback confirmation; complete object-connection
tests; and Star/Raft process recovery.
