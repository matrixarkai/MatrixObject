# Star append journal and traffic cost

The public owned-write workflow appends fresh pages to an immutable prefix.
This workload measures one such append with three MatrixRaft metadata voters
and a three-member journal-backed Star data group on local WSL. The routed
threshold remains N=3, W=3, R=1.

## Measurement window

Each case creates and publishes its prefix before measurement. Publication
fencing is enabled. The measured append starts by preparing an owned namespace
request, writing its identity to a file and syncing that file before submission.
Only an explicit consumed-position refusal allows a new prepared request.
Unconfirmed allocation or publication fails the run without replay.

The window includes the cursor query, reservation, staged-record check, write
planning, suffix data writes and publication. Metadata API meters forward the
original complete frames and count both directions across the pinned voter
catalog. They separately count staged publication frames. The observed
publication frame length must equal the length of the client's encoded request,
including its six-byte transport header.

Metadata WAL growth sums observed logical file growth over all three voters
after a short stability check. Data journal growth sums all three chunk journals.
Application and data checkpoints are disabled. Prepared-request files, filesystem
metadata, device writes and peer network traffic are excluded from those counts.
Data network traffic is not measured.

The suffix must be readable with the committed sequence on every data replica
after acknowledgement. Data journal growth is bounded independently of prefix
size on each node. The old extents must remain unchanged. After every case,
a normal fenced object read verifies the complete result. These verification
reads occur outside the measured window.

All six services are then stopped and restarted through their existing durable
files. Every page of every committed object is read and checked independently on
every data replica, followed by the ordinary object API read. Those recovered
data reads must leave the journals unchanged. Staged allocations and retained
reader handles must be empty at completion.

## Local release sample

Storage code: 61936669, release profile with all features. Prefix pages are
4 KiB and the suffix is exactly one 4-KiB page in every case. The largest append
reaches the 4,096-extent manifest limit. There is one publication attempt per case.
All byte values below include replication where the column describes journals.

| Prefix extents | Prefix bytes | Metadata request bytes | Metadata reply bytes | Publication frame bytes | Metadata WAL growth | Data journal growth |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4,096 | 426 | 734 | 279 | 2,502 | 12,552 |
| 128 | 524,288 | 18,713 | 19,020 | 18,566 | 57,390 | 12,552 |
| 4,095 | 16,773,120 | 605,066 | 605,373 | 604,919 | 1,816,467 | 12,552 |

The data journal total stays at 12,552 bytes. The largest metadata publication
frame is 604,919 bytes; its ManifestOutcome body is 604,903 bytes. That body
contains complete before and after manifests. The reservation is bounded, but
publication, its acknowledgement and the write-plan response still carry prefix
metadata. These sizes explain why writing only suffix payloads does not establish
bounded metadata amplification.

These are observed logical journal and API frame bytes. Proxying and decoding
frames add measurement overhead, prepared-request persistence is part of elapsed
time, and the host is shared. The emitted elapsed time is not a native latency
benchmark. Physical device write amplification, CPU, allocations, checkpoint
interference and repair interference require separate measurements.

## Compact publication release sample

The opt-in [compact staged append protocol](compact-staged-append.md) binds the
prior generation and canonical digest, immutable suffix and resulting digest.
It persists a version 11 binary protobuf decision and acknowledges the committed
allocation and digest. All metadata voters and recovery readers must be upgraded
before activation. Existing write methods continue to use full publication.

The same local WSL release workload, with the same 4-KiB pages and three-member
durability thresholds, produced the following observed bytes. Every case had one
publication attempt and passed complete six-process recovery and replica reads.

| Prefix extents | Metadata request bytes | Metadata reply bytes | Publication frame bytes | Metadata WAL growth | Data journal growth |
|---:|---:|---:|---:|---:|---:|
| 1 | 330 | 604 | 183 | 2,226 | 12,552 |
| 128 | 334 | 9,747 | 187 | 2,256 | 12,552 |
| 4,095 | 337 | 302,923 | 190 | 2,283 | 12,552 |

At 4,095 prior extents, publication falls from 604,919 to 190 bytes and total
metadata WAL growth from 1,816,467 to 2,283 bytes. Small bounded size differences
between rows come from encoded integers and record lengths. Data writes retain
the same durable suffix cost. These counts exclude peer traffic and physical
filesystem/device amplification, as in the original measurement window.

Planning still returns the complete prior manifest, accounting for the growing
reply column. Prefix hashing and catalog preparation still scale with extent
count. Compact planning and catalog work remain the next implementation; this
measurement does not establish constant append CPU, allocations, total network
traffic or improved native latency.

## Reproduce

```sh
cargo test --release --locked --all-features --test network_metadata_consensus \
  star_append_amplification::measure_one_star_append_as_prefix_grows \
  -- --exact --ignored --nocapture --test-threads=1
```

Require one passed test and three STAR_APPEND_COST rows. The short CI case uses
128-byte pages and prefixes of one and 32 pages:

```sh
cargo test --locked --all-features --test network_metadata_consensus \
  star_append_amplification -- --test-threads=1 --nocapture
```

Require one passed test and one ignored measurement. The bounded meter accepts
at most 64 connections per voter endpoint and frames at most 8 MiB. All meter
sockets, threads, service processes and temporary files belong to the fixture
and are shut down or removed on success and failure.
