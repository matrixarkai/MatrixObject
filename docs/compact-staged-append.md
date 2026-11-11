# Compact staged append publication

`RemoteMetaClient::append_staged_object_compact` writes a nonempty suffix using
an existing append allocation and publishes a digest-bound metadata decision.
`publish_staged_append_compact` exposes the publication step for applications
that already own and verify their staged pages. Existing write and recovery
methods keep their existing protocol.

Upgrade **every metadata voter, recovery binary and journal reader** before
calling either method. The opt-in request is opcode 93, version 4, action 11.
Its acknowledgement is version 3 and contains only the committed allocation ID
and resulting manifest SHA-256. The durable binary protobuf placement outcome
uses version 11 and field 14. An older reader cannot replay this outcome;
downgrading after activation is unsupported. There is no automatic fallback to
full publication and no automatic replay after an uncertain response.

The decision contains the exact prior generation, length, extent count and
canonical manifest SHA-256, the immutable suffix, and the resulting length and
SHA-256. Publication requires live staged ownership, the unchanged prefix,
fresh pages in the owned namespace, a live lease, configured groups and the
existing capacity and publication-fence checks. The allocation is consumed
atomically with publication. Replay checks the recorded decision against the
current catalog without reevaluating wall-clock expiry.

The outcome expands against the current manifest during validation and replay
and then uses the existing full publication checks. Prefix hashing and catalog
copies therefore remain proportional to the existing extent count. Planning
still returns the full prior manifest. This change bounds **publication request,
acknowledgement and durable suffix decision** for a fixed suffix and placement;
it does not make total append CPU, allocations or network traffic constant.

Validation covers malformed and noncanonical messages, digest and generation
conflicts, unowned pages, expiry, capacity refusal, equivalent legacy state,
speculative rollback before Raft commit, metadata leader loss, and mixed journal recovery. The local
process workload retains `N=3, W=3, R=1`, verifies the suffix on every Star
replica before reporting success, restarts all three metadata voters and three
data replicas, and reads the complete object from recovered storage.

Run the bounded CI workload with:

```sh
cargo test --locked --all-features --lib compact_append -- --test-threads=1
cargo test --locked --all-features --test network_metadata_consensus star_append_amplification -- --test-threads=1
```

Run the longer release measurement explicitly:

```sh
cargo test --release --locked --all-features --test network_metadata_consensus star_append_amplification::measure_compact_star_append_as_prefix_grows -- --exact --ignored --nocapture
```

The workload checks publication frames below 256 bytes and total three-voter
metadata WAL growth below 6,000 bytes for one suffix extent. These bounds include
the workload's short names and single placement group; larger identifiers,
more suffix extents and more placement groups require larger messages.

The [release measurement](star-append-amplification.md) records 183–190-byte
publication frames and 2,226–2,283 bytes of three-voter metadata WAL growth for
one 4-KiB suffix at 1, 128 and 4,095 prior extents. Data journal growth remains
12,552 bytes; the full-prefix planning reply remains visible in total traffic.
