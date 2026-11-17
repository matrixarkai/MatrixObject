# Compact staged append publication

`RemoteMetaClient::append_staged_object_compact` writes a nonempty suffix using
an existing append allocation and publishes a digest-bound metadata decision.
`publish_staged_append_compact` exposes the publication step for applications
that already own and verify their staged pages. Existing write and recovery
methods keep their existing protocol.

Upgrade **every metadata voter, recovery binary and journal reader** before
calling either method. The opt-in request is opcode 93, version 4, action 11.
Its acknowledgement is version 3 and contains only the committed allocation ID
and resulting manifest digest (the extent hash chain, `docs/manifest-chain-digest.md`). The durable binary protobuf placement outcome
uses version 12 and field 14. An older reader cannot replay this outcome;
downgrading after activation is unsupported. There is no automatic fallback to
full publication and no automatic replay after an uncertain response.

The decision contains the exact prior generation, length, extent count and
manifest chain digest, the immutable suffix, and the resulting length and
chain digest. Publication requires live staged ownership, the unchanged prefix,
fresh pages in the owned namespace, a live lease, configured groups and the
existing capacity and publication-fence checks. The allocation is consumed
atomically with publication. Replay checks the recorded decision against the
current catalog without reevaluating wall-clock expiry.

The outcome expands against the current manifest during validation and replay
and then uses the existing full publication checks. Prefix hashing and owned
candidate copies therefore remain proportional to the existing extent count.
[Catalog snapshots and undo](manifest-catalog-sharing.md) share immutable entries.
The compact
append helper now also uses [digest-bound planning](compact-append-planning.md),
which omits the prior extent list from both directions. Publication bounds the
**request, acknowledgement and durable suffix decision** for a fixed suffix and
placement; hashing, object construction and catalog copies still need profiling.

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
12,552 bytes. That initial publication measurement retained the full planning
reply; the subsequent compact-planning measurement records its removal.

## Reserving the next allocation in the publishing commit

A log that appends in a loop needs a staged allocation per append and so two metadata
commits per append: one to reserve pages, one to publish. `StagedAllocationRequest` version 5,
action 12 publishes a compact append exactly as version 4 does and, in the same durable
record, creates the allocation for the append after it (`page_count` pages, `ttl_ms` lease,
both in the request). The placement outcome for it is version 13: the compact decision, the
removal of the consumed allocation and the new reservation (field 15) are one record, so
they become durable together or not at all, and replay of that record gives exactly the
state the live node reached. The reply (version 5) returns the reservation.

What the record guarantees, and what it does not:

* Nothing is acknowledged before the record is durable, as for every metadata commit.
* The reservation is created at the metadata position of the record that publishes the
  append, for the generation that append produces, in the same object, as an append, with a
  lease. An allocation can therefore only ever be reserved after the publication it follows,
  and it cannot publish over any manifest but that one; a record that publishes without
  consuming the allocation it names, or reserves at another position, for another object or
  for another generation, is refused whole.
* The allocation table still holds 128 active allocations. A reservation is one of them, so
  a gateway that keeps one reservation per active log can serve fewer than 128 logs, and an
  idle log's reservation stays until its lease ends. The gateway keeps at most 48, gives a
  reservation a 10 second lease, and reuses one only inside the first half of it.
* If the reply is lost the reservation exists but the client does not know it; it expires
  with its lease, as an allocation whose publication was never sent does today.

The gateway uses it with `MATRIXOBJECT_CLUSTER_RESERVE_NEXT=1` (requires compact append);
an append that finds a usable reservation for the manifest it extends skips the allocation
commit. Every metadata voter, recovery binary and journal reader must support the version 13
outcome before the first use; downgrading afterwards is unsupported.
