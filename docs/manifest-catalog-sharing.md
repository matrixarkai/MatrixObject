# Immutable catalog sharing

Metadata catalog clones now share immutable manifest entries. Snapshot capture
and Raft preparation keep shared handles; publishing a new generation installs
a new entry. Public manifest lookups still return immutable manifest values and
catalog clones remain logically independent. Earlier snapshots keep the exact
generation they selected across append, deletion and recreation.

Each entry stores its encoded catalog size. Publication uses the existing
validated size decision to populate that cache. Snapshot restoration computes
the size from the validated manifest. Speculative rollback restores the exact
entry handle and uses its cached size for body accounting, avoiding an extent
scan for each predecessor and successor during size restoration. Replacing an
existing entry reuses its map position.

These changes affect the in-memory catalog. Canonical catalog images, metadata
snapshots, binary protobuf outcomes, generation checks, capacity checks and
checkpoint limits keep their existing formats and rules. Tests compare images
against the owned protobuf representation, cross the generation 127/128 boundary,
check tombstones and cached sizes, and verify pointer identity after speculative
rollback. Once Raft commits, the current entry must have the new identity.

## Allocation measurement

The local WSL allocator test counts allocation requests and requested bytes only
inside `ManifestCatalog::clone`. Fixtures and serialization are outside that
window. Both runs use the all-features test profile; baseline storage code is
ffda552e. Every cloned catalog is checked for equal values and canonical bytes.

| Objects | Extents per object | Earlier calls | Shared calls | Earlier requested bytes | Shared requested bytes |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 6 | 1 | 1,593 | 456 |
| 1 | 4,096 | 8,196 | 1 | 632,223 | 456 |
| 16 | 1 | 83 | 3 | 7,072 | 1,464 |
| 16 | 4,096 | 131,123 | 3 | 10,097,152 | 1,464 |

Map cloning still depends on the number of objects. Each published generation
needs an owned manifest and a shared entry allocation. Public DTO clones,
candidate construction, prefix validation and hashing, and checkpoint
serialization can still copy or traverse the prefix. This measurement does not
establish constant whole-append allocations, resident memory, native latency,
CPU time or device write amplification.

CI checks that clone allocation counts and bytes are independent of extent count
for fixed object counts, and that publishing into one clone preserves the other.
It also runs canonical-image and revision accounting checks. The distributed
append workload exercises speculative rollback, metadata leader loss, minority
refusal and recovery of all metadata voters and Star replicas.

The isolated partition workload also waits for a successful metadata write plan
before initial, partitioned and healed writes. Quorum leadership can return
before asynchronous chunk health reports restore placement eligibility. The
read-only readiness check has a deadline and preserves all write-admission
rules; it does not replay a failed publication.

```sh
cargo test --locked --all-features --test catalog_manifest_sharing_allocations -- --test-threads=1
cargo test --locked --all-features --lib shared_catalog -- --test-threads=1
cargo test --locked --all-features --test catalog_manifest_sharing_allocations measure_manifest_catalog_clone_allocations -- --exact --ignored --nocapture
```
