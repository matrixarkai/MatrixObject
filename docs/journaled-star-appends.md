# Journaled Star append preparation

An exact append to a healthy, validated page can prepare its durable outcome from
the suffix and scalar page identity. The RPC server no longer retains a payload
handle solely to build that append's before-image. The resulting version-1 binary
protobuf `PageOutcome` is byte-identical to the general decision for the same valid
states, including sparse zero suffixes. Existing readers and journal replay remain
compatible.

## Recovery profile

Standalone nodes retain local XOR recovery by default. Rust options default to
`local_recovery_sidecar: true`; `chunkserver` defaults
`MATRIXOBJECT_REPLICA_LOCAL_RECOVERY` to `xor`.

The three-member Star bring-up script uses `peers`. This profile requires a durable
standalone journal and relies on configured replica repair and the journal for
recovery instead of keeping a redundant local XOR sidecar. It preserves payload
checksums, read verification, journal synchronization and the client's `N=3, W=3,
R=1` thresholds. It neither creates peers nor enforces the client's quorum: configure
the actual three distinct members and durable write threshold. Do not select it
for an unreplicated copy.

```sh
export MATRIXOBJECT_REPLICA_LOCAL_RECOVERY=peers
export MATRIXOBJECT_REPLICA_JOURNAL=/path/to/node/journal.wal
chunkserver
```

The startup line reports `local_recovery=xor` or `local_recovery=peers`. Unknown
values and peer recovery without a durable journal are refused before serving.
Checkpoint restoration is followed by application of the configured recovery
profile: peer mode removes local sidecars, and XOR mode reconstructs missing ones
before the listener is exposed. This startup work can scale with stored data.

Local recovery data is omitted from transferred page state and does not change
the page head used for quorum selection. Nodes with different local recovery
profiles can supply the same validated repair source. The process test restores
one healthy member in XOR mode, replaces a second member from two agreeing
sources, appends at W=3 and restarts every member in peer mode. Keep healthy copies
while changing configuration and verify each member's startup report. A stopped
member still blocks W=3 publication to that group; this setting does not introduce
degraded W=2 writes.

## Foreground path

The fast decision requires an existing nonempty page, a nonempty suffix at its
exact physical end, matching logical/physical lengths, no discarded ranges, no
local sidecar and no active mutation batch. Other shapes use the general path.
The existing sequence/state guards, service mode, namespace fence, capacity and
QoS checks still decide whether the request may mutate the page.

A cold validation identity hashes the prefix once and builds the existing 4-KiB
validation-block memo. A warm append checks the old tail block and carries the
remaining block identities forward. Normal mutations invalidate or correctly
extend this memo. Untouched historical blocks remain checked on reads and scrubs;
an append is not a new whole-object health certificate.

The outcome hashes only the suffix and builds patches relative to the old length.
After the ordinary mutation, the server checks resulting length, sequence,
checksum and the appended bytes' SHA-256 before journaling. Persistence failure
prevents acknowledgement and fences later requests until recovery. Batched
requests retain their shared durable synchronization point.

Prefix memory faults outside the touched block are detected when that block is
read or scrubbed. Their bytes are not copied into the durable suffix decision;
replay against the clean committed prefix recovers the expected data. Invalidated
or cold corrupt prefixes are refused before mutation.

This removes the append decision's prefix scans and retained-buffer copy. Payload
growth can still occasionally reallocate, an actual pinned snapshot can require
copy-on-write, and journal replay still performs general state validation. Full
checkpoints remain a separate foreground-stall problem.

## Validation

The focused `journal_append` library tests cover binary equality with general
decisions, zero suffixes, recovery, exclusive payload ownership, conditional RPC
variants, persistence failure, corruption detection, fallback shapes, durable
storage requirements and switching recovery profiles across checkpoints.

`journal_append_allocations` measures a real TCP request with client and server
threads in one instrumented process. It warms validation and geometric headroom,
then budgets a 4-KiB append below 512 KiB of total requested allocations at
16-KiB, 4-MiB and 64-MiB prefixes. It detects a retained-prefix copy while allowing bounded
framing and validation-index allocations. It is not a separate-process allocation
measurement or a worst-case guarantee for buffer growth.

The measured requested allocation totals were 50,899 bytes at 16 KiB and
30,341 bytes at both 4 MiB and 64 MiB, with 31, 29 and 29 allocation calls,
respectively. Setup sends bounded 1-MiB blocks and is outside measurement.
These counts include client and server framing in the same process; they are
not peak live memory or separate-process allocation measurements.

The [Star corpus workload](star-corpus-scale.md) uses three metadata voters and
three journal-backed data processes, verifies every member's peer-recovery profile,
and checks restart recovery at unchanged W=3. Report the recovery profile with
performance results; older XOR-profile measurements include different local
recovery work and are not an isolated measurement of this outcome optimization.
