# Cluster-backed S3 gateway

`object_server` normally serves one in-process `MatrixObjectStore`. With
`MATRIXOBJECT_CLUSTER_META` set it serves the replicated object plane instead: a metadata
quorum plus chunk groups running as separate processes, reached through the cluster SDK
(`RemoteMetaClient::write_object`, `append_object`, `read_object`, `read_object_range`,
`delete_object`, `list_manifests`). The in-process path is untouched.

This is a deliberately small door. It answers the S3 verbs a shared-log client needs and
answers every other verb with an S3 `NotImplemented` error, so a client that probes can fall
back and nothing returns a plausible-looking wrong answer.

## Verbs served

| S3 request | SDK call |
|---|---|
| `PUT /{b}/{k}` | `read_manifest` for the expected generation, then `write_object` |
| `PUT /{b}/{k}` + `x-amz-write-offset-bytes: N` | offset check against the manifest length, then `append_object` (first append: `write_object` with no expected manifest) |
| `GET /{b}/{k}` | `read_object` |
| `GET /{b}/{k}` + `Range: bytes=a-b` | `read_object_range` straight away; clamping, open-ended and suffix ranges read the manifest first |
| `HEAD /{b}/{k}` | `read_manifest`; length in `x-amz-object-size` |
| `DELETE /{b}/{k}` | `delete_object` (tombstone generation); absent key answers 204 |
| `GET /{b}?list-type=2` (`prefix`, `start-after`, `continuation-token`, `max-keys`, `delimiter`) | `list_manifests` pages of 32, tombstones skipped |
| `PUT /{b}`, `HEAD /{b}`, `GET /{b}?location` | answered without a cluster call: buckets are implicit |
| `GET /healthz` | one `list_manifests` call |

Refused with `NotImplemented`: multipart, copy, tagging, versioning, `versionId`, CORS,
`POST ?delete`, bucket delete, `If-Match`. `If-None-Match: *` on a PUT is honoured.

## Mapping

* Bucket and key are passed to the SDK unchanged, so the same names work through either door.
  The SDK validates them (`validate_bucket`, `validate_key`); a refusal is `InvalidArgument`.
* An object is a manifest of immutable page extents published by compare-and-swap on its
  generation. Delete publishes a tombstone generation; the key can be written again on top
  of it.
* An append is an offset condition plus a CAS. The gateway reads the manifest (or uses a small
  per-key cache), refuses with 412 when the offset is not the end, and otherwise calls
  `append_object` with that manifest as `expected`. If another writer got in first the SDK
  refuses with a precondition error, the gateway drops its cached manifest, re-reads and
  re-evaluates: the offset now either still matches (retry) or no longer does (412, which the
  client answers by re-reading the length). The `x-amz-object-size` in the reply is the length
  the published manifest reports, so a receipt always says where the bytes landed.
* An unconfirmed publication is not retried and not reported as success: it answers 503 with
  the SDK's own words.
* Listing walks the bucket's manifest catalog in key order from the beginning (the SDK takes
  an exclusive `after` key, not a prefix), skips tombstones, and stops at the first key past the
  prefix. A bucket with very many tombstones before the prefix therefore costs pages.

## Limits that surface as errors

* An object holds at most 4096 extents. Every append adds at least one extent, so a log made
  of small appends stops at about 4096 appends. The gateway checks this before writing any
  page and answers `400 InvalidRequest` naming the extent count; it does not work around it.
* A catalog holds at most 100,000 objects and a manifest at most 1 MiB.
* Whole-object GET reads the entire object into the gateway before answering.
* The gateway is unauthenticated, like the in-process door.

## Opt-in append modes

Compact publication and tail compaction are off by default, and the default append is the plain
one described above. Reserving the next allocation follows compact publication: on with it,
off without it.

| Variable | Mode |
|---|---|
| `MATRIXOBJECT_CLUSTER_COMPACT_APPEND=1` | compact publication |
| `MATRIXOBJECT_CLUSTER_RESERVE_NEXT` (on with compact publication; `0` turns it off) | with compact publication, reserve the next append's allocation in the publishing commit (one metadata commit per append instead of two) |
| `MATRIXOBJECT_CLUSTER_COMPACT_EXTENTS=N` (2 to 4096, for example 3500) | tail compaction |

**Compact publication.** Each append takes a leased staged allocation (60 s) and publishes a
digest-bound decision with `append_staged_object_compact`, instead of the whole manifest. On
the measured workload the metadata plane wrote about 1.5 KB per write instead of about
134 KB, at the price of one more metadata commit per append (higher sync write latency).
Requirements:

* Every metadata voter and journal reader must support the version 12 outcome; downgrading
  after use is unsupported.
* It turns on the durable publication fence the first time it is used. The fence is
  cluster-wide and permanent, and from then on the plain publish path is refused for EVERY
  write ("fresh extents require owned publication"). The gateway therefore also publishes
  whole-object PUTs under an owned allocation while this mode is on.
* If enabling the fence is refused, the gateway prints one line,
  `cluster gateway: compact append is unavailable (enabling the publication fence: ...); using the plain publish path from here on`,
  and uses the plain path for the rest of the process. Once the fence is on there is no
  fallback: a failed staged write is reported as the failure it is, and a failed append
  releases its allocation.

**Tail compaction.** When an object holds `N` extents, the next append goes through
`append_object_compacting`: the short tail is merged into full pages by compare-and-set, so a
log object stays appendable past 4096 extents. This is plain publication (the full manifest
per append) and so cannot be combined with compact publication, because the fence refuses
plain publication. Do not set both: below its bound compact publication would turn the fence
on, and at the bound tail compaction would then be refused. With tail compaction on, the
gateway no longer refuses appends at 4096 extents itself.

### What was measured

TemporalStore's shared-store harness, three nodes, 2000 operations per phase (4000 writes),
one host, loopback, a single metaserver and three chunk servers, release builds. The in-process
door is the same server with its journal.

| | sync write p50 / p99 | metadata written per write |
|---|---|---|
| plain append (default) | 42-59 ms / 150-167 ms | about 134 KB |
| compact publication | 77-81 ms / 143-161 ms (earlier round, loaded machine: 101-131 ms) | about 1.35 KB |
| in-process door | about 6 ms / 11-14 ms | not applicable |

* Compact publication cuts the metadata written per append by about 100 times. The sync write
  p50 is not better than the plain path: every append now costs one more metadata commit (the
  allocation). The compact-publication figures above were taken on a machine whose door
  latency was itself higher than on the plain-path round, so compare within a round.
* The plain path fails at the 4097th append to one object with
  `400 InvalidRequest` ("the object already holds 4096 extents"). Compact publication does not
  lift that bound.
* Crossing 4096 extents worked only with tail compaction on and compact publication off
  (16000 writes at 8000 operations per phase, sync p50 95 ms against 8 ms for the door), and
  that run wrote 3.45 GB of metadata because it is plain publication. The disk was 97 percent
  full during that run, so its absolute latencies are inflated.
* Not measured: data-node failure, and any metadata checkpoint setting (the Raft checkpoint
  size was left unset; 4000 writes ran without it).

### Which mode for a log workload

For a log that stays below 4096 appends per object (rotate the object before that), compact
publication: it removes the metadata growth that dominates the plain path, at a latency cost
that the extra allocation commit explains. For a log that must grow one object past 4096
appends, tail compaction, accepting plain publication's metadata cost, until a staged variant
of the tail merge exists. The default remains the plain path.

## Running it for real

* **Tests.** `tests/node_regime/cluster_gateway_processes.rs` starts three metadata voters,
  three chunk servers and the gateway as separate processes and drives every served verb over
  HTTP, then kills the metadata leader and checks that writes, appends and reads continue.
  Run it with
  `cargo test --features metadata-consensus --test network_metadata_consensus cluster_gateway_processes`.
* **Transport security.** The gateway reaches the metadata and chunk servers over mutual TLS
  when `MATRIXOBJECT_CLIENT_TLS_CA_CERT`, `MATRIXOBJECT_CLIENT_TLS_CERT` and
  `MATRIXOBJECT_CLIENT_TLS_KEY` are set (built with the `api-tls` feature). Its own listener is
  plaintext and unauthenticated: bind it to loopback or put it behind something that protects
  the network.
* **Startup.** It refuses to bind until a metadata endpoint answers, and prints
  `object_server listening on ADDR pid=PID ... backend=cluster` once it is serving.
  `GET /healthz` makes one metadata call, so it reports the cluster and not just the listener.
* **Not yet covered.** Data-node failure through the gateway, a metadata checkpoint setting,
  request authentication, and streaming of large whole-object reads.

## Concurrency

The SDK client takes `&mut self`, so the gateway keeps a small pool of metadata connections and
lends one to each request. Accept threads serve one connection at a time (as the in-process
door does), so `MATRIXOBJECT_THREADS` bounds concurrent clients.
