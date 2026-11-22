# MatrixObject

A distributed object store, in Rust. **Apache-2.0** open source: public, forkable,
and open to issues and pull requests from anyone.

Objects are stored across replicated nodes with quorum publication, versioned
manifests, per-page integrity checks, and explicit repair paths. New version 2
manifests authenticate page reads against committed SHA-256 digests; version 1
manifests retain exact quorum-voted reads. Configure repair workers explicitly;
automatic returning-node repair scheduling is not complete. See
authenticated reads and
[repair scheduling](docs/star-repair-worker.md).

## Related Projects

**On this page:** [Start here](#start-here) | [Services](#services) |
[Independent replicas](#replicas-in-their-own-processes) | [Building](#building) |
[Configuration](#configuration) | [Limitations](#what-it-is-not) |
[Contributing](#contributing).

All six projects are independently usable under Apache-2.0. Choose by workload
and execution model; installing one does not deploy the others.

| Project | Use it for | Execution model |
|---|---|---|
| **MatrixObject (this repository)** | Distributed object storage | Object API with separately configured metadata and replica services. |
| [TemporalStore](https://github.com/matrixarkai/TemporalStore) | Temporal features, sequences, and context serving | Serving engine with its own endpoints, persistence, and replication settings. |
| [MatrixCache](https://github.com/matrixarkai/matrixcache) | Tiered caching inside a Rust application | Embedded library; the application owns authoritative data and network requests. |
| [MatrixRaft](https://github.com/matrixarkai/matrixraft) | Consensus inside a service | Embedded library; the host supplies transport and state-machine application. |
| [MatrixArk](https://github.com/matrixarkai/matrixark) | Context ingestion, retrieval, and feedback | Python runtime with explicitly configured storage and model providers. |
| [MatrixAdsDemo](https://github.com/matrixarkai/MatrixAdsDemo) | Exploring an ads retrieval and auction pipeline | Standalone generated-data demonstration, not an object-store workload. |

For this store's service topology and build features, start with
[Deployment Responsibilities](#deployment-responsibilities).

## Getting Help

Search [public issues](https://github.com/matrixarkai/MatrixObject/issues) before
opening a report. Include the revision, enabled Cargo features, service topology,
replica and quorum settings, object size, and the API operation involved. Describe
whether a write was acknowledged, published, or awaiting placement; for reclamation,
distinguish deleted objects from physical bytes returned to storage.

Use synthetic object keys and payloads. Remove credentials, tokens, and private
certificates from configuration excerpts. Follow the [security policy](SECURITY.md)
for sensitive reports, not a public issue. For code or documentation changes,
start with [CONTRIBUTING.md](CONTRIBUTING.md).

## Implementation entry points

| Area | Start reading |
|---|---|
| Object API process | [Object server](src/bin/object_server.rs) |
| Metadata process and control-plane configuration | [Metadata server](src/bin/metaserver.rs) |
| Replica process and chunk configuration | [Chunk server](src/bin/chunkserver.rs) |
| Metadata consensus integration | [Metadata consensus](src/cluster/meta_consensus.rs) |
| Chunk consensus integration | [Chunk state machine](src/raft/matrixraft_chunk_state_machine.rs) |

These are distinct implementation entry points, not a ready-made cluster configuration.
Use the deployment guides below to select durable storage, network endpoints, peer
membership, and security settings before starting independent processes.

## Start Here

For performance sizing, record object and range sizes, replica/quorum settings,
metadata topology, and acknowledgement boundary. Separate upload throughput from
publication latency and repair traffic. Logical deletion is not physical space
reclamation: account for retained versions, active read leases, and pending reclaim
before interpreting storage amplification. The guides below describe these paths.

| Task | Guide |
|---|---|
| Build and configure the store | [Building](#building) and [configuration](docs/configuring.md) |
| Serve object requests | [Network access](#reaching-the-store-over-a-network) and [API TLS](docs/api-tls.md) |
| Run metadata independently | [Metadata consensus and recovery](docs/metadata-consensus.md) |
| Place data on independent replicas | [Chunk consensus](docs/chunk-consensus.md) and [group catalog](docs/chunk-group-catalog.md) |
| Append to a very large log without a metadata write per append | [Chunk-granular manifests and log objects](docs/chunk-granular-manifest.md) |
| Plan distributed deployment work | [Distributed delivery](docs/distributed-delivery.md) |

The object store is a cluster: a metadata quorum, chunk groups and the doors that serve them run as
separate processes. There is no in-process store to embed. Building the service binaries does not
configure remote placement, metadata consensus, TLS, or repair scheduling. Select and configure those
paths explicitly using the guides above; local multi-process results are not independent-host
production qualification.

Objects are stored as chunk manifests (see [chunk-granular manifests](docs/chunk-granular-manifest.md)
and [the gateway](docs/cluster-gateway.md)). **Data directories from earlier builds are not supported**:
a door refuses to start over a cluster that still holds page-granular objects, and there is no migration.

## Deployment Responsibilities

### Build features are not deployment modes

The default Cargo features include `raft-rs`, `http-server`, `replica-server`,
and `meta-server`. They make the corresponding APIs and binaries available;
they do not turn an in-process store into independent durable voter groups.

| Opt-in feature | Purpose |
|---|---|
| `metadata-consensus` | Independent MatrixRaft voters for metadata requests |
| `chunk-consensus` | Independent MatrixRaft voters for chunk groups |
| `api-tls` | TLS support for the binary service APIs |

For the independent consensus features, use Rust 1.89 or newer and install the
Protobuf compiler, Autoconf, Automake, and Libtool before building. The metadata
API and Raft peer transport use separate ports; reserve both in each voter's
network configuration. See the [metadata deployment guide](docs/metadata-consensus.md)
for process-specific settings rather than sharing a state directory between voters.

From the repository root, build these paths together with:

```bash
cargo build --locked --release --features metadata-consensus,chunk-consensus,api-tls
```

Configure endpoints, group identities, durable directories, and TLS credentials
using the guides above before starting services. Each voter needs exclusive
ownership of its own durable files. Enabling features alone does not create
replicas or establish quorum availability.

### Configure the services

| Layer | Configure before relying on it |
|---|---|
| Object API | Request authentication, TLS, request budgets, and the network endpoint |
| Metadata service | Durable voter storage, membership, quorum availability, and recovery |
| Chunk and page services | Independent replica placement, persistence, and repair scheduling |
| Reclamation | Reader protection, stale-range handling, and the work needed to reclaim storage |

Keep acknowledgement, publication, and reclamation separate: accepting staged data does not
by itself publish an object, and deleting metadata does not immediately return physical
space. Use the manifest, lease, and repair guides above when choosing recovery and capacity
policies. A local embedding example does not exercise these independent service boundaries.

## Deployment and Upgrade Checklist

- Choose metadata and chunk consensus features explicitly, then configure their
  independent processes, identities, ports, and durable directories. Building all
  binaries is not a cluster deployment.
- Preserve metadata checkpoints and journals together with the data replicas
  they describe. An object API process alone is not a backup of the store.
- Upgrade protocol consumers before enabling nonempty namespace fences. Consult
  the linked protocol guides for refused operations rather than assuming an older
  peer will transparently downgrade.
- Budget separately for published objects and repair work. A delete publishes a
  tombstone; the pages no manifest names are freed by the reclaim sweep after its
  grace, not by the delete.
- Use [distributed delivery](docs/distributed-delivery.md) to distinguish local
  multi-process behavior from independent-host availability and capacity claims.

## How storage is laid out

An object is a manifest of chunk extents (see [chunk-granular manifests](docs/chunk-granular-manifest.md)): runs of pages in a chunk of a replica group, with a digest the manifest commits to.

### Storage units

| tier | default size | what it is |
|---|---|---|
| **chunk** | 64 MiB | the append-only container objects are allocated from |
| **page** | 1 MiB | the replicated unit: what a quorum votes on and what repair rebuilds |
| **fragment** | derived | the pieces an erasure code splits one page into, plus its redundancy piece |

An object becomes one or more **extents** — a `(chunk, offset, length)` range,
each at most `max_extent_bytes` — and an extent maps onto the pages covering
it by `page_index = absolute_offset / page_size`. So a ranged read fetches
only the pages it needs instead of scanning a chunk, and a repair rebuilds one
page rather than a whole container.

**shard** is used in the distributed sense only -- metadata partitioned by key
range across services. The pieces inside one page are **fragments**: they never
leave the page they came from. The service coordinating a chunk's replicas is the
**chunk service**; the one holding a single replica is the **replica service**.

Two classes of name are still spelled the old way on purpose, because the text
itself is load-bearing rather than cosmetic:

| still named the old way | why it stays |
|---|---|
| **151 durable format constants** (36 contain `BLOCK`, 3 `SEGMENT`) | they are hashed into proofs, so changing the text changes every checksum computed from it |
| **154 control-field and report keys** | they are observable output -- someone's dashboard reads them |

Both are a versioned migration rather than a rename, and
`every_durable_format_constant_is_accounted_for` pins all 151 by name in both
directions: a renamed constant fails it, and so does a new one. The second count
is distinct lower-case literals containing `block` or `segment`; counted another
way it differs, which is why the definition is here and not only the figure.

The directories `src/services/page` and `src/services/chunk` hold the chunk
service and the replica service respectively, and move in a later change; the
types and fields are already `ChunkId`, `chunk_size`, `PageId`, `page_size`.

## Services

| service | responsibility |
|---|---|
| **meta** | object metadata, leases, key-range shards, transactions, the change feed |
| **page** | allocation, placement, the write and read quorums, repair and scrub |
| **chunk** | the data itself: one replica of each chunk, with its checksum and discard map |

### What a write promises

**A put either completes or it did not happen.** There is no partial object and no stated
size below which that holds and above which it does not: the guarantee is the same for a
one-page object and a five-page one.

A put is split into pages by `max_extent_bytes` and written one page at a time, so a
failure part way through is the case that matters. Both shapes are pinned:

| the failure | the guard |
|---|---|
| refused *after* mutating — the expected-checksum check fires once counters have moved | `a_put_that_fails_after_mutating_leaves_the_store_unchanged` |
| refused *inside* the write loop — four pages written, the fifth refused | `a_write_that_fails_part_way_through_its_pages_did_not_happen` |

Each asserts the store is unchanged in four views: the object is absent, its neighbour is
intact, the metadata fingerprint is unchanged, the chunk ledger is byte-identical and the
**snapshot bytes are identical**. One audit key is allowed to fall and never to rise — a
rollback clones the receipt maps and a clone allocates exactly, so a restored receipt holds
the same text in a tighter allocation.

What the store does **not** promise: a partial write does not report how far it got. The
error names why it was refused, not how many pages had landed.

### What a compaction gives back

A superseded object leaves its bytes behind, and the space ratio says so:
`space_amplification_ppm` is the logical bytes a chunk occupies over the bytes still live
in it. Overwrite one object in ten and it reads **1,100,000** — a tenth of the payload left
behind. Overwrite half and it reads 1,500,000.

`compact_all(min_stale_bytes)` moves every live object out of the chunks above that
threshold and retires them, which takes the ratio back to **1,000,000** and then finds
nothing to do: a second pass compacts zero chunks. Two things make that true, and both were
wrong before:

- **the chunk being drained is out of the allocator** for the whole rewrite, or the rewrite
  lands back in its own source — and the compaction then marks it stale, including the bytes
  it has just written there;
- **a drained chunk is retired**, or its logical bytes keep counting against the ratio and
  each pass makes the figure worse than the last.

A **retained version refuses** the compaction of its chunk. Compaction rewrites current
objects and has no path to move a version, and a version's bytes are never marked stale, so
compacting underneath one would declare them stale and offer them to the next writer.

`reuse_stale_chunk_space` is the other way to get the space back — the next write takes it
instead of growing the chunk — and it holds the ratio at 1,000,000 at any overwrite rate.
**It is on by default**, because the alternative is to accumulate without bound: nothing
here drives compaction, so a store nobody maintains keeps every superseded copy. Overwrite
one object nine times and the ratio reads 10,000,000.

Set it to `false` to get the other behaviour — the garbage accumulates where the collector
can see it, and `compact_all(min_stale_bytes)` takes it back on demand. Either way a
retained version's bytes are never stale, so neither route can take them.

### A replica that missed a write does not get a vote

A page carries a write sequence on each replica that holds it, and a read takes a majority
over those. That compares the replicas with **each other**, so a replica that missed a
committed write and came back is only ever *outvoted* -- and when it is not outvoted, it
wins. With a read quorum of one it won, and served its superseded bytes as current.

So the page service keeps its own record, which the replicas cannot write to: the replicas
it will not believe. A replica goes in when it stops being able to receive writes --
quarantined, frozen, or taken out of the endpoint set -- and comes out only through
`check_in_replica`, which reads its pages and refuses while any of them trails a replica
that is believed, naming the page and both sequences.

| | |
|---|---|
| a believable replica | counts toward the read quorum |
| a replica in the record | still readable, still holds its bytes, still a repair target -- but it does not count |
| a read that cannot reach a quorum of believable replicas | **refused**, and the refusal says staleness was why |
| a read served without one | reported as degraded |

Three consequences worth stating plainly, because each is a choice:

- **A repair is not a check-in.** Repairing a page brings the bytes across; it does not
  establish that the replica holds every page it should, which is a different statement and
  the one the record is about.
- **Nothing is deleted.** A stale copy is kept -- it may be the last copy of something --
  which is also why a refused check-in can cite the evidence rather than having thrown it
  away.
- **A restart does not forget.** The record is in the page service snapshot, at format
  version 24; a snapshot written before that version has its record reconstructed from the
  quarantine and endpoint sets it does carry, because a replica that was not receiving
  writes is not believable whatever wrote the snapshot.

It costs one `BTreeSet` field on the page service, which is empty on a healthy store and
so allocates nothing: staleness is a property of a replica rather than of a page, so there
is no per-page term at all.

### What a scrub can be asked to do

A scrub reads every stored byte of every active replica, verifies it against its checksum
and, when asked, repairs from quorum. It is the one maintenance pass meant to run at a
throttled rate over days -- which means it has to be able to stop and carry on, and
`scrub_page_replicas(repair)` cannot: it does the whole store in one call.

That is the right shape for a store small enough to finish and the wrong one for a store
large enough to need throttling, so the bounded form is additive:
`scrub_page_replicas_bounded(after_page_id, max_chunks, repair)` scans at most `max_chunks`
pages and says where it stopped.

| field | what it carries |
|---|---|
| `marker` | the cursor the call was given |
| `next_marker` | the last page the call scanned |
| `has_more` | whether there is more to do -- **this is the field to loop on** |

A `max_chunks` of zero is refused rather than taken to mean "no bound", and all three
fields carry the meanings they already carry on `audit_page_replicas`, which bounds and
resumes over these same pages. Two calls over one set of pages, taking the same argument
names, should not mean different things -- so a test asserts the pair agree, rather than a
comment claiming they do.

## What it does

- **Quorum publication and verified reads.** An object is a manifest published with a generation
  check; every page a read returns is checked against the digests the manifest committed, so a replica
  that has fallen behind or been corrupted cannot answer with stale or wrong bytes. See
  object manifests and authenticated reads.
- **Replicas are their own processes.** Metadata voters and chunk groups run as separate processes; see
  [replicas in their own processes](#replicas-in-their-own-processes) for the kill test and for what is
  not remote yet.
- **Repair.** A replica that missed writes is brought back by the repair worker; see
  [repair](docs/star-repair-worker.md).
- **Idempotent mutations.** Allocation, write, discard and repair carry receipts, and an append that may
  have landed is settled by its attempt id, so a retried request is not applied twice.
- **Chunk-granular manifests.** An object can be stored as chunk manifests, one extent per run of pages
  with a digest page, and appended to without rewriting it; see
  [chunk-granular manifests](docs/chunk-granular-manifest.md). Pages no manifest names (a deleted or
  overwritten object, a writer that died before publishing) are freed by a sweep the door runs.
- **S3 front door.** PUT, GET with ranges, HEAD, DELETE, append, copy, multipart, batch delete,
  conditional requests and delimiter listings; see [the gateway guide](docs/cluster-gateway.md).

## Reaching the store over a network

An S3-compatible HTTP front door and a binary that serves it are built in. The door serves a
cluster: it refuses to start unless `MATRIXOBJECT_CLUSTER_META` names the metadata endpoints, and every
request is answered by the cluster SDK against that quorum and its chunk groups.

```
MATRIXOBJECT_CLUSTER_META=127.0.0.1:17101,127.0.0.1:17102,127.0.0.1:17103 \
MATRIXOBJECT_BUCKET=mybucket \
cargo run --release --bin object_server
```

| variable | default | meaning |
|---|---|---|
| `MATRIXOBJECT_CLUSTER_META` | *(required)* | `host:port[,host:port...]`: the metadata endpoints |
| `MATRIXOBJECT_HTTP_ADDR` | `127.0.0.1:17200` | address to bind |
| `MATRIXOBJECT_BUCKET` | `default` | bucket the door serves |
| `MATRIXOBJECT_MAX_BODY_BYTES` | 64 MiB | largest object this door accepts |
| `MATRIXOBJECT_THREADS` | 4 | accept threads |

The binary's own documentation lists every cluster, roster, TLS and reclaim variable
(`src/bin/object_server.rs`), and [the gateway guide](docs/cluster-gateway.md) explains what each one
changes. A door that stores chunk objects runs the reclaim sweep that frees the pages no manifest names; it
refuses to start with the sweep off unless that is acknowledged.

The verbs, where `{bucket}` is the first path segment and `{key}` is everything after it:

| request | does |
|---|---|
| `PUT /{bucket}/{key}` | write an object; honours `If-None-Match: *` and `If-Match: "etag"` |
| `GET /{bucket}/{key}` | read it; honours `Range: bytes=` and `If-None-Match` |
| `HEAD /{bucket}/{key}` | its headers, and the length the matching `GET` would send |
| `DELETE /{bucket}/{key}` | remove it; idempotent, so a retried delete answers 204 |
| `PUT /{bucket}` | `CreateBucket`, idempotently; answers a `Location` and no body |
| `HEAD /{bucket}` | `HeadBucket`: 200 if it exists, `NoSuchBucket` if not |
| `DELETE /{bucket}` | `DeleteBucket`; `BucketNotEmpty` with 409 while it still holds objects |
| `GET /{bucket}?prefix=&delimiter=&continuation-token=&start-after=&max-keys=` | `ListObjectsV2`, with the delimiter rolled up into `CommonPrefixes` |
| `GET /` | `ListBuckets` |
| `POST /{bucket}/{key}?uploads` | `CreateMultipartUpload` |
| `PUT /{bucket}/{key}?partNumber=&uploadId=` | `UploadPart`; answers the `ETag` the completion must name |
| `POST /{bucket}/{key}?uploadId=` | `CompleteMultipartUpload`; the part list in the body is validated against what was uploaded |
| `DELETE /{bucket}/{key}?uploadId=` | `AbortMultipartUpload` |
| `GET /{bucket}/{key}?uploadId=` | `ListParts` |
| `GET /{bucket}?uploads` | `ListMultipartUploads` |
| `POST /{bucket}?delete` | `DeleteObjects`; 200 with a per-key report, because one key failing is not the batch failing |
| `PUT /{bucket}/{key}` + `x-amz-copy-source` | `CopyObject`, honouring `x-amz-metadata-directive` |
| `GET /{bucket}?location` | `GetBucketLocation` |
| `GET /{bucket}?versioning` | `GetBucketVersioning` |
| `PUT /{bucket}?versioning` | `PutBucketVersioning` |
| `GET /{bucket}?versions` | `ListObjectVersions`, with delete markers reported as such |
| `GET` / `HEAD /{bucket}/{key}?versionId=` | read one named version |
| `DELETE /{bucket}/{key}?versionId=` | remove one named version |
| `GET` / `PUT` / `DELETE /{bucket}/{key}?tagging` | object tagging |
| `GET` / `PUT` / `DELETE /{bucket}?cors` | bucket CORS |
| `GET /healthz` | readiness, which takes the store lock rather than only reporting the listener |

A write answers the object's `ETag`, `Content-Type`, `Last-Modified`, its version and length, and
any `x-amz-meta-*` headers it was given back again. A listing reports `<IsTruncated>` and, when it
is truncated, the `<NextContinuationToken>` to resume from -- a client cannot tell a full page from
a truncated one by counting, and a token on a final page would make it ask again for nothing.

Every failure is an `<Error>` document carrying the `<Code>` a client branches on, and the codes
are S3's: `NoSuchBucket` and `NoSuchKey` are distinguished rather than both reported as a 404 with
prose, because one tells a client to create the bucket and retry and the other tells it to stop.
`NoSuchUpload` is a third, for the same reason. A V1 listing client that sends `marker` and no
`list-type` is served the same body.

`CompleteMultipartUpload` is the one request whose body is read. The parser recognises exactly that
document and refuses everything else -- including a document type declaration, a part list under
another root element, and trailing or missing elements -- because accepting more of XML buys nothing
a client needs. The part list is then checked against what was actually uploaded, and a mismatch
answers `InvalidPart` or `InvalidPartOrder`: the store completes whatever parts it holds and takes
no list, so a mismatch not caught here is not caught at all, and the client would be handed an
object assembled from something other than what it sent.

### Two things to know before putting data in it

**It is unauthenticated.** No signing, no token, no TLS on the door. Anything that can open a socket to
the port can read, overwrite and delete every object. That is why the default bind is loopback; moving
it off loopback puts the whole store behind whatever else protects that network. The binary says so on
startup when the address is not loopback.

**The door holds no data.** What survives is what the metadata quorum and the chunk groups keep; they have
their own durability settings ([metadata](docs/metadata-consensus.md), [chunks](docs/chunk-consensus.md)).
Without durable directories on those processes nothing survives their restart.

### What it is not

An `Authorization` header is accepted and ignored rather than refused, so a signing client works
against this surface -- it is simply not verified. There is no SigV4.

Every verb the door does not serve is answered with an S3 `NotImplemented` error, so a client that
probes can fall back and nothing returns a plausible-looking wrong answer; the list is in
[the gateway guide](docs/cluster-gateway.md). `GET /healthz` is deliberately not S3: it answers plain
text, because a container probe and an operator read it.

`chunked` framing is refused with 501 rather than read as a body of the wrong length, a `PUT` with no
`Content-Length` is refused with 411 rather than stored as empty, and a multi-range request falls back to
the whole object rather than returning one range under a 206 claiming to be all of it.

**Footprint (memory and bytes per object) has not been measured for the cluster stack yet.** The figures
this page used to carry were measurements of an in-process store that no longer exists.

### Embedding it

```rust
use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
use matrixobjectstore_rs::net::{ObjectHttpServer, ServerOptions};

let gateway = std::sync::Arc::new(ClusterGateway::over_sdk(open_metadata_client, GatewayOptions::default(), "mybucket"));
let server = ObjectHttpServer::bind_handler("127.0.0.1:17200", gateway, ServerOptions::default())?;
let handle = server.spawn()?;          // returns at once; `local_addr` reports the bound port
// ... later ...
handle.shutdown();                     // stops the accept loops and waits for them
```

The listener is non-blocking so the accept loops can be stopped, which is what makes the server
usable as a component and testable in-process; the cost is one poll per idle interval.

It adds **no dependencies**. The HTTP reading and writing is written on the standard library
alone -- see the comment at the top of `src/net/http.rs` for why that is a deliberate constraint
for a crate that other people's data sits inside, and for the list of framings it refuses rather
than guesses at.

## Replicas in their own processes

The front door above lets a **client** reach the store from another machine. This is the
other direction: letting a **replica** live somewhere other than the coordinator's heap.

Until `src/cluster`, every replica in this store was an element of
`replicas: Vec<MatrixReplicaService>`. The fan-out was a `for` loop over
`self.replicas.iter_mut()`, and a write quorum was a count of loop iterations that returned
`Ok`. Three replicas were three `BTreeMap`s in one address space, which pays the full cost of
three copies and buys none of what three copies are for: one process ending takes all three.

`chunkserver` is one replica in its own process, reachable over a socket:

```
MATRIXOBJECT_REPLICA_ADDR=127.0.0.1:17301 cargo run --release --bin chunkserver
```

| variable | default | meaning |
|---|---|---|
| `MATRIXOBJECT_REPLICA_ADDR` | `127.0.0.1:17300` | address to bind; port `0` lets the kernel choose |
| `MATRIXOBJECT_REPLICA_THREADS` | `4` | connections served at once |
| `MATRIXOBJECT_REPLICA_MAX_PAYLOAD_BYTES` | `8388608` | largest payload one frame may declare |
| `MATRIXOBJECT_REPLICA_JOURNAL` | *(unset)* | append-only replica journal file in an existing directory |
| `MATRIXOBJECT_REPLICA_CAPACITY_BYTES` | `268435456` | total page payload bytes, including zero-filled space before write offsets; positive |
| `MATRIXOBJECT_REPLICA_MAX_PAGES` | `65536` | maximum page identities, including empty pages; positive |

The payload and page limits are checked before admitting a write and during journal replay.
Restart with limits below the saved history is refused without truncating the journal. Overwrites that do not
increase either budget remain available at capacity. These are storage admission limits,
not a process memory ceiling: indexes, caches, sidecars, and network buffers use additional
memory. Discard marks byte ranges and does not release the underlying payload allocation.
Zero disables neither limit; it is rejected at startup.

The bound address and the PID are printed on the first line of stdout and flushed before the
accept loop starts, so a supervisor -- or a test standing three of these up on port `0` --
can learn which port the kernel chose.

`ReplicaPlacement` is the one place that chooses, and **its default is the in-process
arrangement**, so nothing that exists today behaves differently:

```rust
use matrixobjectstore_rs::cluster::{ReplicaClientOptions, ReplicaPlacement};

let placement = ReplicaPlacement::Remote {
    endpoints: vec!["127.0.0.1:17301".into(), "127.0.0.1:17302".into(), "127.0.0.1:17303".into()],
    write_quorum: 2,
    read_quorum: 2,
};
let mut fanout = placement.open(&ReplicaClientOptions::default())?;
fanout.write_chunk(&page_id, 0, &bytes)?;      // three processes, one quorum
```

The network transport uses a six-byte frame header and a payload on the standard library.
The `fs2` dependency supplies the exclusive journal file lock. The frame carries a protocol stamp in both directions
so that a rolling restart -- a new coordinator reaching an old replica -- refuses the frame
by name instead of taking its length field from the wrong offset.

### Replica restart recovery

Set `MATRIXOBJECT_REPLICA_JOURNAL` to retain a replica's writes and discards across restarts.
The server locks and replays the file before listening, then syncs each successful mutation
before acknowledging it. Failed persistence blocks all further requests until restart.
Unset keeps the existing in-memory behavior; startup reports `storage=journal` or
`storage=memory`. Each node needs its own journal on persistent storage.

Records contain a version marker, bounded length, header checksum, payload checksum, and
encoded mutation. Recovery truncates only an incomplete final record and refuses complete
records with checksum or decoding failures. The checksums detect accidental corruption;
they do not authenticate the file. A lost acknowledgment can leave a durable operation, so
blind mutation retries are not idempotent. Linux process-kill tests verify writes, discards,
exclusive ownership, and recovery of all three replicas followed by another node loss.
Power-loss and non-Linux filesystem behavior have not been tested.

Standalone journal nodes support opt-in checkpointing through
`MATRIXOBJECT_REPLICA_CHECKPOINT_BYTES`. The server writes and syncs a checkpoint before
reclaiming covered journal records. Do not replace or remove these files while the server runs.
Recovery restores that node's own durable history; it does not fetch writes missed while offline
or authorize readmission. Explicit `RemoteReplicaSet::repair_inventory_batch` repairs bounded
page batches from a matching source quorum. Returning nodes can remain in maintenance mode,
excluded from placement and ordinary writes, until an operator explicitly resumes them.
The [five-replica repair workload](docs/star-repair-scale.md) verifies this sequence through
metadata failover and full process restart. Automatic repair scheduling remains unfinished.
New object writes use authenticated object reads
to accept one digest-matching star copy and retry a missing, stale, or slow member.
Replicated metadata uses the separate [MatrixRaft configuration](docs/metadata-consensus.md).

### Metadata restart recovery

Set `MATRIXOBJECT_METASERVER_STATE_DIR` to an existing directory to enable durable metadata with
a bootstrap snapshot and an incremental binary protobuf outcome journal. Initial roster
bootstrap saves a synced snapshot. Subsequent heartbeats, heartbeat expiry, and placement
commits append decided record changes to `metadata.outcomes` and sync before acknowledgment.
A directory lock prevents another server from opening the same directory. Startup retains
its `storage=snapshot` label for the snapshot-plus-journal backend.

Recovery validates the snapshot, restores metadata and placement state, then installs each
outcome without rerunning placement decisions or expiry logic. A recovered roster overrides
bootstrap environment settings. Interrupted temporary snapshots are ignored; only an incomplete
final journal record is truncated. A missing initialized snapshot or outcome journal, a damaged
complete record, or a prior-state mismatch prevents startup. Persistence failures stop requests
until restart. The journal currently retains its full history; automatic checkpoint compaction
is still required for bounded long-running deployments. Older binaries must not open a directory
after it starts recording outcomes because they do not replay the journal.

Keep all files in the state directory together and do not modify them while the node runs.
Linux process-kill recovery and write failures are tested; power-loss and non-Linux filesystem
behavior are not. A lost acknowledgment may still mean a durable mutation. This provides
single-node recovery, not a replicated or highly available metadata service.

### What the kill test establishes, and in what order

`tests/a_replica_is_its_own_process.rs` is the test this arrangement exists to pass. A test
that wrote bytes, killed something and read the bytes back would pass identically against
three replicas on one heap, so the regime is asserted first:

| | what is asserted |
|---|---|
| 1 | **the regime** -- three distinct live PIDs, none of them the test's, and an ESTABLISHED socket per replica attributed to that replica's PID. Read twice, from `ss -tnp` and from `/proc/<pid>/fd` against `/proc/net/tcp`, and the two readings must agree |
| 2 | 24 pages written, each acknowledged by **all three** replicas, each replica then reporting all 24 |
| 3 | one replica **killed by PID**, after confirming from `/proc/<pid>/environ` that the process carries this run's token |
| 4 | all 24 pages read back from the survivors: exact count, **zero missing, zero wrong byte** |
| 5 | **the negative control** -- a page never written reads absent, on the same survivors at the same moment |
| 6 | a write needing all three is now **refused**, so the count in step 2 was counting replicas and not iterations |

Steps 1 and 5 are why 2-4 mean anything. Without the regime, the test is a statement about
one heap; without the control, 24 hits are consistent with a read path that answers for every
address it is given.

### What is not remote yet, and how large it is

The data calls cross the socket: `write_chunk`, `read_chunk`, `head_chunk`, `discard_chunk`.
The bounded `page_inventory` call below also reads page identities and sequences remotely.
**`MatrixChunkService` -- the service that holds `replicas` and runs the fan-out described
above -- is unchanged and does not route through `ReplicaPlacement`.**

Remote clients also support conditional writes and discards, complete page-state reads,
staged state installation and explicit bounded star repair. The distributed object client
writes fresh pages to configured Raft groups, verifies their contents, and conditionally
publishes a manifest through replicated metadata. These paths do not convert every operation
of the local `MatrixChunkService` into a network operation.

Use `tools/replica_surface.sh` on a clean checkout to measure the local service's current
replica-call surface. It temporarily substitutes the replica element type and classifies
compiler diagnostics before restoring the file. Its historical counts are not a current
readiness score. Read [chunk consensus](docs/chunk-consensus.md),
[chunk-group routing and membership](docs/chunk-group-catalog.md), and
object manifests for the implemented remote contracts and limits.
The local multi-process tests do not establish sustained large-scale availability or
independent-host production qualification.

### Remote page inventory

`RemoteReplicaClient::page_inventory(after, limit)` reads page identities and write sequences
from a replica node, without transferring payloads. Limits are 1–4,096 rows per request;
each row occupies 32 wire bytes. When `has_more` is true, resume with the last returned
`PageId` as the exclusive cursor. An empty final batch has `has_more = false`.

This is a live ordered traversal, not a snapshot. Fence writers before using it to certify
that a returning replica is current. The main chunk-service dispatch still uses local
replicas; this inventory extends the remote client for that integration. Existing wire
verbs keep their codes and layouts; older nodes explicitly refuse the new inventory verb.

## Building

Check out the public source, then build from the repository root:

```bash
git clone https://github.com/matrixarkai/MatrixObject.git
cd MatrixObject
cargo build --locked --release
cargo test
```

These commands use the default feature set; they do not start metadata or chunk
services. Independent consensus builds require additional native build tools and
explicit features. Follow [Deployment Responsibilities](#deployment-responsibilities)
and the [metadata build prerequisites](docs/metadata-consensus.md) before enabling
those paths. Configure separate service identities and durable directories before
starting voters.

## Configuration

The processes are configured by environment variables, documented at the top of each binary
(`src/bin/object_server.rs`, `src/bin/metaserver.rs`, `src/bin/chunkserver.rs`) and explained in
[configuring](docs/configuring.md) and [the gateway guide](docs/cluster-gateway.md). Object writes
take their page size, group spread and size limit from the writer's options
(`ObjectWriteOptions`); the metadata plane keeps one control plane, the metaserver, selected by
whichever endpoints a door or client names.

**`page_bytes` is the one worth setting deliberately.** It is the replicated unit, so an object smaller
than a page still occupies a page's worth of replication, and an object much larger than a page is split
across more pages than a ranged read needs. Sizing it near your median object size keeps both from
happening. [docs/configuring.md](docs/configuring.md) has the sizing table and the measurements behind it.

## Contributing

Anyone can contribute: fork, commit, open a pull request. Issues are open, and
`main` carries no branch protection, so an outside pull request can land without a
review you cannot obtain. [CODEOWNERS](.github/CODEOWNERS) requests a reviewer; it
gates nothing.

[CONTRIBUTING.md](CONTRIBUTING.md) is the guide -- what CI runs and what it does not,
how to validate changes and investigate failures, and the shape a change
is expected to take. [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) applies. There is a
pull-request template and two issue templates; a security or durability report goes
privately through [SECURITY.md](SECURITY.md) instead of an issue.

`the_repository_carries_what_an_outside_contributor_needs` checks those files are
here, that the contributing guide is this project's, and that every path it sends a
reader to exists -- the guide was a sibling library's for a while, and a check on mere
existence scored it present.

Two invariants worth knowing before you start:

- **A durable format is a contract.** Snapshot and proof encodings are hashed
  into checksums, so changing the bytes a format emits — including the text of a
  format constant — changes every checksum computed from it. That is a
  migration, not a rename.
- **A measurement needs its denominator.** Performance claims here are expected
  to say what they are per: per put, per live byte, per stored object. "Faster"
  without one is not a result.

## Continuous integration

On pushes to `main` and on every pull request, four jobs:

- **build & docs** -- `cargo build --all-targets` and `cargo doc`, each for the
  default features and for `--all-features`, with warnings denied.
- **format and rollback guards** -- selected library guards use `--exact`;
  explicit integration and binary targets run their selected harnesses. Each
  command checks a nonempty successful result, with expected counts or required
  test names, because `cargo test` exits 0 when a filter matches nothing. Star
  append guards cover restart and mixed-profile repair, binary decisions, invalid
  recovery settings and warm append allocation budgets through a 64-MiB prefix.
  A [checkpoint interference case](docs/star-corpus-scale.md#foreground-checkpoint-interference)
  verifies append copies and recovery while a foreground checkpoint runs.
  [Shared journal sync](docs/star-journal-synchronization.md) guards also cover
  conditional ordering, durable completion, failure fencing and group bounds.
  Durable payload extents add binary-format,
  integrity, killed-writer recovery and bounded allocation guards.
  [Stored outcome records](docs/stored-outcome-records.md) test one-sync binary
  decisions, metadata-only reads and three killed storage writers; live server
  routing and metadata-only checkpoints remain to be implemented.
  [Managed outcome segments](docs/outcome-segment-rollover.md) add rollover,
  bounded readers, committed scan cuts and killed-writer publication guards.
  Process lifecycle and compiled-test discovery checks run in isolated invocations
  so forked children cannot retain unrelated library-test file locks.
  The guards over
  [replica processes](#replicas-in-their-own-processes) check their output for
  `running 1 test` as well: they name a test of an integration target rather than a
  `tests::` filter, so a rename would leave the filter matching nothing and the
  `1 passed` line absent rather than wrong.

- **network metadata consensus** -- named routing, process, recovery, storage, and
  checkpoint tests with the optional consensus feature enabled.

- **network chunk consensus** -- named checks of independently durable chunk
  voters, leader loss, restart recovery, capacity, shutdown, and metadata health.
  See [chunk consensus configuration](docs/chunk-consensus.md).

CI does not run the whole library suite. `tools/suite_gate.sh` does, locally: it
compares failures by name against `tools/known_failures.txt` and fails in **both**
directions: a new failure, or a recorded failure that has started passing.
There are no recorded library failures. Ignored tests are not covered by this result;
run optional consensus coverage separately with `cargo test --all-features --lib`.

## Licence

Apache License 2.0 -- see [LICENSE](LICENSE) and [NOTICE](NOTICE).

Every Rust source opens with the two lines that say so:

```text
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
```

`every_rust_source_carries_the_licence_header` walks the tree and fails if a source
is missing either line, if the walk finds fewer than a thousand sources, or if the
`LICENSE` and `NOTICE` stop saying what the header claims.

### Node reply budgets

The replica and metadata server `max_payload_bytes` setting bounds both incoming
and outgoing payloads. It must be between 24 bytes and the four-byte frame length
maximum; the default remains 8 MiB. A read includes 40 bytes of sequence, checksum,
and length fields, so its data must fit within the remaining budget. Oversized
reads are refused before copying data, and oversized roster lists before copying
the roster. Other successful replies are checked before wire encoding. Failure
messages are shortened on UTF-8 boundaries to fit the same budget.

A reply-size refusal leaves the connection usable. Request smaller read ranges
or inventory batches when necessary. Roster listing currently has no pagination;
configure enough space for the roster. Head replies may require more space as
discarded ranges accumulate. Client and server limits are configured separately.

### Metadata Raft command application

With `matrix-raft`, `MetadataStateMachine` applies the eight lease and shard
migration commands carried by `ObjectCommand::to_payload`. It checks group,
term, and consecutive log indexes. Repeating the latest identical entry returns
its saved response without applying it again; conflicting or older delivery is
refused. A response starts with byte 0 for success, or byte 1 followed by UTF-8
text for a committed application refusal. Business refusals advance the log;
corruption errors stop application until a checkpoint is installed.

The `MOMRAFT1` checkpoint contains metadata, group and Raft log identity, the
latest command and its response, and a checksum. It preserves the Raft index
separately from the metadata service commit counter, including across rejected
commands. Checkpoint transfers reject changed identities, invalid offsets,
stale log positions, and more than 64 MiB of accumulated data. Legacy raw
metadata snapshots remain readable through the metadata service API; they are
not Raft checkpoints and cannot recover a Raft replay position.

This section describes the state-machine adapter. The optional
`metadata-consensus` and `chunk-consensus` features additionally run independent
durable MatrixRaft voters. Group-aware placement, the distributed HTTP backend,
and large-cluster failure testing remain required before a distributed
production-readiness claim. See [chunk consensus configuration](docs/chunk-consensus.md).

### Binary page outcomes

New chunk journal records, `ChunkRaftWriteGroup` proposals, and independent
`chunk-consensus` voters use the same
binary protobuf `PageOutcome` schema in `proto/page_outcome.proto`. The outcome
contains decided byte patches, page sequence, lengths, checksums, discard ranges,
and physical counter increments. Recovery installs that state without running a
write command again. A prior-state digest rejects reordered or mismatched changes.
Appending six bytes to a 1 MiB page produces an outcome smaller than 256 bytes.

Payloads are bounded to 8 MiB and materialized pages to 256 MiB on this path.
The local `MOR2` journal framing adds header and payload checksums. Older `MOR1`
records and chunk command log entries remain readable. Older binaries cannot
read the new records. The page codec does not establish network Raft consensus. The group adapter remains an in-process apply harness.

### Binary placement outcomes

The metaserver journal and metadata MatrixRaft adapter accept the same `PlacementOutcome`
binary protobuf schema in `proto/placement_outcome.proto`. Each outcome carries prior and
resulting node usage or object placement values and consecutive placement commit indexes.
All prior-state and shape checks complete before mutation. A heartbeat captures its node and,
for a catalog member, its logical chunk group. An object placement captures the object and its
affected nodes. Heartbeat expiry scans the roster.
The payload is limited to 8 MiB and protected by a checksum.

This covers the network placement API. Other metadata commands still use their existing codec,
and checkpoints retain the metadata snapshot inside a group-aware envelope when a chunk catalog
is configured. With `metadata-consensus`, network placement requests commit through MatrixRaft.

### Network metadata voters

The optional `metadata-consensus` feature runs one durable MatrixRaft voter per
metaserver process. See [configuration, recovery, and current limits](docs/metadata-consensus.md).

### Raft chunk groups in placement

A metaserver can load a fixed binary protobuf chunk catalog through
`MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE`. Each replicated group contributes one
placement target and one copy of capacity. Voters report health under separate
member IDs. Metadata clients resolve the persisted membership and verify chunk
configuration before sending data. See [catalog configuration and recovery](docs/chunk-group-catalog.md).

### Storage naming compatibility

The storage tiers are chunk, page, and fragment. Page-range operations on
`MatrixChunkService` use `can_accept_page_*`, `read_page_ranges`,
`write_page_ranges*`, and `discard_page_ranges*`. The corresponding fenced
Raft read methods also use `read_page_ranges_*`.

Older `slab`-named methods remain as deprecated forwarding methods for existing
callers; internal calls use the page names. Receipt proof sources use
`storage_receipt_page_parent_chunk_filter` to select a chunk. Its default still
honors an existing implementation of the deprecated filter hook. This naming
change does not alter persisted bytes, proof checksums, protocol headers, or
accepted legacy node-kind values.

### Binary API transport security

The chunk and metadata binary APIs support mutual TLS for local and replicated
storage. See [API TLS configuration and WSL validation](docs/api-tls.md).
