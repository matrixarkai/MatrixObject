# Cluster-backed S3 gateway

`object_server` serves the replicated object plane: a metadata quorum plus chunk groups running as
separate processes, reached through the cluster SDK (`RemoteMetaClient::write_object`,
`append_object`, `read_object`, `read_object_range`, `delete_object`, `list_manifests`).
`MATRIXOBJECT_CLUSTER_META` is required, and the process refuses to start without it.

This door answers the S3 verbs a shared-log client needs, and the rest of what an S3 client
commonly uses (copy, multipart, batch delete, conditional requests, delimiter listings). Every
verb it does not serve is answered with an S3 `NotImplemented` error, so a client that probes can
fall back and nothing returns a plausible-looking wrong answer.

## Verbs served

| S3 request | SDK call |
|---|---|
| `PUT /{b}/{k}` | `read_manifest` for the expected generation, then `write_object` |
| `PUT /{b}/{k}` + `x-amz-write-offset-bytes: N` | offset check against the manifest length, then `append_object` (first append: `write_object` with no expected manifest) |
| `GET /{b}/{k}` | `read_object` |
| `GET /{b}/{k}` + `Range: bytes=a-b` | `read_object_range` straight away; clamping, open-ended and suffix ranges read the manifest first |
| `HEAD /{b}/{k}` | `read_manifest`; length in `x-amz-object-size` |
| `DELETE /{b}/{k}` | `delete_object` (tombstone generation); absent key answers 204 |
| `GET /{b}?list-type=2` (`prefix`, `start-after`, `continuation-token`, `max-keys`, `delimiter`) | `list_manifests` pages of 32, tombstones skipped; a `delimiter` folds keys into `CommonPrefixes` and counts each prefix toward `max-keys` |
| `PUT /{b}/{k}` + `If-Match` | the object must exist and be that version (etag `"gen-N"`, or `*`), else 412 |
| `GET`/`HEAD /{b}/{k}` + `If-None-Match` / `If-Match` | decided from the manifest's etag before any data is read: 304 / 412 |
| `PUT /{b}/{k}` + `x-amz-copy-source` | `copy_object_atomic` on the SDK plane: the destination names the source's extents, no data moves; the destination's generation is the compare-and-swap |
| `POST /{b}?delete` | one `delete_object` per key; an absent key is reported deleted |
| `POST /{b}/{k}?uploads`, `PUT ?partNumber=&uploadId=`, `POST ?uploadId=`, `DELETE ?uploadId=`, `GET ?uploadId=`, `GET /{b}?uploads` | multipart, below |
| `GET /` | the bucket the gateway was started for (buckets are implicit and cannot be enumerated) |
| `DELETE /{b}` | 409 `BucketNotEmpty` while any live object remains, else 204 |
| `GET /{b}?versioning`, `GET /{b}?acl`, `GET /{b}/{k}?acl`, `GET /{b}/{k}?tagging` | fixed answers: not versioned, private to the owner, no tags |
| `PUT /{b}`, `HEAD /{b}`, `GET /{b}?location` | answered without a cluster call: buckets are implicit |
| `GET /healthz` | one `list_manifests` call |

Refused with `NotImplemented`: changing tags, ACLs, versioning, CORS or any other bucket
configuration, `versionId`, `UploadPartCopy`, `versions`. `If-None-Match: *` on a PUT is honoured.

### Multipart uploads

An upload in progress is a set of ordinary objects under the reserved prefix `.multipart/`: a
record `.multipart/<id>/meta` (the target key and the time it began) and one object
`.multipart/<id>/<part number>` per part. Keys under the prefix are not served as objects and are
not listed. `CompleteMultipartUpload` validates the claimed parts as S3 does (ascending, each
named once, each etag the part the upload holds: `InvalidPartOrder`, `InvalidPart`), reads them,
and publishes ONE object under the target key, so the target appears whole or not at all, then
removes the parts and the record. Abort removes them. The assembled object is built in memory and
is bounded by the gateway's whole-object limit (`MATRIXOBJECT_MAX_BODY_BYTES`); a larger
completion is refused with 400 and the upload stays usable. A part's etag is its object's
generation, so uploading a part again replaces it and invalidates the old etag. Parts that could
not be removed after a completion are invisible and are removed by a later abort.

## What the retired embedded door answered, and where each is tested now

The embedded store had its own HTTP door. The cluster gateway answers the same S3 requests over chunk objects.
Each row says what happens now and which test proves it against real metadata and chunk processes (the same
scenarios also run against a fake plane in the gateway's unit tests, so a plane cannot change a verb's meaning).

| request | now | tested end to end by |
|---|---|---|
| `PUT`, `GET`, `HEAD`, `DELETE` of an object | served | `a_chunk_object_round_trips_through_a_star_quorum_and_a_wrong_digest_fails_its_read` (Star), `chunk_objects_over_raft_groups_survive_leader_loss_races_appends_and_total_restart` (Raft, through the client) |
| append (`x-amz-write-offset-bytes`) and its attempt-id resolve | served | the same Star test (append, wrong offset, resolve landed / ambiguous / absent, fence, retry lands once) |
| append through the loss of one data node | served by `append_chunk_object_with_failover` | `a_chunk_write_and_append_switch_groups_when_a_member_of_the_chosen_group_stops` |
| `Range` (closed, open-ended, suffix) | served | `the_s3_door_over_chunk_objects_serves_listing_conditions_copy_batch_multipart_and_ranges` |
| `ListObjectsV2` with prefix, delimiter, paging | served | the same test |
| `If-Match`, `If-None-Match` on writes and reads | served | the same test |
| copy (`x-amz-copy-source`) | served: the bytes are copied and the copy does not follow the source | the same test |
| `POST ?delete` (batch delete), `DELETE` of a bucket (409 while objects remain) | served | the same test |
| multipart upload (create, part, list, complete, abort) | served | the same test |
| `GET /healthz` | served | the same test |
| object tagging (change), bucket CORS, versioning (configuration, `versions`, `versionId`), ACLs | **not served**: answered `NotImplemented`, reads of tags, ACLs and versioning answer a fixed value | refusal is covered by the gateway's unit tests |
| object lock, retention, lifecycle, bucket policy, quotas, change feed | **not served**: these were methods of the embedded store's own API and never S3 requests the cluster gateway answered | none |

A chunk manifest holds only the object's latest generation, so versioning would need retained manifests
and is not on the chunk path. The Raft and Star group modes both carry chunk objects; the Star test runs against real
chunk servers, the Raft test against a Raft group of three voters per group.

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
* Concurrent appends to one object are serialised by the client, not hidden by the gateway. A writer
  that loses the race is refused with 412 and told the object's new length, and retries at it. The
  gateway never turns that refusal into a success because bytes equal to the append's are present: two
  writers appending identical bytes at one offset leave one copy, and only the attempt's own extents
  (its attempt id) say that it landed.
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
* The gateway is unauthenticated: anything that can reach its port can read, overwrite and delete.

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

## Reclaiming the pages no manifest names

A door stores objects as chunk manifests, and the pages of a
deleted object, of an overwritten generation and of a writer that died before publishing are named by no
manifest. The door runs a sweep that frees them, in a thread of its own process, with no setting needed:

| variable | default | meaning |
|---|---|---|
| `MATRIXOBJECT_RECLAIM` | `on` | `off` is the only way to turn it off |
| `MATRIXOBJECT_RECLAIM_GRACE_MS` | `3600000` | a page must stay unnamed this long before it is freed; 60000 to 604800000. Keep it longer than a write takes from its first page to its publication |
| `MATRIXOBJECT_RECLAIM_INTERVAL_MS` | a quarter of the grace | time between rounds, 1000 up to the grace |
| `MATRIXOBJECT_RECLAIM_ACK_LEAK` | unset | `1` lets a chunk-object door start with `MATRIXOBJECT_RECLAIM=off` |

A door that stores chunk objects with the sweep off **refuses to start** unless the leak is acknowledged. A
value that is out of range or not a number is refused, never replaced by the default.

Known limits:

* **N doors make N scans.** No door is elected to sweep. Every door that runs one reads the page inventory of
  every group and the whole chunk catalog each round. That is safe, because a discard is conditional on the
  page still having its first-write sequence and the catalog is read again just before each one, but it is
  repeated work. Electing one sweeper is a follow-up.
* **A restart delays freeing, never advances it.** What a sweeper has seen is in memory, so a restarted door
  starts every grace clock again.
* **Data directories from earlier builds are not supported.** This build stores and reads chunk objects
  only, and there is no migration. A door that finds a page-granular object that is not deleted when it
  starts refuses to start, with `older data directory not supported` and the number of such objects. The
  sweep also checks before every round (the counter `reclaim_refused_page_objects` counts the rounds it
  refused) because it takes a page that no chunk manifest names for garbage and would otherwise free the
  pages of such an object.
* **The digest check is integrity, not access control.** A manifest that names a page keeps it alive;
  publishing assumes trusted cluster clients.

## Concurrency

The SDK client takes `&mut self`, so the gateway keeps a small pool of metadata connections and
lends one to each request. Accept threads serve one connection at a time, so `MATRIXOBJECT_THREADS` bounds concurrent clients.

## Appends that may have landed: attempt ids

An append whose reply was lost (a timeout, a reset, a consensus deadline, a killed gateway) may or may not be in the
object. The gateway answers "did my append land?" by the attempt, never by the bytes: two writers that append identical
bytes at one offset leave one copy, and a byte comparison would tell both that theirs landed.

* An append may carry `x-amz-attempt-id`, 32 hex characters (16 random bytes) chosen by the client once per attempt and
  reused on every retry of that attempt. The gateway echoes it. A malformed id is a 400.
* Every attempt allocates its own pages, so the extents it publishes are its identity: the committed manifest holds
  them if and only if the attempt landed, even if later appends followed. When an append's outcome is unknown, the
  gateway checks that identity; it answers 200 only when the attempt's extents are in the manifest.
* `POST /bucket/key?append-resolve&attempt=<id>&offset=O&length=N[&sha256=<hex>]` settles an attempt later and answers
  `landed`, `absent` or `ambiguous` in `x-append-resolution`. `absent` is returned only after the attempt is fenced: the
  object is published again unchanged at the next generation, so a late copy of the attempt is refused, and the client
  may retry it as a new append. `ambiguous` means bytes equal to the append's are at that range but this gateway cannot
  prove they are this attempt's (it never saw the attempt, for instance after a restart); it is never reported as
  `landed`. The `attempt` parameter is required; `sha256` is optional evidence about the bytes.
* An append with an unknown outcome that the gateway cannot decide is a 409 `AppendAmbiguous` with
  `x-amz-append-state: ambiguous` and `x-amz-object-size`. The client decides from its own record of the length it last
  acknowledged: a client that is the object's only writer can, because the length then proves the bytes are its own.
* The gateway remembers the last 4096 attempts. After a restart it remembers none, so an attempt it served before is
  `ambiguous` or `absent` afterwards, never `landed`.
