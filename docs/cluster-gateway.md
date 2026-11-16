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

## Concurrency

The SDK client takes `&mut self`, so the gateway keeps a small pool of metadata connections and
lends one to each request. Accept threads serve one connection at a time (as the in-process
door does), so `MATRIXOBJECT_THREADS` bounds concurrent clients.
