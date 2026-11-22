# Buckets and object features on the chunk path

The cluster gateway stores objects as chunk manifests. On top of the object verbs it serves what an S3
client expects of a bucket and of an object: buckets that are created and configured, content type,
metadata and tags, object lock, quotas, CORS, lifecycle rules, versioning and a change feed. This page says
what each one does, where it is stored, and what it does not promise.

Everything here applies when buckets are explicit (`MATRIXOBJECT_REQUIRE_BUCKETS`, on by default in
`object_server`). A gateway embedded in a process that never creates buckets leaves them implicit and none
of the bucket rules below are read for its requests.

## Buckets

A bucket is a record, [`BucketConfig`](../src/bucket_config.rs), stored as an ordinary chunk object in the
reserved bucket `matrixobject-system` under the key `bucket/<name>`. It exists exactly when its record does, so
every gateway on one cluster sees the same buckets and the same configuration. A change is a compare-and-swap
on the record's generation, retried when another writer got in first, so two clients configuring one bucket
never overwrite each other. The reserved bucket cannot be addressed by a client.

| Request | Answer |
|---|---|
| `PUT /{b}` | creates the bucket; creating one that exists succeeds |
| `HEAD /{b}` | 200, or 404 `NoSuchBucket` |
| `GET /` | every bucket |
| `DELETE /{b}` | 409 `BucketNotEmpty` while a live object or a kept version remains, else 204 |
| a request to a bucket that does not exist | 404 `NoSuchBucket` |
| `GET`/`PUT`/`DELETE /{b}?versioning`, `?tagging`, `?cors`, `?policy`, `?lifecycle`, `?object-lock`, `?quota`, `?changefeed` | the configuration, stored in the record |

A bucket record is an entry of the metadata catalog like any object, so it counts against the catalog's capacity
(`MATRIXOBJECT_METASERVER_MAX_OBJECTS`): a catalog bounded at 8 entries holds 7 objects beside one bucket record.

A gateway remembers a bucket's configuration for 2 seconds, so a change made through another gateway is seen
there within that time; one made through this gateway is seen at once.

## Object attributes

An object carries, in its manifest, the content type it was written with, its `x-amz-meta-*` metadata, up
to 10 tags, a retention (mode and time), a legal hold, and the time it was written. They are part of the
generation the catalog publishes.

- `PUT` with `Content-Type`, `x-amz-meta-*` and `x-amz-tagging` stores them; `GET` and `HEAD` return them
  (`x-amz-tagging-count`, `Last-Modified`). A write names its own attributes: an overwrite does not inherit
  the old ones. An append keeps what the object carries and stamps a new time.
- `GET`/`PUT`/`DELETE /{b}/{k}?tagging`, `GET`/`PUT ?retention` and `GET`/`PUT ?legal-hold` read and change
  them without moving any data: the same extents are published again one generation later.
- Closed-range reads of a log (`Range: bytes=a-b`) skip the attribute headers, because they are the hot read
  of a shared log and answer without reading the manifest. A plain `GET`, a `HEAD` and an open range carry them.
- A multipart upload carries what was named when it began (content type, metadata, tags, retention): the record of the upload
  holds them and the assembled object is published with them. Completing it is a write like any other: a lock, a quota and
  versioning apply to the object it replaces.

## Object lock

A bucket created with object lock (`PUT /{b}?object-lock`) lets objects carry a retention and a legal hold:
`x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date` (an ISO 8601 time in the future) or
`?retention`, and `x-amz-object-lock-legal-hold: ON` or `?legal-hold`. A default retention in the bucket's
configuration applies to a write that names none. Where object lock is not enabled the headers are refused
(400), so no object there can be locked and a plain write costs no extra read.

While an object is locked its overwrite, its delete, the batch delete of it, a copy onto it and an append to it
are refused with 403 `AccessDenied`. A GOVERNANCE retention may be bypassed with
`x-amz-bypass-governance-retention: true`; a COMPLIANCE retention and a legal hold may not. A retention in
force is only lengthened, unless it is GOVERNANCE and the caller bypasses it.

The catalog's decision is clock-free, so the gateway enforces the lock: it reads the manifest with its
attributes, refuses when the object is locked at the time of the request, and otherwise publishes on top of
exactly the generation it read. A change to the attributes in between is a new generation, so the publication is
refused and the request starts again.

## Quotas

`PUT /{b}?quota` with `<Quota><Bytes>N</Bytes></Quota>` bounds the live bytes of a bucket. A write that would pass
it (an overwrite counts the bytes it frees) is refused with 403 `QuotaExceeded`, and nothing of it is stored.

The usage a gateway compares against is what it listed at most 5 seconds ago plus what it has written since.
Two gateways, or two writers in one interval, each see the usage as it was, so together they can pass the quota
by what they add within one interval. It is a bound that holds in steady state, not a lock. Kept versions do
not count.

## CORS

The rules of `?cors` are applied to requests with an `Origin` header: the first rule that allows the origin and
the method adds `Access-Control-Allow-Origin` (and `Expose-Headers`, `Vary: Origin`). A preflight
(`OPTIONS` with `Access-Control-Request-Method`) is answered from the rule, with `Allow-Methods`,
`Allow-Headers` and `Max-Age`, or refused with 403 when no rule allows it, as it is for a bucket without rules.
A request without an `Origin` header costs nothing here.

## Versioning

With versioning enabled, the manifest an overwrite or a delete is about to replace is first kept as a second
manifest under the hidden key `.versions/<key>/<generation>`: the same extents and attributes under a new name.
No data is copied, because a manifest names pages and two manifests may name the same ones, and the reclaim
sweep frees a page only when no manifest names it. A version's id is the generation it had when it was current
(`x-amz-version-id`). A delete publishes a tombstone, the delete marker (`x-amz-delete-marker: true`), which is
the current version until something is written and is kept like any other version when it is replaced.

| Request | Answer |
|---|---|
| `GET`/`HEAD /{b}/{k}?versionId=N` | that version, with the attributes it carried; a delete marker answers 405 |
| `DELETE /{b}/{k}?versionId=N` | removes that version for good (an object lock on it applies); deleting the current version brings the latest kept one back as current |
| `GET /{b}?versions` (`prefix`, `key-marker`, `max-keys`) | every version and delete marker, newest first within a key |

The keep and the replacement are two publications; a crash between them leaves a retained copy of what is
still current, and the retry keeps the same generation under the same hidden key, which already exists and is
accepted. An append extends the current version in place and keeps no copy. A bucket that holds only kept
versions is not empty.

## Lifecycle

`PUT /{b}?lifecycle` rules are acted on by a pass the door runs every `MATRIXOBJECT_LIFECYCLE_INTERVAL_MS`
(default one hour; `0` turns it off): an expiration deletes the objects under the rule's prefix last written that
many days ago (in a versioned bucket it leaves a delete marker), a noncurrent expiration deletes kept versions
that stopped being current that many days ago, and an abort rule removes multipart uploads begun that many days
ago and never completed. An object an object lock still protects is left alone and counted. A pass is safe to
repeat and to run from several doors at once, because every deletion is conditional on the manifest it decided
from. Objects written before the write time was recorded have no age and are never expired.

## Change feed

A bucket opts in with `PUT /{b}?changefeed` (`<ChangeFeedConfiguration><Status>Enabled</Status>`). From then on
every put, append, copy, delete and version delete in it writes one small entry, an ordinary object under the
hidden prefix `.changes/`; its key is its cursor (the writer's clock in milliseconds, then an id unique per
process and call).

- `GET /{b}?changes&after=<cursor>&limit=N` returns the entries after a cursor (`Operation`, `Key`,
  `VersionId`, `Size`, `Time`, `Cursor`) and the `NextCursor` to continue from.
- `DELETE /{b}?changes&before=<cursor>` drops the entries up to and including one, so a consumer that has
  processed them frees them.

An entry is written after its mutation succeeded, so a gateway that dies between the two leaves a mutation with
no entry: the feed is at-most-once per mutation, never invents one, and a failed write of an entry is logged and
does not fail the request. Entries are ordered by the clock of the gateway that wrote them, so a consumer that
must see every entry reads from a cursor a little behind the last one it saw; the entries name the key and the
version and are safe to replay.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `MATRIXOBJECT_REQUIRE_BUCKETS` | `1` | a request to a bucket that was never created is refused with `NoSuchBucket`; `0` keeps buckets implicit and turns every rule on this page off |
| `MATRIXOBJECT_LIFECYCLE_INTERVAL_MS` | `3600000` | time between lifecycle passes, 1000 to 86400000; `0` turns them off |
