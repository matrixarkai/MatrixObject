# Forgetting the tombstones of never-reused keys

A delete publishes a tombstone that stays in the metadata catalog, counts against its object limit
and takes space in the catalog image. The tombstone exists for two reasons: generations never repeat
for a key across delete and recreate, and a create that was built before the delete is refused. A
workload that creates a key once, deletes it later and never touches it again (a write-ahead log whose
segments are named by an ever-growing index) therefore fills the catalog with tombstones at the rate it
rolls segments, whatever its live size.

This page describes the opt-in forget-prefix behavior of the placement-manifest catalog.
It is off by default and does not retire initialized native objects.

## The contract

A key may be listed as a forget prefix only if **nobody ever creates it twice and no create of it can
arrive after its delete**. The catalog cannot check this. If a key under a forget prefix is created again,
it restarts at generation 1, and anything that treats a generation as identity can be fooled: a client's
cached ETag (`gen-N`), a leased read session, a copy source, a retained manifest or a staged allocation
that names the old incarnation. A late create from a fenced writer would also succeed instead of being
refused. **Ordinary objects must not be listed.**

## What it does

* `MATRIXOBJECT_METASERVER_FORGET_PREFIXES` is a comma-separated list of `bucket/key-prefix`. Unset
  (the default) forgets nothing and every byte of every outcome and snapshot is what it was without the
  feature.
* It takes effect only after the cluster has raised the forget-prefix feature bit (bit 5). The bit is never
  raised by default or by bringup. Every voter must declare it first, because a voter that does not know the
  field refuses the log entry that carries it: raise it only when every voter runs a build that knows it.
* The decision is the leader's. While it prepares a plain delete of a live object under a forget prefix, and
  only then, it sets `forget` (placement outcome field 20) in the outcome; voters never re-decide, so a
  follower configured differently cannot change a committed decision. If that follower later becomes
  leader, its own list governs later deletes. Apply drops the tombstone and the placement record.
* The leader does not forget a key that its own state still names (a retained manifest, which includes every
  read session and lease, or a staged allocation): that delete is the ordinary tombstone, and is not an
  error. It cannot see a gateway's or a client's cache; the contract covers those.

## What it does not do

* It does not help ordinary objects, and it is not a general tombstone expiry. Dropping tombstones for
  arbitrary keys needs a retention age and a generation floor, which this change does not include.
* It does not raise the object limit or the 64 MiB image bound.

## Without the feature

These need no format change and only slow the growth:

* Larger compact WAL segments reduce key churn per append. For example,
  `TS_SHARED_STORE_WAL_SEGMENT_APPENDS=2048` rolls eight times fewer segments than 256 appends
  per segment, for the same append count.
* Put the log bucket on its own cell, so its tombstones cannot fill the catalog of ordinary data.

For a workload that deletes two uniquely named keys per segment, estimated time until
100,000 tombstones at 10 / 100 / 1,000 appends per second is about 15 days / 1.5 days /
3.6 hours with 256-append segments, and 118 days / 12 days / 28 hours with 2048-append
segments. These estimates assume a steady append rate and two tombstones per rolled
segment; they are capacity examples, not measured retention times or a claim about
every WAL configuration.
