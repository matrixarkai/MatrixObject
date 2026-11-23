# Whole object append placement and publication

The object API accepts a request body up to 64 MiB. Native LOG chunks also hold up
to 64 MiB; the writer splits a body into records up to 1 MiB. Durable record
prefixes alone do not establish atomic visibility of the original object request.

## Placement

`cluster::plan_object_append` places the complete request in the current tail if
it fits. Otherwise it selects a new tail starting at the current object end.
The caller seals the settled old tail and publishes the new empty tail before
sending any record of the request. An exact fit stays in the current tail. A
64 MiB request following a nonempty tail switches early and remains one request.
No padding is required, and the request size limit remains unchanged.

The helper is allocation-free and checks the expected object end, invalid sizes,
and logical overflow before selecting placement. Its tail length must describe
settled, published bytes; the caller must first resolve any pending attempt.
It does not acquire a lease, issue a metadata operation, or publish data.

## Remaining publication work

A native object implementation needs a durable boundary for the whole request.
Intermediate records must preserve the prior visible object end; the final
record must establish the new end under its CRC. Both replay and live readers
must distinguish durable physical bytes from visible complete requests. Exact
attempt identity remains necessary for ownership; matching payloads do not
establish an attempt outcome.

Readers and protocol handlers must understand the new boundary before any writer
emits it. The normal single-record request should carry the boundary in its data
record rather than requiring a second write or per-append metadata update.
The durable encoding and capability gate have not been introduced by this change.

Early switching keeps one request out of multiple chunks. A crash after sealing
but before publishing a new tail still requires recovery: obtain and validate the
old footer, then finish the metadata switch without appending to the sealed file.
An incomplete new tail must not become a visible object suffix merely because a
quorum holds some records. The current native stream reader retains its existing
raw-prefix contract; it is not yet the default atomic object reader.

Overwrite and delete must fence cached writers before acknowledging the new
object state. HEAD, GET, list sizes and read sessions must use the visible complete
length. Reply-loss recovery in both SDK modes must resolve the original attempt,
incarnation and range. These requirements remain prerequisites for changing the
default object path.

## Validation

Nine focused tests include exact fit, early switch, the full-size request,
invalid sizes, stale offsets, overflow and exhaustive placement for capacities
1 through 64 bytes. These tests cover planning only; they do not claim durable
atomic publication, crash recovery or a measured latency improvement.
