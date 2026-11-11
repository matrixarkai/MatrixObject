# Manifest route reply encoding

Metadata replies borrow their manifests and routes when calculating protobuf
lengths and writing their wire frames. The existing field numbers, versions,
ordering, zero-field omission and transport header are preserved. A complete
manifest-route frame is written directly into one correctly sized output buffer;
encoding no longer constructs cloned envelope payloads and then copies a
separate payload buffer into the frame.

The same path serves a single manifest and native batches. Request bounds,
manifest generation selection, routing fences, object order and reader lease
admission are unchanged. Durable outcomes and checkpoint formats are unchanged.
The decoder retains its shared canonical protobuf check and removes a second,
redundant re-encoding of the same reply.

## Allocation measurement

The explicit workload constructs inputs before counting. It measures only the
calling thread's allocation requests during payload encoding, complete frame
encoding and decode. The fixture has one or four objects with one or 4,096 extents
each. Extents describe one byte each, a group name and SHA-256; routes are omitted to
isolate manifest envelope costs. Fenced replies use the same policy and contents
before and after the change.

The initial storage code is 1476ccdb. Both measurements use the release profile
and all features on local WSL. Allocated bytes are cumulative allocator requests,
including reallocations; they are not peak resident memory. Frame byte counts
include the six-byte transport header. Input construction, catalog lookup,
grant creation and data reads are excluded.

| Extents/object | Objects/reply | Frame allocations before | After | Allocated bytes before | After | Wire payload bytes |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 7 | 1 | 337 | 89 | 83 |
| 1 | 4 | 23 | 1 | 1,894 | 348 | 342 |
| 4096 | 1 | 8,197 | 1 | 1,101,390 | 241,443 | 241,437 |
| 4096 | 4 | 32,783 | 1 | 4,406,122 | 965,772 | 965,766 |

Every updated payload encoder and complete-frame encoder made one allocation,
for exactly its output buffer length. The CI guard permits at most one allocation
and no more requested bytes than that output length, independently of extent
count. Payload bytes and complete frames also match the derived protobuf
envelopes across length boundaries, absent/present routes, Star and Raft route
descriptions, and legacy/fenced versions.

Removing the duplicate canonical pass saves one allocation of a complete reply
payload during decode. Field decoding, validation and one canonical re-encoding
still allocate. The workload measures these separately; reply construction in
the metadata service and storage for retained manifests are additional costs.

## Reproduce

```sh
cargo test --release --locked --all-features --test manifest_route_encoding_allocations \
  -- --ignored --nocapture --test-threads=1
```

Require one passed measurement and four ROUTE_ENCODING_ALLOC rows. Normal CI runs
the non-ignored output-buffer allocation guard and the two wire compatibility
tests:

```sh
cargo test --locked --all-features --lib manifest_route_encoding -- --test-threads=1
cargo test --locked --all-features --test manifest_route_encoding_allocations -- --test-threads=1
```

The malformed-input checks retain rejection of duplicate fields, unknown fields,
overlong varints, explicit default fields and truncated messages. The replicated
workload in [leased-read-scale.md](leased-read-scale.md) exercises complete reads,
lease cleanup and admission across three metadata voters and three Star replicas.
Its first published timing sample remains recorded against its original storage
revision; shared-host timing variation requires separate profiling before
attributing a throughput change to this encoder.

The updated release build also completed all 18 replicated leased-read
configurations with verified payloads, empty retained catalogs after each phase,
and no data-journal growth. These checks establish continued read behavior;
the allocation figures above isolate the measured encoding cost.
