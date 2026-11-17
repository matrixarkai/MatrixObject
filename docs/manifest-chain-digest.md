# The manifest digest is a hash chain over the extents

A compact append binds the manifest it extends and the manifest it produces by digest.
The digest is a chain over the extents, not a hash of the whole encoding, so an append of
`k` extents costs `k` hash steps and one header hash however many extents the manifest
already holds.

```text
c_0  = SHA-256("MOC0")
c_i  = SHA-256("MOC1" || c_{i-1} || encoding(extent_i))
D(m) = SHA-256("MOC2" || encoding(header(m)) || c_n || n as u64 little endian)
```

`encoding` is the canonical protobuf encoding; `header(m)` is the manifest without its
extents (format version, bucket, key, generation, length, deleted). `D` binds the identity,
generation, length, extent count and every extent in order. The three prefixes keep the
three uses of the hash unambiguous.

The metadata catalog keeps the chain head and a checkpoint for every 64th extent beside
each stored manifest. An append carries the previous entry's chain forward and extends it
by the new extents; the client does the same from its own last append. A stored chain is
computed from the extents the first time it is needed (after recovery, or for an object
never appended to since the process started) and checked against the decision's digest
before an append is accepted: a stale or altered chain makes the append fail, because the
digest of the result is built from it.

The same digest is the read-session witness and the digest in the append planning request.

`manifest_chain.rs` pins `D` for 0, 1, 64, 65 and 4096 extents; the pinned values were
produced by an independent implementation, and the placement outcome version moved from 11
to 12 when the definition replaced the earlier digest of the whole encoding.
