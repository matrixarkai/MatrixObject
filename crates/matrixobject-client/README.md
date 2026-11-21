# matrixobject-client

The client SDK for the MatrixObject S3 door. Apache-2.0, two small dependencies (`sha2`,
`getrandom`).

* `wire` is the contract the gateway and every client share: the header and query names, the
  attempt id, the resolve request and its three answers, and what an append's status means. The
  gateway links this crate, so a change to the contract is one change that both sides compile
  against.
* `S3Client` is a blocking client of one bucket: `put`, `get`, `get_range`, `get_range_from`,
  `length_or_zero`, `delete`, a paged `list`, and `append`. An append that may have landed is
  settled by its attempt id, never by comparing bytes, and reports where its bytes landed.
* `Transport` carries one request to the door. `TcpTransport` is HTTP/1.1 over pooled keep-alive
  sockets and counts what crossed the wire; a door served in the same process implements the trait
  directly, so both ways of reaching a cluster run the same client code.

```rust
use matrixobject_client::{S3Client, TcpTransport};

let client = S3Client::new(TcpTransport::new("127.0.0.1:9000"), "logs");
let receipt = client.append("wal/0001", b"record")?;
assert_eq!(receipt.start_offset + 6, receipt.end_offset);
# Ok::<(), matrixobject_client::ClientError>(())
```

```bash
cd crates/matrixobject-client
cargo test --locked
```

See `docs/cluster-gateway.md` ("Appends that may have landed: attempt ids") for what the contract
means.
