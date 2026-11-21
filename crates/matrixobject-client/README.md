# matrixobject-client

The client SDK for the MatrixObject S3 door. Apache-2.0, std-only.

`wire` is the contract the gateway and every client share: the header and query names, the attempt
id, the resolve request and its three answers. The gateway links this crate, so a change to the
contract is one change that both sides compile against.

```bash
cd crates/matrixobject-client
cargo test --locked
```

See `docs/cluster-gateway.md` ("Appends that may have landed: attempt ids") for what the contract
means.
