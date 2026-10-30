# Binary API mutual TLS

Build `chunkserver`, `metaserver`, and `distributed_probe` with `api-tls`
(or `peer-tls`) in addition to their storage and consensus features.
API TLS wraps the existing binary protobuf protocol. It works with local
journals, metadata snapshots, star replication, and Raft-backed data and metadata.
The HTTP/S3 frontend has separate transport behavior; these settings do not secure it.

Set all three paths on each API server:

```sh
export MATRIXOBJECT_API_TLS_CA_CERT=/etc/matrixobject/api-ca.pem
export MATRIXOBJECT_API_TLS_CERT=/etc/matrixobject/node.pem
export MATRIXOBJECT_API_TLS_KEY=/etc/matrixobject/node.key
```

Set the client paths for `distributed_probe` and for chunk-server heartbeats:

```sh
export MATRIXOBJECT_CLIENT_TLS_CA_CERT=/etc/matrixobject/api-ca.pem
export MATRIXOBJECT_CLIENT_TLS_CERT=/etc/matrixobject/client.pem
export MATRIXOBJECT_CLIENT_TLS_KEY=/etc/matrixobject/client.key
```

Every TLS client must present a certificate trusted by the server CA. Server
certificates must contain the endpoint IP in a subject alternative name. TLS
clients require numeric IP socket addresses. Certificates used in both roles
must allow server and client authentication. Keep private keys outside the
repository and restrict their filesystem permissions.

The transport uses TLS 1.3, verifies server identity and client certificates,
and requires the `matrixobject-api-v1` ALPN protocol. It disables early data
and session resumption. API and Raft ALPN values differ; API credentials do
not make an API connection usable as a Raft connection. Use separate trust
roots for API clients and Raft peers when they have different trust policies.
Configure Raft TLS independently with the existing chunk and metadata Raft
TLS settings.

For Rust clients, load `ApiTlsConfig` once from `ApiTlsOptions` and clone it
into `ReplicaClientOptions::tls` and `MetaClientOptions::tls`. Servers accept
the same configuration through `ReplicaNodeOptions::tls` and `MetaNodeOptions::tls`.
Loading validates the complete CA/certificate/key set. Partial environment
settings, invalid files, and TLS settings in a build without TLS support fail
startup instead of falling back to plaintext. No TLS settings means plaintext
for a new store. Configuration is loaded at startup; restart with replacement
credentials to rotate them.

TLS handshakes share the client's connect deadline. Encrypted frame I/O retains
the existing read, write, and absolute operation deadlines. Partial TLS records
continue within the same deadline without replaying the application request.
The API server services handshakes through its bounded connection loop.

Once durable storage is opened with API TLS, an `api.transport` marker in the
metadata or Raft directory, or a `.api.transport` sidecar next to a local chunk
journal, requires TLS on subsequent opens. Preserve these files when backing up
or moving the store. Missing credentials or an invalid marker prevent startup.
This guards against accidental plaintext restart; it does not protect files
from an administrator who can alter the storage directory.

Trust is cluster-wide: any accepted client certificate can invoke the API.
There are no per-user, bucket, or object ACLs in this transport layer.

## WSL validation

Run the process tests in the shared WSL checkout:

```sh
cargo test --locked --all-features --test api_tls_processes
cargo test --locked --all-features --test network_chunk_consensus authenticated_api_and_peer_objects_recover_after_restart
bash tools/network_partition_test.sh
```

The encrypted recovery case starts three metadata voters and two three-voter
data groups. It verifies objects through leader loss and full process restart.
The partition script uses a private network namespace and also checks live
isolated leaders, quorum refusal, healing, and recovered objects with API and
Raft TLS enabled. These are local multi-process simulations.

For concurrent object loads, the regular test runs 15 processes and places each
object across four groups. An explicit scale case runs 51 processes: three
metadata voters and 16 three-voter data groups. It writes 128 one-MiB objects,
removes every group leader, verifies the objects, writes another 32 objects,
and verifies all 160 after a complete restart. Both API and Raft traffic use TLS.
Every node's persisted transport markers and each group's physical and committed
capacity accounting are checked. The workload retains a ten-second deadline per
object operation and does not retry uncertain mutations.

```sh
cargo test --locked --all-features --test network_chunk_consensus coupled_groups::encrypted_api_load_recovers_four_way_objects -- --exact --nocapture
cargo test --release --locked --all-features --test network_chunk_consensus coupled_groups::encrypted_api_sixteen_groups_recover_four_way_objects -- --exact --ignored --nocapture
```

The output reports write latency and throughput, verified read throughput,
process count, and recovered object count. Run scale cases individually in WSL
to avoid competing process workloads. These measurements describe this bounded
local simulation, not independent-host throughput or a production capacity limit.

## Measured encrypted scale run

An October 30, 2026 Ubuntu WSL release run at commit
`889d3a6198cfee10fcba23295fead371b42ba911` passed the explicit 51-process
case above in 32.68 seconds, excluding compilation. Four clients wrote one-MiB
objects, each split across four of the 16 data groups. The run verified all
160 objects after complete process restart, including 32 written after leader
loss. API and Raft peer mutual TLS were enabled together.

| Phase | Count | Elapsed | Rate | p95 operation latency |
|---|---:|---:|---:|---:|
| Initial writes | 128 objects | 16.495 s | 7.8 objects/s | 827.150 ms |
| Reads after leader loss | 128 objects | 1.206 s | 106.06 MiB/s | 91.815 ms |
| Writes after leader loss | 32 objects | 3.474 s | 9.2 objects/s | 602.518 ms |
| Reads after full restart | 160 objects | 1.717 s | 93.18 MiB/s | 107.212 ms |

This is one shared-host functional scale measurement with fixed placement groups.
It does not establish behavior during sustained host I/O pressure, independent
failure domains, or catalog-managed voter replacement. Those remain separate
validation and integration requirements.
