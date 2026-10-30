# Mutual TLS for Raft peers

Build with `--features chunk-consensus,metadata-consensus,peer-tls` on Rust 1.89
or newer. `peer-tls` is optional. It protects Raft peer connections for both
metadata and chunk voters; it does not protect the separate metadata, chunk or
HTTP client APIs. Keep those APIs on a trusted network until their transport and
authorization are configured separately.

Supply all three PEM file paths on each voter before startup:

| Metadata voter | Chunk voter | Contents |
|---|---|---|
| `MATRIXOBJECT_METASERVER_RAFT_TLS_CA_CERT` | `MATRIXOBJECT_CHUNK_RAFT_TLS_CA_CERT` | Trusted cluster CA certificates |
| `MATRIXOBJECT_METASERVER_RAFT_TLS_CERT` | `MATRIXOBJECT_CHUNK_RAFT_TLS_CERT` | This voter's leaf certificate followed by its intermediates |
| `MATRIXOBJECT_METASERVER_RAFT_TLS_KEY` | `MATRIXOBJECT_CHUNK_RAFT_TLS_KEY` | This voter's unencrypted private key, readable only by the service account |

Partial or empty settings, unreadable files, malformed certificates and mismatched
certificate/key pairs fail startup. TLS settings without Raft configuration or
without the `peer-tls` build feature are refused. Individual files are limited to
1 MiB. Files are read once at startup; restart a voter to load rotated material.
No certificate or private-key contents are written to logs or consensus records.

Use a dedicated CA that issues certificates only to authorized cluster members.
Every peer must present a trusted client certificate. Each server certificate
must include the configured peer IP address as an IP subject alternative name;
the common name is not a substitute. Leaf certificates need server and client
authentication usage. The transport authenticates CA-authorized members, not a
certificate-to-numeric-voter-ID mapping. It does not provide Byzantine fault
tolerance or protect against a compromised authorized member.

The implementation uses rustls with TLS 1.3, mandatory client authentication,
address validation and the `matrixobject-raft-v1` application protocol. Early data
and session resumption are disabled. There is no plaintext fallback. Both the
four-byte length and the existing binary Raft packet are encrypted; the packet
and durable outcome encodings do not change.

The existing 100 ms outbound deadline includes queueing, TCP connection, TLS
handshake and encrypted writes. The receiver's 100 ms deadline includes its
handshake and frame receipt. There are still at most 16 active receive sockets,
an 8 MiB packet limit and a 16 MiB plaintext frame budget. TLS has additional
record and handshake buffers; this is not a bound on total process memory.
Slow or failed handshakes are dropped and Raft handles later retransmission.
No application mutation is replayed by the transport.

On first successful TLS configuration, the locked voter state directory gains a
synced `peer.transport` marker. This version refuses to reopen that directory in
plaintext mode while that marker exists, or with an invalid marker value. Missing TLS settings never
silently downgrade an already configured voter. The marker is an additional file,
not a WAL migration; keep it with the other state files. Older binaries do not
understand this guard, so do not roll a TLS voter back to an older binary.

For an existing plaintext deployment, use a controlled cluster stop, configure
all voters and restart with TLS enabled. Mixed TLS/plaintext members cannot
exchange peer packets. Preserve all state and bootstrap settings. Certificate
rotation uses an overlap of trusted CA certificates when changing authorities;
remove the old authority only after all members have switched and been verified.

WSL tests generate short-lived test certificates locally with OpenSSL and delete
their temporary directories afterward. No private keys are committed. Tests cover
trusted frames, missing/untrusted client identity, wrong server address or CA,
plaintext rejection, downgrade rejection, stalled handshakes, and coupled
metadata/data leader-loss and full-restart recovery.

Rust callers set `peer_tls` on `MetaRaftOptions` or `ChunkRaftOptions` to
`Some(PeerTlsOptions { ca_certificate, certificate, private_key })`, or `None`
for a new plaintext voter. Existing option struct literals must add this field.

The coupled recovery guard also exercises five and seven voters per group:
one metadata group and three data groups, for 20 and 28 local WSL processes.
It removes the current leader plus enough other voters to leave exactly a
majority in every group (two failures with five voters, three with seven), then
checks reads and new writes before restarting all voters from their own state.
Exact object bytes, manifest generations, write sequences, physical usage,
committed capacity and every voter TLS marker are verified. This exercises fixed
membership fault tolerance; it does not change membership during the run.
