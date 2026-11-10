# Star repair with replicated metadata

The metadata process suite exercises five journal-backed chunk replicas and three MatrixRaft metadata voters. All five replicas receive an initial page set. Two stop while the remaining quorum updates every page and records a discarded range. The stopped replicas return in maintenance mode and remain excluded from placement while repair restores their pages.

Repair traverses the union of inventories in batches of seven. Each result must identify the expected page and sequence, install both stale copies, retain the three matching source copies, and advance its cursor only through completed pages. After the first batch, the test terminates the metadata leader and continues against the new leader. Each batch verifies placement uses only the three serving replicas and refuses a fourth.

After repair, all eight processes stop. Metadata restarts before any chunk reporter, proving that placement exclusion is recovered from durable metadata. The chunk processes then restart from their journals and checkpoints. Every copy must retain its bytes, sequence and discarded-range behavior. Maintenance replicas still refuse writes. An explicit restart without maintenance enables all five replicas to acknowledge new writes.

The regular CI case uses 32 pages (160 verified copies). The explicit scale case uses 256 pages (1,280 verified copies), each carrying 1 KiB. Run it in the local WSL environment:

```sh
cargo test --release --locked --features chunk-consensus,metadata-consensus,api-tls --test network_metadata_consensus star_repair_metadata_scale::five_star_replicas_repair_256_pages_with_metadata_failover_and_restart -- --exact --ignored --nocapture --test-threads=1
```

These tests cover bounded repair traversal and recovery across independent local processes. They do not certify sustained throughput, independent-host failures, automatic repair scheduling, or the separate multi-group Raft membership workload.

The metadata-routed object case also has an explicit WSL scale run:

```sh
cargo test --locked --features metadata-consensus,replica-server --test network_metadata_consensus standalone_star_quorum_writes_and_recovers_256_object_pages -- --exact --ignored --nocapture --test-threads=1
```

It writes a 16 MiB object as 256 independent 64 KiB pages through three journal-backed star members and three metadata voters, reads the committed object, repairs all 256 pages in four bounded passes, verifies all 768 copies, then checks a member loss, rejoin, and minority refusal. One local debug run completed the initial write in 3.0 seconds and the complete scenario in 343.7 seconds. The complete duration includes repair, process restarts, and reads; it is not a sustained write throughput figure. Automatic repair scheduling and independent-host failure testing remain open work.
