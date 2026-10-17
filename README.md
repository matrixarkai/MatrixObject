# MatrixObjectStore Rust Core

This crate is the Rust implementation track for MatrixObjectStore. It mirrors the core object-store semantics used by the native services:

- bucket/object metadata
- large object segmentation into extents
- bounded batch object and range reads with per-item data/error results
- raft-backed bounded batch object copies with per-item results and byte-budget admission
- raft-backed bounded batch object deletes with per-item results and RootServer placement release
- batch object delete duplicate-key accounting with deterministic request-order execution
- raft-backed bounded prefix deletes with continuation markers and batch delete semantics
- paginated object inventory with placement records and payload-budget continuation
- object-range read and delete-discard admission probes aggregated from BlockServer scalar admission
- conditional object head/full/range reads with `If-Match` and `If-None-Match` style ETag preconditions
- raft-read-gated object admission probes for linearizable and shard-routed clients
- raft-read-gated object governance reads for tags, legal hold, and retention state, with shard-owner/epoch fenced variants
- raft-read-gated object tag listing for governance and lifecycle controllers, with shard-owner/epoch fenced variants
- raft-read-gated bucket inventory pages for gateways and control-plane controllers, with shard-owner/epoch fenced variants
- raft-read-gated object inventory pages for repair, placement, and migration controllers, with shard-owner/epoch fenced variants
- object inventory, version, and multipart listing controller reads expose compact metadata read proofs for lifecycle, repair, migration, and upload-cleanup handoff
- native object, inventory, version, and multipart listing proto/RPC responses carry consensus read proofs for distributed gateways and controllers
- native versioned object get/head proto/RPC responses carry consensus read proofs matching current-object read semantics
- Rust versioned object head/get/range reads expose linearizable and shard-routed compact read proofs for lifecycle and recovery controllers
- Rust scalar object head/get/range reads expose route-control checksum fenced proof wrappers, including combined MetaServer shard-owner/epoch plus BlockServer route-proof variants for shard-routed controllers
- Object range-read and delete-discard admission preflights expose compact metadata read proofs in Rust and native proto/RPC responses
- Scalar native object range cache-control proto/RPC responses carry consensus read proofs matching batch cache-control
- native batch object copy proto/RPC/client responses carry source-read consensus proofs for migration and repair copy waves
- native batch object delete proto/RPC/client responses carry metadata consensus proofs for fenced distributed delete waves
- native prefix object delete proto/RPC/client responses carry metadata consensus proofs across list-and-delete pages
- native object tag listing proto/RPC/client responses carry metadata consensus proofs for tag-driven lifecycle and migration controllers
- native placement migration, replica repair, decommission, and capacity rebalance proto/RPC/client responses carry metadata consensus proofs for distributed controller handoff
- native object changefeed and GC proto/RPC/client responses carry metadata consensus proofs for discard, compaction, and changefeed controller handoff
- Rust object changefeed and GC controller reads expose `ObjectReadProof` for linearizable discard, compaction, and changefeed handoff
- raft-read-gated object version listings for lifecycle, retention, and recovery controllers, with shard-owner/epoch fenced variants
- raft-read-gated multipart upload listings for gateways and lifecycle cleanup controllers, with shard-owner/epoch fenced variants
- raft-read-gated multipart part listings for upload completion gateways, with shard-owner/epoch fenced variants
- raft-read-gated raw metadata record gets and prefix scans for MetaServer controllers, with shard-owner/epoch fenced variants
- raft-read-gated raw metadata batch gets for MetaServer controllers, with per-key shard-owner/epoch fencing and bounded payload accounting
- raft-read-gated metadata read-set checks for conditional controllers, fencing every condition key for shard-routed clients
- raft-read-gated object metadata batch heads for MetaServer controllers, with missing-key reporting and no ChunkServer data-plane IO
- route-topology checksum fenced object metadata batch heads and metadata audits, with echoed expected shard epoch/checksum in Rust control objects
- raft-read-gated bucket metadata reads for versioning, retention, quota, and lifecycle policy, with shard-owner/epoch fenced variants for route-cached MetaServer clients
- raft-backed MetaServer route-cache capacity/TTL configuration with direct and prepared checksum-fenced raft apply paths plus snapshot durability
- raft-backed MetaServer membership planning and rollout drive multi-voter targets through bounded prepared one-voter phases, preserving commit-index and checksum fences while reporting partial progress and read-index refresh requirements
- raft-backed object changefeed records with durable consumer cursor commits
- batch block write/read/discard APIs with same-chunk read/discard coalescing and ordered read responses
- raft-gated BlockServer batch write admission that mirrors `CanWriteBlockBatch` control-plane preflight without mutations
- Rust BlockServer batch write admission exports selected-write counts, byte
  budgets, and continuation cursors for resumable large writes
- Rust BlockServer batch write admission accepts cursor-start resume calls
  through direct, fenced, and raft-read-gated controller APIs
- Rust BlockServer strict batch write admission can require the admission proof
  checksum for a resumed cursor window
- bounded prepared BlockServer batch write raft entries with admission-budget fences
- raft-gated BlockServer batch read admission that mirrors `CanReadBlockBatch` control-plane preflight without payload reads
- raft-gated BlockServer batch discard admission that mirrors `CanDiscardBlockBatch` GC/stale-range preflight without mutations
- Rust BlockServer strict batch discard admission can require the admission proof
  checksum for a resumed discard cursor window
- BlockServer and ChunkServer batch admission preflights expose compact route/control proof wrappers for write/read/discard controllers
- bounded prepared BlockServer batch discard raft entries with admission-budget fences
- route-control checksum fenced BlockServer scalar/batch write and discard mutations, including prepared raft entries
- BlockServer read batching reports coalesced logical reads and cold physical ChunkServer reads
- ChunkServer direct batch reads coalesce adjacent same-chunk ranges and preserve ordered range responses
- raft-gated ChunkServer batch read admission that mirrors `CanReadChunkBatch` before payload reads
- raft-gated ChunkServer batch write admission that mirrors `CanWriteChunkBatch` for prepared replica writes
- Rust ChunkServer batch read/write/discard admissions export budget,
  continuation cursor, and conflict fields in control maps for distributed
  schedulers
- bounded prepared ChunkServer batch write raft entries with admission-budget fences
- control-checksum fenced prepared ChunkServer batch write/discard raft entries
- raft-gated ChunkServer batch discard admission that mirrors `CanDiscardChunkBatch` for prepared replica discards
- bounded prepared ChunkServer batch discard raft entries with admission-budget fences
- ChunkServer batch heads coalesce duplicate chunk ids and report physical/coalesced metadata-head counts
- ChunkServer scalar and batch metadata-head reads expose compact storage read proofs for repair and cache controllers
- ChunkServer, BlockServer, and object-range scalar reads expose response-frame budget fences
- ChunkServer batch write coalescing with physical write-count/byte accounting propagated through BlockServer
- ChunkServer batch discard coalescing with physical discard-range/byte accounting propagated through BlockServer
- ChunkServer snapshot-durable cumulative physical write/discard counters with BlockServer and ObjectStore aggregate accessors
- ChunkServer EC sidecars with two data shards plus XOR parity for corrupt-payload read recovery and repair
- ChunkServer scalar reads slice verified healthy payloads without cloning the full chunk, while preserving EC fallback recovery
- BlockServer anti-entropy prefers local ChunkServer EC payload repair before cross-replica quorum handoff
- ChunkServer durable idempotent scalar write/discard receipts with snapshot-safe retry accounting
- ChunkServer durable idempotent batch write/discard receipts with snapshot-safe retry accounting
- ChunkServer and BlockServer receipt retention controls bound durable retry metadata while preserving snapshot-safe receipt paging
- BlockServer allocation ledger with pending reservations, idempotent allocation retry receipts, release cleanup, and snapshot durability
- BlockServer allocation release cleanup has idempotent retry receipts, snapshot durability, and raft proposal replay
- BlockServer scalar write/read/discard admission probes matching the native `Can*Block` service paths
- raft-backed BlockServer idempotent segment-write receipts with checksum fences and snapshot durability
- raft-backed BlockServer idempotent batch-write receipts with ordered range and aggregate payload checksums
- raft-backed BlockServer idempotent segment-discard receipts with checksum fences and snapshot durability
- raft-backed BlockServer idempotent batch-discard receipts with ordered range checksums
- raft-backed prepared BlockServer batch write/discard entries with projected service images and commit-index fences
- raft-backed linearizable BlockServer and ChunkServer receipt audit pages for allocation, write, discard, and batch mutation reconciliation
- BlockServer and ChunkServer receipt audit pages expose compact receipt read proofs for distributed repair/replay controllers
- Rust BlockServer allocation and release-allocation receipt audit pages expose operation and page proof checksums for allocator-ledger reconciliation
- Rust BlockServer write/discard receipt audit pages expose page-level proof checksums over pagination, segment filters, and emitted receipt proofs for distributed replay controllers
- BlockServer idempotent scalar write/discard fanout uses durable per-ChunkServer receipt keys
- BlockServer idempotent batch write/discard fanout uses durable per-ChunkServer receipt keys
- Split native BlockServer/ChunkServer service adapters reject malformed ids, range overflow, and chunk-RPC length truncation before pipeline fanout, while preserving zero-length discard as a no-op
- bounded LRU ChunkServer read-through range cache with BlockServer/ObjectStore stats and invalidation controls
- ChunkServer read-cache entries carry range checksums and self-evict before serving corrupt cached payloads
- raft-backed ChunkServer read-cache capacity configuration with direct and prepared checksum-fenced raft apply paths plus snapshot durability
- Rust raft-backed ChunkServer prepared capacity, service-mode, trim-flow, mutation-QoS, and read-cache controls expose linearizable route-control-proof fenced helpers for stale-controller fail-closed config changes
- budgeted ChunkServer/BlockServer/ObjectStore cache prewarm and invalidation progress for large ranges
- ChunkServer and BlockServer storage readiness reports expose writable/readable quorum state, capacity pressure, reclaim pressure, trim-flow limits, cache residency, and physical IO counters
- ChunkServer storage readiness and BlockServer maintenance planning expose compact storage read proofs for controller handoff
- BlockServer/ObjectStore storage maintenance planning turns readiness and reclaim candidates into bounded controller actions for quorum restoration, capacity relief, trim-flow pressure, and discarded-chunk reclaim
- raft-backed BlockServer/ObjectStore storage maintenance execution runs bounded audit repair before discarded-chunk reclaim and returns post-run readiness
- raft-backed ChunkServer trim-flow supports direct and prepared checksum-fenced raft apply paths for SSD-aware discard/reclaim throttling
- raft-backed BlockServer read/write quorum reconfiguration with active-replica fences
- raft-backed BlockServer replica quarantine/restore controls that keep unhealthy ChunkServer replicas out of quorum IO
- raft-backed ChunkServer replica freeze/thaw controls that fence maintenance replicas out of write/discard/repair paths while preserving reads
- raft-backed BlockServer route QoS supports direct and prepared checksum-fenced raft apply paths, persists MOSRSBLOCK18/MOSRSBLOCKROUTE8 read/write/discard op-byte windows, route-control commit watermarks, repair counters, receipt retention, read-efficiency counters, and route pressure through storage readiness
- raft-backed ChunkServer mutation QoS supports direct and prepared checksum-fenced raft apply paths, persists MOSRSCHUNK14/MOSRSCHUNKCONTROL9 write/discard op-byte windows, mutation-control commit watermarks, EC recovery counters, and gates BlockServer fanout before local chunk mutation
- BlockServer replica scrub with missing/corrupt/divergent chunk detection and quorum repair
- raft-backed exact ChunkServer/BlockServer chunk repair planning and mutation for a target `ChunkId`, matching the native targeted repair service surface
- BlockServer paged replica audit with missing/corrupt/divergent ChunkServer state and payload-budget continuation
- raft-backed BlockServer replica audit pages with route-control fencing and compact storage-read proofs
- raft-backed BlockServer audit-driven bounded replica repair for incremental ChunkServer anti-entropy
- Rust raft-backed BlockServer prepared audit repair can execute proven
  prepared entries with route-control, raft-entry, and storage-read proof fences
- Rust raft-backed BlockServer prepared range/chunk repair can execute proven
  prepared entries with route-control, raft-entry, and storage-read proof fences
- Rust prepared BlockServer audit repair and ChunkServer reclaim raft entries expose fail-closed bounded controller wrappers
- BlockServer/ObjectStore repairing reads that return quorum-verified data and heal missing replicas without rereading the chunk range
- raft-backed object read-repair reports expose compact object read proofs for repair controller handoff
- Rust raft-backed planned object-replica repair exposes compact metadata read proofs so repair schedulers can bind degraded-object plans to the committed object image they observed before proposing BlockServer repair work
- bounded batch ObjectStore repairing reads with per-item data/error results and aggregate read-heal accounting
- raft-backed bounded BlockServer/ObjectStore range repair with continuation cursors for large ranges
- raft-backed ChunkServer partial reclaim for bounded discard-range compaction across block replicas
- ChunkServer partial reclaim budget-limited and skipped-byte accounting propagated through BlockServer and raft receipts
- ChunkServer reclaim planning with discard-ratio, fragmentation, rewrite, and tail-trim budgets plus raft-backed targeted reclaim
- raft-backed BlockServer/ChunkServer reclaim planning exposes route-control fenced storage-read proofs before mutation
- Rust raft-backed segment discard and compaction can execute proven prepared
  GC entries with route-control, raft-entry, and storage-read proof fences
- Rust raft-backed BlockServer aggregate reclaim can execute proven prepared
  reclaim entries with route-control, raft-entry, and storage-read proof fences
- Rust raft-backed ChunkServer prepared reclaim can execute proven prepared
  reclaim entries with BlockServer route-control and storage-read proof fences
- raft-backed segment compaction fallback prepare/propose helpers now support linearizable BlockServer route-control fencing and compact storage read proofs for GC controller handoff
- raft-backed RootServer HA leader lease campaign/renew fencing
- raft-backed RootServer HA leader lease reads expose linearizable and compact root-read-proof wrappers
- RootServer HA leader lease receipt audits page campaign/renew idempotency records with root read-index proof wrappers
- Rust `MatrixObjectStore::bootstrap_embedded_control_plane` mirrors the native
  MetaServer-first topology: a MetaServer can self-register an embedded root
  owner, campaign the local root lease, and publish metadata health into the
  root catalog without requiring a separate RootServer process.
- Rust root catalog nodes now persist advertised service endpoints in
  `MOSRSROOT20` snapshots, and `discover_blockserver_endpoints_from_catalog`
  plus `discover_chunkserver_endpoints_from_catalog` expose the same
  MetaServer-embedded catalog discovery flow used by the native launchers.
- Rust RootServer coordinator policy is now persisted in `MOSRSROOT21`
  snapshots and can be updated through raft-backed
  `propose_configure_root_coordinator_config`; root leader campaign/renew
  rejects leases whose TTL or service endpoint falls outside the committed
  external coordinator policy.
- Rust local ChunkServer catalog registration now derives root health from a
  selected local chunk replica with `register_local_chunkserver_in_catalog` and
  `heartbeat_local_chunkserver_in_catalog`, matching the native ChunkServer
  MetaServer-catalog registration path.
- Rust local BlockServer catalog registration now derives root health from
  `BlockStorageReadiness` with `register_local_blockserver_in_catalog` and
  `heartbeat_local_blockserver_in_catalog`, including allocator-shard fencing
  and route-control proof state for catalog discovery.
- Rust local ObjectStore gateway registration now exposes
  `register_local_objectstore_gateway_in_catalog` and
  `heartbeat_local_objectstore_gateway_in_catalog`, completing Rust service
  registration parity for MetaServer-catalog deployments.
- Rust local ObjectStore gateway, BlockServer, and ChunkServer catalog
  registration, heartbeat, and offline helpers now also expose `*_at_catalog_fenced`
  variants that validate the selected catalog route checksum before returning,
  matching the native daemon fail-closed liveness route fence.
- Rust raft-backed catalog liveness receipts now expose
  `ProvenRootCatalogRegistration::require_catalog_route_checksum` and
  `into_route_fenced`, so controllers can retain raft receipt evidence while
  enforcing the same selected-catalog route checksum on register, heartbeat, and
  offline updates.
- Rust root catalog liveness now mirrors the native compact
  `MXOSROOT-CATALOG-LIVENESS-RECEIPT1` checksum through typed receipts on
  ObjectStore gateway, BlockServer, and ChunkServer catalog registration
  results. Direct and raft-backed callers can require the expected liveness
  checksum while keeping the route fence and raft receipt sequence intact.
- Rust liveness receipts now also export the native response-field vocabulary via
  `to_native_liveness_receipt_fields` and the proven-registration
  `to_liveness_control_fields`, so mixed controllers can compare
  `root_catalog_liveness_receipt_*` maps without implementation-specific
  adapters.
- Rust controllers can now verify native/daemon-style
  `root_catalog_liveness_receipt_*` maps with
  `RootCatalogLivenessReceipt::require_native_liveness_receipt_fields`, rebuilding
  the compact checksum from returned node, allocator, heartbeat, and raft fields
  before accepting a Root catalog liveness mutation.
- Rust object gateway endpoint discovery now exposes
  `discover_objectstore_gateway_endpoints_from_catalog`, returning stable
  de-duplicated online gateway endpoints from the same root catalog used for
  BlockServer and ChunkServer discovery.
- Rust object gateway endpoint discovery also exposes the effective catalog
  source, embedded catalog endpoint, endpoint count, skip counters, and
  ready/active gateway counts through `to_control_fields`, matching the native
  lightweight RPC projection for MetaServer-first controllers and load
  balancers.
- Rust BlockServer and ChunkServer catalog discovery now expose the same
  `to_control_fields` projection, including effective catalog source, embedded
  catalog endpoint, endpoint counts, allocator coverage, readiness policy,
  disabled ChunkServer indexes, min-free-byte fences, and coverage status for
  service bootstrap controllers.
- Rust controllers can use `select_catalog_endpoint` to apply the same
  MetaServer-first `auto|metaserver|globalcatalog` control-plane selection as
  the native daemons: auto selects a MetaServer catalog endpoint when present and
  otherwise runs without an external catalog, while standalone global-catalog
  mode is explicit and fail-closed when the catalog endpoint is missing. The
  old `rootserver` Rust mode remains a compatibility alias over the same
  snapshot code.
- Rust controllers can also call `select_catalog_endpoint_set_from_env` to
  resolve the production environment surface directly. Precedence matches the
  native services: `MATRIX_OBJECTSTORE_METASERVER_CATALOG_ENDPOINTS`, singular
  `MATRIX_OBJECTSTORE_METASERVER_CATALOG_ENDPOINT`, legacy
  `MATRIX_OBJECTSTORE_METASERVER_ENDPOINTS`, legacy
  `MATRIX_OBJECTSTORE_METASERVER_ENDPOINT`, plus
  `MATRIX_OBJECTSTORE_GLOBAL_CATALOG_ENDPOINT` before the legacy
  `MATRIX_OBJECTSTORE_ROOTSERVER_ENDPOINT`.
- Rust MetaServer coordinator configuration now has the same env bootstrap
  surface as the native daemon. `MetadataCoordinatorConfig::from_env`,
  `configure_metadata_coordinator_config_from_env`, and
  `propose_configure_meta_coordinator_from_env` accept generic coordinator
  backend/namespace/TTL/endpoints plus backend-specific etcd and ZooKeeper
  endpoint variables, then validate through the normal embedded-Raft,
  etcd, or ZooKeeper-compatible policy path. The direct and raft-backed report
  variants return before/after metadata-control readiness, expose a stable
  report checksum, and honor the same expected-checksum env gate used by the
  native MetaServer daemon.
- Rust MetaServer raft membership bootstrap now accepts the native daemon's
  MatrixObjectStore-prefixed voter and healthy-peer env aliases through direct
  and raft-backed report variants. The report binds target voters, before/after
  metadata-control readiness, membership receipts or raft metadata, and an
  expected-checksum env gate for service-manager handoff.
- Rust MetaServer bootstrap controllers can now call one direct or raft-backed
  combined env bootstrap helper that applies raft membership before coordinator
  policy, returns both component reports, binds before/after metadata-control
  readiness, and exposes a top-level expected-checksum env gate.
- Rust raft-backed BlockServer route-control can now bootstrap from
  `MATRIX_OBJECTSTORE_BLOCKSERVER_ROUTE_CONTROL_COMMIT_INDEX`, matching the native
  daemon startup fence. `bootstrap_block_route_control_from_env` advances
  through prepared linearizable route-QoS entries, respects
  `MATRIX_OBJECTSTORE_BLOCKSERVER_ROUTE_CONTROL_BOOTSTRAP_MAX_STEPS`, and
  returns before/after route-control proofs and flat control fields for
  catalog-registration handoff. The report also exposes a public checksum
  format, stable checksum, expected-checksum verifier, and by-value require
  helper for controller handoff.
- Rust raft-backed ChunkServer mutation-control can now bootstrap from
  `MATRIX_OBJECTSTORE_CHUNKSERVER_RAFT_COMMIT_INDEX` for a selected embedded
  replica (`MATRIX_OBJECTSTORE_CHUNKSERVER_REPLICA_INDEX`, default `0`).
  `bootstrap_chunk_mutation_control_from_env` advances through prepared
  linearizable per-replica mutation-QoS entries and returns before/after
  ChunkServer control proofs plus flat control fields for split-service startup
  parity, with a public checksum format, stable report checksum,
  expected-checksum verifier, and by-value require helper.
- Rust raft-backed RootServer catalog state can now bootstrap from
  `MATRIX_OBJECTSTORE_ROOTSERVER_RAFT_COMMIT_INDEX` and
  `MATRIX_OBJECTSTORE_ROOTSERVER_RAFT_READ_INDEX`, with
  `MATRIX_OBJECTSTORE_ROOTSERVER_REQUIRE_RAFT_BOOTSTRAP` forcing a status
  report even when no target is provided. `bootstrap_root_catalog_from_env`
  advances through rafted root-node heartbeats, honors
  `MATRIX_OBJECTSTORE_ROOTSERVER_RAFT_BOOTSTRAP_MAX_STEPS`, and returns
  before/after root catalog status plus flat control fields for explicit
  global-catalog/federation controllers. Report-only calls that are already at
  the requested fence do not require a registered rootserver heartbeat node.
  The root bootstrap report also carries a public checksum format, stable
  checksum, verifier, and by-value require helper.
- Rust `ControlPlaneMode::parse` accepts the native daemon mode strings
  `auto`, `metaserver`, `globalcatalog`, `global-catalog`, `catalog`, and the
  legacy `rootserver`/`root` aliases. Runtime readiness and catalog-selection
  reports now surface `globalcatalog` as the effective mode for explicit
  federation catalog deployments, even when loaded from older rootserver
  snapshots.
- Rust controllers can use `discover_metaserver_endpoints_from_catalog` to
  select healthy MetaServer endpoints from the same root catalog surface used
  for BlockServer, ChunkServer, and ObjectStore gateway discovery. The report
  carries selected endpoints, metadata quorum/read-fence readiness, and
  skipped-node counters for distributed gateway bootstrap parity with native.
- RootServer draining/offline placement repair planning and raft-backed bulk migration
- RootServer capacity-pressure rebalance planning and raft-backed bulk migration
- Rust raft-backed object capacity rebalance can execute proven RootServer
  rebalance pages with BlockServer route-control fencing
- RootServer paginated node catalog with service, eligibility, online, and draining filters plus compact root-read-proof wrappers
- RootServer paginated object-placement audit with node filters for drain/rebalance controllers
- RootServer paginated node-usage inventory with service, draining, pressure, and placement-eligibility filters
- RootServer node-usage inventory exposes derived reclaim pressure and reclaimable/overcommitted byte totals
- RootServer reclaim-node planning ranks bounded reclaim candidates by reclaimable, overcommitted, capacity-pressure, draining, and offline signals
- RootServer paged placement-health audit with missing/offline/draining/pressure and under-replication classification
- RootServer linearizable node, node-usage, reclaim-node, placement, and placement-health reads expose compact root read proofs for distributed controllers
- RootServer health-driven placement repair for missing/offline/draining/pressure and under-replicated placements
- RootServer snapshot chunk export/install for replacement-node bootstrap with placement and committed-capacity validation
- ChunkServer snapshot chunk export/install for replacement-node bootstrap with discard-range, checksum validation, and high-level bounded stream helpers
- ChunkServer and BlockServer chunk-state handoff proofs bind the handoff token, chunk identity, payload checksum, state checksum, write sequence, and discard ranges
- Rust raft-backed ChunkServer handoff preparation exposes compact BlockServer storage read proofs so replacement-replica controllers can retain the route/read-index fence before applying state handoff batches
- Rust raft-backed ChunkServer handoff execution can consume proven handoff
  batches or proven compact handoff envelopes plus payloads, and now persists
  the BlockServer route-control checksum into the raft apply command for replay
  fencing
- BlockServer snapshot chunk export/install for replacement-node bootstrap with replica topology, quorum, chunk snapshot validation, and high-level bounded stream helpers
- Rust raft-backed full-store snapshot chunk export exposes compact metadata read proofs so replacement ObjectStore nodes can bind bootstrap streams to the committed raft image they read
- overwrite/delete stale-range accounting
- merged discard planning per segment
- segment compaction fallback planning
- raft-shaped command application with a `raft-rs` linked build surface
- raft proposal preflight that rejects invalid commands before durable log append
- copy-object semantics with conditional writes, versioning, stale-range accounting, and raft replay
- explicit bucket-versioning read/propose helpers for Rust controllers
- raft-backed object-version delete with retention/legal-hold gates, stale extent accounting, delete-marker handling, and deterministic replay
- object tags with set/get/list-by-tag, snapshot durability, copy/multipart preservation, and raft replay
- raft-backed bucket retention, object retention, and legal-hold governance that fences overwrite/delete/version/lifecycle paths
- raft-backed bucket live-byte/live-object quotas that gate put, copy, and multipart completion before segment allocation
- marker and payload bounded multipart upload pages with read-index gates and shard-owner fences for resumable-upload controllers
- marker and payload bounded multipart part pages with read-index gates and shard-owner fences for large staged upload scans
- raft-backed storage-class transitions and restore windows that gate cold-object payload reads while keeping metadata visible
- raft-backed bucket lifecycle policies with bounded prefix/tag-filtered transition, current expiry, delete-marker cleanup, and noncurrent expiry
- raft-backed bucket lifecycle transition-only apply for cold-tiering controllers that must not expire current, noncurrent, or delete-marker metadata in the same pass
- raft-backed bucket lifecycle apply reports for controllers, preserving transition/expiration/delete-marker counters while committing through the existing lifecycle raft command
- Rust raft-backed bucket lifecycle apply reports expose compact metadata read proofs so lifecycle schedulers can bind transition and expiration counters to the object metadata image used for the raft command
- Rust lifecycle apply and transition-only report proofs are stamped after the raft mutation commits, matching the native post-mutation consensus proof contract for lifecycle controllers
- raft-backed object-version restore that promotes a noncurrent version through the normal put path, preserving version allocation, quota checks, stale-range accounting, and metadata projection
- multipart upload staging/completion/abort with raft replay and snapshot validation
- MetaServer coordinator sessions for etcd/ZooKeeper-style shard-owner liveness fences
- raft-backed MetaServer coordinator backend config for native embedded-raft plus explicit etcd/ZooKeeper adapter endpoints, namespace, session TTL, and snapshot-durable prepared apply
- native and Rust coordinator policy surfaces prefer native embedded-raft and expose checksum-fenced external etcd/ZooKeeper compatibility-mode decisions for service launchers
- Rust MetaServer coordinator backend config retries use snapshot-durable receipts, retention trimming, and snapshot-compaction pruning
- MetaServer bounded metadata prefix scans with resume markers and payload-budget pagination
- MetaServer paged shard inventory with owner filters and live/deleted/value-byte pressure stats
- raft-backed MetaServer shard ownership and namespace lease acquire/renew/release fences
- raft-backed MetaServer namespace lease idempotency receipts with snapshot-safe acquire/renew/release retries
- Rust MetaServer namespace lease acquire/renew/release now also has prepared checksum-fenced raft entries with source/target metadata fingerprints and contiguous commit-index guards
- MetaServer namespace lease retry receipts persist release state across raft snapshots
- raft-backed MetaServer coordinator-session idempotency receipts with snapshot-safe register/renew/release retries
- raft-backed MetaServer bounded expired-lease maintenance with prefix scheduling
- Rust MetaServer byte-bounded prepared lease and coordinator-session expiry entries for large coordination-state cleanup
- raft-backed bounded MetaServer maintenance that batches lease, coordinator-session, and transaction expiry
- raft-backed MetaServer maintenance planning exposes compact metadata read proofs and fails closed on stale applied indexes
- Rust raft-backed MetaServer maintenance can execute proven maintenance plans
  with metadata read-proof and plan-checksum fences
- raft-backed MetaServer shard split/merge topology changes with epoch and owner-session fences
- Rust MetaServer shard split/merge topology changes now expose linearizable read-proof prepare helpers plus proven execution helpers that bind metadata-control proof and raft-entry checksums before mutating shard topology
- raft-backed MetaServer shard owner migration for failover/rebalance with expected-epoch fences
- raft-backed MetaServer shard topology idempotency receipts for configure, split, merge, and migrate retries
- raft-backed MetaServer shard failover/rebalance planning and bounded execution with session fences
- raft-backed MetaServer shard failover/rebalance planning now exposes compact metadata read proofs and rejects stale applied indexes before preparing ownership-migration raft entries
- Rust raft-backed MetaServer shard migration can execute proven prepared
  failover/rebalance plans with metadata read-proof and plan-checksum fences
- MetaServer response-budgeted shard route resolution with owner-session serving state for distributed clients
- MetaServer route cache with split topology/session fingerprints, hit/miss/invalidation counters, read-fenced stale-entry audit/eviction, and snapshot-safe rebuild
- MetaServer route-cache audits expose compact metadata read proofs for shard-routing controllers
- MetaServer route cache TTLs capped by coordinator-session expiry to fence stale owner routes
- MetaServer route cache capacity and TTL are production-tunable in `StoreOptions`, snapshot-preserved, and default to native service scale
- MetaServer object metadata, scalar/batch payload reads, and scalar/batch cache controls with raft read-index and optional live shard-owner fences for shard-routed clients
- marker and payload bounded object-list pages with read-index gates and shard-owner fences for bucket/prefix controllers
- MetaServer batch conditional object reads with raft read-index and optional live shard-owner fences, returning per-item payload/not-modified/error outcomes while avoiding chunk IO for unchanged ETags
- marker and payload bounded object tag-list pages with read-index gates and shard-owner fences for tag-index controllers
- version-specific conditional head, full-object, and bounded range reads with raft read-index gates, preserving exact-version cache validation for noncurrent object versions
- marker and payload bounded object-version pages with read-index gates and shard-owner fences for large version-history scans
- version-specific conditional batch reads with per-item payload/not-modified/error/admission outcomes, bounded aggregate payloads, read-index gates, and shard-owner fences
- native-parity compact object read proofs for Rust linearizable head/full-object/conditional/range reads, governance and bucket policy reads, bounded metadata/payload/cache-control batch reads, and shard-routed head/full-object/range/batch/cache-control/governance reads
- raft-backed MetaServer owner/epoch/session-fenced metadata writes for shard-routed writers
- raft-backed MetaServer idempotent metadata batch receipts and proposal APIs for generic and owner-fenced shard writers
- Rust MetaServer prepared metadata batch raft entries carry validated mutations, deterministic post-mutation images, idempotency/request checksums, and commit-index fences before append/apply
- Rust MetaServer bounded prepared metadata batch raft entries add controller mutation-count and payload-byte fences before deterministic image projection
- Rust MetaServer prepared transaction raft entries carry deterministic begin/renew/commit/abort transaction images, idempotency receipts, request checksums, record images for commits, and commit-index fences before append/apply
- Rust MetaServer bounded prepared transaction raft entries fence begin-time and commit-time mutation counts/payload bytes before deterministic transaction image projection
- Rust ChunkServer prepared batch write/discard raft entries carry deterministic post-mutation chunk images, batch receipts, request/payload checksums, and commit-index fences before append/apply
- raft-backed MetaServer transactions, prefix deletes, transaction expiry, and changefeed checkpoints
- Rust MetaServer bounded prepared prefix deletes select deterministic namespace windows under record-count and payload-byte budgets before raft append
- raft-backed MetaServer transaction begin/renew/commit/abort idempotency receipts with snapshot durability and compaction
- MetaServer transaction get/list controller reads with prefix markers, owner/state filters, expired-active scans, and snapshot durability
- MetaServer byte-bounded transaction summary pages with raft read-index fencing for large transaction controller scans
- MetaServer raft membership receipt pages with raft-log linearizable read fencing for production membership audits
- MetaServer receipt audit pages expose compact receipt read proofs for membership, lease, shard, transaction, coordinator-session, batch, and mutation reconciliation
- Rust MetaServer lease, shard, coordinator-config, and coordinator-session receipt audit pages expose content-level proof checksums over filters, counters, payload budgets, and emitted receipt proofs
- Rust MetaServer batch, mutation, and transaction receipt audit pages expose content-level proof checksums for shard-writer replay and transaction cleanup controllers
- MetaServer prefix-filtered, byte-bounded changefeed pages with raft-backed consumer checkpoints
- MetaServer raft-read-gated watch/resync pages report compacted cursors and retained-window bounds for etcd/ZooKeeper-style metadata watchers
- MetaServer checkpoint-lag plans, checkpoint pages, and watch/resync pages expose compact metadata read proofs for controller handoff
- MetaServer changefeed checkpoint lag planning with retained-window compaction risk and raft-read-fenced linearizable access
- raft-backed MetaServer changefeed compaction fenced by durable consumer checkpoints
- raft-backed MetaServer transaction GC for bounded cleanup of committed/aborted/expired transaction state
- raft-backed MetaServer tombstone compaction with checkpoint safety, byte/record bounds, and durable compaction watermarks
- raft-backed MetaServer snapshot compaction prunes committed batch, namespace-lease, and coordinator-session retry receipts
- MetaServer raft snapshot chunk export/install with deterministic byte checksums, commit-index fences, and high-level bounded stream helpers
- MetaServer Raft readiness reports expose applied/log commit indexes, read-index lag, owner-session health, lease/transaction counts, and compaction state for routing/write gates
- raft-read-gated MetaServer integrity planning reports stale shard owners, expired sessions/leases/transactions, compactable tombstones/changefeed windows, route-cache staleness, snapshot pressure, bounded samples, and stable plan checksums
- raft-backed MetaServer admission policy bounds metadata batch mutations/value bytes, open transactions, namespace leases, coordinator sessions, and shard fanout, with direct and prepared checksum-fenced raft apply paths, MOSRSMETA12 snapshot durability, and readiness pressure signals
- raft-backed MetaServer QoS bounds metadata write operations/value bytes per window, supports direct and prepared checksum-fenced raft apply paths, persists MOSRSMETA13 QoS config/state, and exposes readiness pressure for shard-routed controllers
- Rust MetaServer control proofs expose `MOSRSMETA-CONTROL5` checksums for raft/read-index state, applied-metadata freshness, shard topology/session health, replication quorum, coordinator-config receipt state, admission/QoS pressure, retention, compaction, and durable metadata fingerprints
- Rust raft-backed MetaServer prepared retention, admission, QoS, and route-cache control config helpers now expose linearizable prepare/propose variants, so external controllers fail closed before projecting stale control-plane entries
- Rust MetaServer coordinator-config receipts are exposed through a bounded marker-based page API with backend filtering, matching the native remote audit surface for raft-applied embedded-raft/etcd/zookeeper coordinator policy
- Rust RootServer snapshots preserve MetaServer control proofs, route-cache freshness/pressure, coordinator-config receipt counts, applied-metadata freshness, raft membership receipt retention, BlockServer allocator shard identity, metadata/data/gateway eligibility, and per-MetaServer metadata byte footprints in `MOSRSROOT10` while importing `MOSRSROOT1-9`.
- Rust distributed readiness now aggregates RootServer-observed MetaServer active/readable/writable/caught-up/apply-caught-up/read-fence coverage, metadata byte footprints, data/control apply-quorum status, apply/read-fence lag bounds, topology mismatches, MetaServer coordinator-config receipt counts, top-level MetaServer management pressure/coordination/changefeed counters, and ChunkServer aggregate write-path/read-cache control counters into the readiness gate and `MOSRSOBJECTREADY35` control checksum.
- Rust RootServer snapshots now preserve BlockServer service health in `MOSRSROOT11`, including ready/read-ready state, route-control checksum, chunk endpoint readiness, quarantines, and backend capacity totals.
- Rust distributed readiness gates complete BlockServer allocator shards on RootServer-observed BlockServer service readiness and binds the coverage into `MOSRSOBJECTREADY36`, failing closed with `root_blockservers_unready` when no ready/read-ready BlockServer remains.
- Rust RootServer snapshots now preserve ChunkServer service health in `MOSRSROOT13`, including accepting-writes, frozen, raft-writable, reported free capacity, and service-mode state, while importing `MOSRSROOT1-12` through numeric version gates.
- Rust RootServer snapshots now preserve RootServer-observed MetaServer read-fence health in `MOSRSROOT15`, including requested/safe read-fence, read-fence covered state, and commit/read-index/applied lag while importing `MOSRSROOT1-14` through numeric version gates.
- Rust distributed readiness now classifies RootServer-observed ChunkServer no-write causes by frozen/read-only/raft-unwritable/capacity pressure and binds them into `MOSRSOBJECTREADY39`.
- Rust RootServer placement planning, placement commits, migration commits, placement-health audit/repair, node-usage eligibility, and capacity rebalance now share the same ChunkServer service-health fence used by distributed readiness.
- Rust RootServer node usage/capacity/online heartbeats plus BlockServer and ChunkServer service-health heartbeats now have typed direct and raft-backed APIs, so live usage, route-control, and service-mode changes update the durable root catalog without full node re-registration.
- Rust distributed readiness gates on RootServer-observed active BlockServer allocator shard coverage, rejecting incomplete, duplicate, invalid, or mixed shard topology before distributed writers can advance overlapping block allocations.
- Rust distributed readiness gates on RootServer-observed active/writable/draining/capacity-blocked ChunkServer coverage, rejecting root catalogs with no writable data endpoints before BlockServer or object writes reserve capacity.
- Rust distributed readiness exposes self-describing BlockServer and ChunkServer state-fingerprint envelopes with checksum, format, and material-byte counts, binding them into `MOSRSOBJECTREADY39`.
- Rust distributed readiness exposes top-level storage read-cache entry counts from BlockServer aggregates and binds them into `MOSRSOBJECTREADY40`, matching the native production readiness surface for cache-residency admission.
- Rust distributed readiness now also sources read-cache direct-fill and EC-fill counters from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY41` plus `MOSRSBLOCK-FINGERPRINT3`, matching the native fill-counter surface for distributed cache residency planning.
- Rust distributed readiness sources read-cache clear operation, cleared-entry, and cleared-byte counters from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY42` plus `MOSRSBLOCK-FINGERPRINT4`, matching the native cache-clear readiness surface.
- Rust distributed readiness sources read-cache invalidation operation, invalidated-entry, and invalidated-byte counters from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY43` plus `MOSRSBLOCK-FINGERPRINT5`, matching the native cache-invalidation readiness surface.
- Rust distributed readiness sources read-cache eviction and corrupt-cache page/byte counters from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY44` plus `MOSRSBLOCK-FINGERPRINT6`, matching the native cache-health readiness surface.
- Rust distributed readiness sources read-cache max-byte capacity and pressure from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY45` plus `MOSRSBLOCK-FINGERPRINT7`, so distributed cache admission sees saturated ChunkServer replicas instead of only local cache state.
- Rust distributed readiness exposes read-cache hit and miss counters from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY46` plus `MOSRSBLOCK-FINGERPRINT8`, matching the native object-level distributed cache effectiveness surface.
- Rust distributed readiness exposes compressed read-cache fill counters from BlockServer replica aggregates and binds them into `MOSRSOBJECTREADY48` plus `MOSRSBLOCK-FINGERPRINT10`, matching the native compressed cache-fill readiness surface for compression-aware cache residency planning.
- Rust distributed readiness also exposes physical/coalesced storage read counters from BlockServer cumulative read stats and binds them into `MOSRSOBJECTREADY48`, `MOSRSBLOCK-FINGERPRINT10`, `MOSRSBLOCK18`, and `MOSRSBLOCKROUTE8` for read-amplification aware placement and cache tuning.
- Rust distributed readiness exposes consensus read-index refresh/cache-hit counters plus last read status and binds them into `MOSRSOBJECTREADY49`, matching the native top-level consensus-read readiness surface for linearizable metadata controllers.
- Rust distributed readiness exposes top-level MetaServer endpoint, commit status, live object-namespace shard/lease fence evidence, and route-cache counters/topology fields from the existing metadata route snapshot, binding them into the readiness checksum so Rust gateways can consume the same service-shaped MetaServer readiness evidence as native controllers.
- Rust module status now carries that same MetaServer shard/lease fence evidence and binds it into `MOSRSMODULESTATUS12`, so status-polling gateways and controllers no longer need a separate readiness call to prove object-namespace ownership.
- Rust MetaServer external coordinator mode now endpoint-fences direct, idempotent, prepared, and renewed coordinator sessions, active shard-owner serving, route preferred/fallback endpoints, expired-owner failover planning, and readiness writable-shard accounting against the configured `etcd`/`zookeeper` endpoint set.
- Rust MetaServer coordinator-session expiry now has optional endpoint-fenced
  cleanup, so external coordinator endpoint rotation can reclaim stale sessions
  under the same prepared raft/checksum envelope used for TTL expiry.
- Rust distributed readiness closes the remaining native top-level controller parity fields for metadata/block readiness, root expected node counts, placement degraded object counts, root MetaServer status availability, read-cache bytes, and BlockServer repair counters, binding them into `MOSRSOBJECTREADY51`.
- Rust distributed readiness now exposes top-level MetaServer route-cache fresh/stale/expired counts, stale-topology/stale-session counters, freshness state, and session checksum, binding them into `MOSRSOBJECTREADY52` to match the native gateway/controller readiness surface.
- Rust distributed readiness now exposes GC and performance ratios and candidates: live/stale/pending/discarded PPM, logical and storage write amplification PPM, space amplification PPM, discard backlog PPM, discard candidate bytes/segments, pending/merged/fragmented discard range counts, compaction candidate bytes/ranges/segments, binding them into `MOSRSOBJECTREADY60` and `MOSRSMODULESTATUS9` for controller admission and GC scheduling.
- Rust MatrixObjectStore exposes `configured_control_plane_mode`, `effective_control_plane_mode`, and `rootserver_required` so controllers can distinguish normal MetaServer-first deployments from explicit RootServer topologies. The effective mode is bound into `MOSRSOBJECTREADY61` and `MOSRSMODULESTATUS10`, and snapshots persist the configured mode in `MOSRSSTORE9`.
- Rust object GC reports now expose native-parity discard policy selection with `min_discard_bytes`, `max_discard_bytes`, `selected_discard_bytes`, selected/skipped range counts, and `discard_budget_limited`, including linearizable read-proof and route-fenced wrappers for distributed GC schedulers.
- Rust versioned object cache invalidation now matches versioned prewarm:
  retained object versions can invalidate ChunkServer read-cache ranges through
  local, routed, batch, linearizable, and shard-owner/epoch fenced APIs.
- Rust ChunkServer and BlockServer batch write/discard paths now expose proven prepared execution helpers, letting distributed data-plane controllers bind a linearizable storage proof, route/control checksum, and raft-entry checksum before committing high-throughput batch mutations.
- Rust ChunkServer service-control changes now expose proven prepared execution helpers for capacity, service mode, trim flow, mutation QoS, and read-cache sizing, binding storage read proof, route-control checksum, and raft-entry checksum before changing service behavior.
- Rust ChunkServer replica reclaim now exposes proven envelope execution, binding route-control proof, storage applied-index proof, reclaim envelope checksum, plan checksum, and raft-entry checksum before compacting discarded replica ranges.
- Rust MetaServer changefeed, tombstone, and snapshot compaction now expose proven prepared execution helpers, binding a linearizable metadata proof and raft-entry checksum before committing compaction entries.
- Rust BlockServer allocation reservation, batch reservation, and release now expose proven envelope execution helpers, binding storage proof, route-control checksum, envelope checksum, and raft-entry checksum before committing allocation state.
- Rust storage maintenance orchestration now exposes a proven envelope execution helper, binding storage proof, BlockServer route-control checksum, envelope checksum, and plan checksum before running reclaim, segment discard/compaction, trim, and capacity actions.
- Rust segment discard now exposes proven compact envelope prepare/execute helpers, binding storage proof, route-control checksum, merged stale-range envelope checksum, and raft-entry checksum before discarding segment ranges.
- Rust segment compaction fallback now exposes proven compact envelope prepare/execute helpers, binding storage proof, route-control checksum, compaction envelope checksum, and raft-entry checksum before rewriting live objects for fragmented stale ranges.
- Rust RootServer placement migration, placement repair, and capacity rebalance now expose proven envelope execution helpers, binding root read proof, envelope checksum, raft-entry checksum, and placement plan inputs before committing root placement changes.
- Rust BlockServer quorum and route-QoS configuration now expose read-proof prepare helpers plus proven execution helpers, binding storage proof, route-control checksum, and raft-entry checksum before changing data-plane admission policy.
- Rust MetaServer retention, admission, QoS, and route-cache controls now expose read-proof prepare helpers plus proven execution helpers, binding metadata control proof, metadata commit-index proof, and raft-entry checksum before changing metadata control behavior.
- Rust MetaServer coordinator backend configuration now carries metadata-control proof stamps in prepared raft entries and exposes proven execution helpers for embedded-raft/etcd/zookeeper policy changes.
- Rust MetaServer coordinator session register, renew, and release entries now carry metadata-control proof stamps and expose read-proof prepare plus proven execution helpers for distributed session owners.
- Rust MetaServer namespace lease acquire, renew, and release now expose read-proof prepare plus proven execution helpers, binding metadata-control proof, commit-index proof, and raft-entry checksum before ownership mutation.
- Rust MetaServer metadata batch commits now expose bounded/unbounded read-proof prepare helpers plus proven execution, binding metadata-control proof, commit-index proof, and raft-entry checksum before applying object metadata mutations.
- Rust MetaServer metadata transactions now expose bounded/unbounded read-proof prepare helpers plus proven execution, binding metadata-control proof, commit-index proof, and raft-entry checksum before begin/renew/commit/abort transaction mutations.
- Rust object metadata current/object-record commits now expose read-proof prepare plus proven execution, binding metadata-control proof, object commit checksum, and embedded prepared batch raft-entry checksum before applying current-version/object-record mutations.
- Rust MetaServer prefix deletes now carry metadata-control proof stamps and expose bounded/unbounded read-proof prepare plus proven execution for lifecycle and namespace cleanup controllers.
- Rust MetaServer transaction GC now carries metadata-control proof stamps and exposes read-proof prepare plus proven execution for terminal transaction cleanup.
- Rust MetaServer change checkpoint commits now carry metadata-control proof stamps and expose read-proof prepare plus proven execution for changefeed consumers.
- Rust MetaServer coordinator-session expiry now exposes proven envelope execution, binding the metadata-control proof, applied metadata commit-index proof, envelope checksum, and raft-entry checksum before expiring dead coordinator sessions.
- Rust MetaServer full maintenance now also exposes proven envelope execution, letting controllers bind the compact maintenance image, metadata-control proof, applied metadata commit-index proof, and raft-entry checksum before running lease/session/transaction/tombstone cleanup.
- Rust BlockServer range, exact-chunk, and audit repair now expose proven envelope execution, binding route-control proof, storage applied-index proof, compact repair envelope checksum, and raft-entry checksum before committing repair entries.
- native MetaServer route resolution now exposes and fences coordinator-session checksums, matching the Rust route-cache model where owner-session changes invalidate cached shard routes before stale shard-routed writes can commit.
- Rust object-store repair now includes object-level replica repair planning and execution over live object metadata, paging by object marker, selecting only objects with degraded BlockServer chunks, and aggregating bounded per-object repair stats for controller-driven fleet repair.
- Rust raft-backed object-store replica repair now executes planned degraded-object repairs through bounded BlockServer raft entries, preserving controller repair counters and follower replay for fleet anti-entropy passes.
- Rust raft-backed RootServer placement reads now expose linearizable placement lookup, placement inventory, and placement-health audit wrappers, fencing repair/decommission/rebalance controllers against stale placement state.
- Rust raft-backed RootServer node catalog, node-usage inventory, and reclaim-node planning reads now require the local applied index to satisfy the root raft commit index before returning scheduler input.
- Rust object-store decommission now coordinates RootServer draining placement plans with bounded BlockServer object repair before committing placement migration, giving Rust fleet controllers the same repair-then-migrate workflow as the native decommission path.
- Rust object-store capacity rebalance now exposes object-level plan/run reports over RootServer capacity-pressure migration plans, with object metadata, pressure node ids, migration counters, and marker-aware pagination for fleet rebalance controllers.
- Rust raft-backed object decommission and capacity-rebalance planning expose compact root read proofs so fleet controllers can bind repair/migration decisions to the root placement image they observed.
- Rust raft-backed object-store capacity rebalance now proposes each object placement migration through prepared RootServer raft entries, preserving object-level reports while retaining checksum and contiguous commit-index fences for replayable controller actions.
- Rust raft-backed RootServer prepared placement migration and capacity rebalance helpers now expose linearizable prepare/propose variants, fencing distributed controllers before projecting or committing root placement actions.
- Rust raft-backed object decommission now runs bounded BlockServer object repair through raft before prepared RootServer placement migration, preserving repair/migration counters for draining-node controllers and follower replay.
- Rust raft-backed object read repair now returns object-range repair reports while appending BlockServer repair entries through raft, so read-heal operations are replayable instead of leader-local.
- Rust raft-backed batch object read repair now returns per-item batch read reports and appends repair entries only for degraded successful items, preserving replayable read-heal side effects without mutating healthy reads.
- Rust RootServer placement and ChunkServer coverage honor explicit data eligibility, so non-data services can heartbeat capacity without being selected for object data placement.
- Rust single-object delete and version-delete paths now have route-fenced report APIs matching the native delete response contract. Local delete, RootServer release delete, raft-backed RootServer release delete, and raft-backed version delete can return deleted metadata, deleted bytes, BlockServer route-control proof, and the expected checksum used to fence the mutation.
- Rust object-level replica repair, draining-node decommission, and capacity-rebalance run reports now carry BlockServer route-control proof plus caller expected checksums. Local and raft-backed fenced helpers reject stale route checksums before launching placement/repair/rebalance work, matching the native service evidence exposed to operators.
- MetaServer, BlockServer, and ChunkServer readiness reports expose distributed control proofs matching the native service surface: retained metadata commit bounds, topology receipt counts, raft fence/apply-envelope support, BlockServer route-control quorum/disabled-replica checksums, and ChunkServer service-mode/capacity/cache/trim-flow/mutation-QoS/EC checksums
- ObjectStore distributed readiness aggregates MetaServer control proofs, BlockServer quorum readiness, ChunkServer control proofs through BlockServer readiness, RootServer placement health, and object/segment counters into one checksum-stamped admission report for gateways and controllers
- RootServer register, node heartbeat, BlockServer health heartbeat, and ChunkServer health heartbeat paths reject stale heartbeat timestamps, preventing delayed status updates from rolling back newer capacity, placement, or service-mode state.
- Rust RootServer now has local and raft-backed TTL expiry for stale node heartbeats, marking expired services offline so placement and readiness stop selecting dead ChunkServer/BlockServer/MetaServer endpoints.
- Rust RootServer now has local and raft-backed safe node removal, rejecting unforced removals when live object placements still reference a service and allowing explicit forced cleanup with reference byte/count accounting.
- Rust RootServer node-usage inventory now surfaces placement references to unregistered nodes as synthetic, non-placement-eligible rows with object/byte totals plus total, registered, and referenced node cardinality for decommission and recovery controllers.
- Rust raft-backed object checkpoint flush reports now mirror the native deferred
  checkpoint control contract for controllers: linearizable read/commit fences,
  metadata commit index before/after, metadata checkpoint lag before/after, and
  compact object read proof.
- Rust bucket CORS mutations now have snapshot-durable idempotency receipts,
  plus local, raft-backed, linearizable, and MetaServer-shard fenced helpers
  that return the original committed policy report on exact retries and reject
  reused keys with different canonical policy requests.
- Rust bucket versioning mutations now have snapshot-durable idempotency
  receipts plus local, raft-backed, linearizable read-proof, and MetaServer
  shard-owner fenced helpers for retry-safe enable/disable changes.
- Rust bucket retention, quota, and lifecycle policy mutations now have shared
  snapshot-durable idempotency receipts and local plus raft-backed configure
  helpers, closing the retry contract gap with the generated native bucket policy
  mutation path.
- Rust bucket retention idempotent raft mutations now expose linearizable
  read-proof and MetaServer shard-owner fenced helpers, so retention policy
  retries can be routed through the live metadata owner.
- Rust bucket quota idempotent raft mutations now expose linearizable
  read-proof and MetaServer shard-owner fenced helpers, giving quota
  controllers the same retry-safe owner-routed policy path as retention.
- Rust bucket lifecycle idempotent raft mutations now expose linearizable
  read-proof and MetaServer shard-owner fenced helpers, so lifecycle policy
  controllers can safely retry rule updates through the active metadata owner.
- Rust bucket policy-document idempotent raft mutations now expose MetaServer
  shard-owner fenced helpers, giving bucket authorization policy controllers
  the same retry-safe owner-routed path as lifecycle, quota, and retention.
- Rust bucket tagging idempotent raft mutations now expose receipt-only
  MetaServer shard-owner fenced helpers, so tag controllers can take the fast
  retry-safe owner-routed path when they do not need a post-commit read proof.
- Rust bucket CORS idempotent raft mutations now expose receipt-only
  MetaServer shard-owner fenced helpers, so browser and gateway CORS policy
  controllers can take the fast retry-safe owner-routed path without a
  post-commit read proof.
- Rust routed batch object-cache prewarm/invalidation now has MetaServer
  shard-owner, BlockServer route-control, and cache-batch proof checksum
  fences for targeted replica cache controllers.
- Rust routed linearizable batch object-cache prewarm/invalidation now exposes
  the same cache-batch proof checksum fence for BlockServer-local targeted
  replica cache controllers.
- Rust bucket create/delete now mirror native bucket membership idempotency:
  durable receipts, raft replay, linearizable read-proof helpers, and
  MetaServer shard-owner fenced retry paths.
- Rust multipart create now supports retry-safe raft proposals for distributed
  large-object writers, with durable pending-upload idempotency keys plus
  MetaServer shard-owner and BlockServer route-proof fences.
- Rust multipart part upload now persists per-part idempotency keys and exposes
  retry-safe route/shard-fenced raft proposals for distributed large-object
  writers.
- Rust multipart completion now persists completion receipts so exact retries
  return the original object metadata without duplicate raft commits.
- Rust multipart abort now persists abort receipts so exact retries return the
  original removed upload without duplicate raft commits.
- Rust multipart completion/abort receipts now support rafted retention trims,
  bounding receipt growth for high-volume upload services.
- Multipart receipt trims also expose linearizable read-proof and MetaServer
  shard-owner fenced wrappers for distributed retention controllers.
- Object-level distributed readiness mirrors MetaServer external-coordinator
  fence evidence, including config checksum/revision, receipt count, endpoint
  readiness, and shard-owner session readiness.
- `ObjectStoreDistributedReadiness` exports flat control fields for scheduler
  handoff, including readiness checksums and MetaServer/BlockServer/ChunkServer
  control proof counters.
- Rust object gateway discovery now returns the selected catalog route proof and
  exposes a checksum-fenced variant, matching the native gateway discovery route
  fence so distributed gateway controllers can reject stale MetaServer/global
  catalog routing before consuming endpoint lists.
- Rust BlockServer and ChunkServer endpoint discovery now expose the same
  selected catalog route proof plus checksum-fenced variants, so storage
  service bootstrappers can reject stale control-plane routing before wiring
  allocation or chunk persistence endpoints.
- Rust MatrixObjectStore now exposes MetaServer and RootServer coordinator
  policy control-field maps directly from the top-level store, matching the native
  generated coordinator responses. Controllers can consume the embedded-raft
  production policy, external compatibility mode, endpoint dependency, and
  policy checksum without reaching into service internals.
- Rust persistent-journal fsync now has strict-by-default grouped-sync knobs for
  latency-sensitive replicated deployments:
  `MATRIX_OBJECTSTORE_RUST_JOURNAL_SYNC_EVERY_FRAMES` defaults to `1`,
  `MATRIX_OBJECTSTORE_RUST_JOURNAL_SYNC_EVERY_BYTES` defaults to `0`, and
  snapshot flushes force-drain pending journal bytes before publishing the
  checkpoint image. Setting a larger frame or byte threshold lets raft-backed
  clusters amortize local fsync cost while keeping the standalone default
  crash-safe.
- Rust scale benchmarking now reports snapshot pressure from a compact metadata
  snapshot plus payload-footprint accounting instead of cloning and serializing
  all committed object payloads under the store lock. Full snapshot export and
  install still carry payload bytes for recovery; the compact path is for
  benchmark/readiness footprint reporting only.
- Rust raft-backed snapshot streaming now serializes the recovery snapshot once
  per transfer and emits bounded chunks from that image under a single
  linearizable read proof. Offset-based single-chunk export remains available,
  but chunk streams avoid repeated full-snapshot clones and checksum work.

The crate is intentionally isolated under `rust/matrixobjectstore-rs` so it can grow toward native parity without disturbing the existing native service binaries. The default feature links the Rust `raft` crate (`raft-rs`) and exposes an adapter marker in `raft_backend`; the in-memory raft log included here is the deterministic validation backend for object-store command ordering and replay.
