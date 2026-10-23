// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoreSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    checksum: String,
}

impl StoreSnapshotExportCache {
    /// A whole encoded store, cached. Its fields are private to this module.
    pub(crate) fn footprint_bytes(&self) -> u64 {
        (self.bytes.len() as u64)
            .saturating_add(self.stamp.capacity() as u64)
            .saturating_add(self.checksum.capacity() as u64)
    }
}

#[derive(Clone, Debug)]
struct PersistentSnapshotDurableStamp {
    state_stamp: String,
    file_stamp: PersistentSnapshotFileStamp,
}

/// What the store keeps about a written payload: its length and content hash,
/// not its bytes.
///
/// The bytes live in the page service, which is where the read path already
/// goes - `allocate_extents`/`read` paths all call `chunk_service` directly.
/// Keeping a second copy here cost one extra copy of every byte written, about
/// 1.00x of the store's resident footprint.
///
/// "Nothing ever looked this map up by key" was the other half of that
/// justification and it is NO LONGER TRUE, so do not remove the map on the
/// strength of it. Two sites look it up by `(chunk_id, offset)`:
/// `capture_targeted_mutation_rollback` captures the previous payloads of the
/// extents it may have to put back, and `validate_objects` fetches each
/// extent's entry to check `payload.len() != extent.length`.
///
/// That second one is why `len` is here at all. It duplicates
/// `ObjectExtent.length` deliberately, so a snapshot whose index and whose
/// objects disagree is caught rather than loaded. The 16 bytes an entry are a
/// durability check and a digest, which is what
/// `measure_what_sits_above_the_replication_floor_on_each_side` reports as the
/// only term above three-way replication on the payload side.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredPayload {
    pub(crate) len: u64,
    /// The digest itself, not its spelling. `etag` formats this same number
    /// as "fnv1a64:" followed by 16 hex digits, so holding the number costs
    /// 8 bytes where holding the text cost 24 inline plus a 24-byte buffer,
    /// and cloning the map no longer allocates once per entry.
    pub(crate) etag_fnv1a64: u64,
}

/// Every written range the store knows about, keyed by chunk and then by offset.
///
/// # WHY IT IS NESTED RATHER THAN KEYED BY A PAIR
///
/// A flat `BTreeMap<(ChunkId, u64), StoredPayload>` spends 24 bytes on every key, and 16
/// of them are the `ChunkId` -- repeated in every entry of a chunk, and a 64 MiB chunk
/// holds tens of thousands of 1 KiB payloads. Keyed by chunk and then by offset, the
/// identity is paid once per chunk and a key is the 8-byte offset.
///
/// **The iteration order is unchanged, and that is what makes this a shape change rather
/// than a format one.** `(ChunkId, u64)` sorts by chunk and then by offset, which is
/// exactly the order a nested map walks, so the snapshot encoder, its checksum and its
/// validator all see the same sequence they saw before. If that were not true this would
/// be a durable-format change: the encoder's output is hashed.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PayloadIndex {
    by_chunk: BTreeMap<ChunkId, BTreeMap<u64, StoredPayload>>,
    /// Counted, not summed on demand. `len()` is read by the audit, by the snapshot
    /// encoder and by the export sizer, and summing the inner maps would make each of
    /// those walk every chunk to answer a question it asks once.
    entries: usize,
}

impl PayloadIndex {
    pub(crate) fn len(&self) -> usize {
        self.entries
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Chunks holding at least one entry, which is how many times the identity is paid.
    pub(crate) fn chunk_count(&self) -> usize {
        self.by_chunk.len()
    }

    pub(crate) fn insert(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        payload: StoredPayload,
    ) -> Option<StoredPayload> {
        let replaced = self
            .by_chunk
            .entry(chunk_id)
            .or_default()
            .insert(offset, payload);
        if replaced.is_none() {
            self.entries = self.entries.saturating_add(1);
        }
        replaced
    }

    pub(crate) fn get(&self, chunk_id: &ChunkId, offset: u64) -> Option<&StoredPayload> {
        self.by_chunk.get(chunk_id)?.get(&offset)
    }

    /// Removes the entry, and the chunk's map with it when that was its last.
    ///
    /// An emptied inner map left behind would keep the 16 bytes this shape exists to stop
    /// paying, and would make `chunk_count` report chunks the index no longer indexes.
    pub(crate) fn remove(&mut self, chunk_id: &ChunkId, offset: u64) -> Option<StoredPayload> {
        let chunk = self.by_chunk.get_mut(chunk_id)?;
        let removed = chunk.remove(&offset);
        if removed.is_some() {
            self.entries = self.entries.saturating_sub(1);
            if chunk.is_empty() {
                self.by_chunk.remove(chunk_id);
            }
        }
        removed
    }

    /// Every entry, in `(chunk, offset)` order -- the order the flat map had.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&ChunkId, u64, &StoredPayload)> {
        self.by_chunk.iter().flat_map(|(chunk_id, offsets)| {
            offsets
                .iter()
                .map(move |(offset, payload)| (chunk_id, *offset, payload))
        })
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &StoredPayload> {
        self.by_chunk.values().flat_map(|offsets| offsets.values())
    }

    pub(crate) fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&ChunkId, u64, &StoredPayload) -> bool,
    {
        let mut kept = 0usize;
        self.by_chunk.retain(|chunk_id, offsets| {
            offsets.retain(|offset, payload| keep(chunk_id, *offset, payload));
            kept = kept.saturating_add(offsets.len());
            // The emptied map goes, for the reason `remove` gives.
            !offsets.is_empty()
        });
        self.entries = kept;
    }
}

impl FromIterator<((ChunkId, u64), StoredPayload)> for PayloadIndex {
    fn from_iter<I: IntoIterator<Item = ((ChunkId, u64), StoredPayload)>>(entries: I) -> Self {
        let mut index = Self::default();
        for ((chunk_id, offset), payload) in entries {
            index.insert(chunk_id, offset, payload);
        }
        index
    }
}

#[derive(Clone, Debug)]
pub struct MatrixObjectStore {
    pub(crate) options: StoreOptions,
    persistent_snapshot_path: String,
    persistent_journal_path: String,
    pub(crate) buckets: BTreeSet<SharedText>,
    /// One allocation per distinct content type, up to
    /// `MAX_SHARED_CONTENT_TYPES`. Derived state: it is not in a snapshot, it
    /// is rebuilt when one is installed, and a mutation that is rolled back
    /// leaves its entry behind -- which interns a type no object uses until the
    /// bound is reached, and is why the per-mutation undo does not capture it.
    pub(crate) content_types: BTreeSet<SharedText>,
    bucket_versioning: BTreeSet<String>,
    pub(crate) objects: BTreeMap<ObjectId, ObjectMetadata>,
    pub(crate) versions: BTreeMap<ObjectId, Vec<ObjectMetadata>>,
    bucket_retention: BTreeMap<String, u64>,
    bucket_quotas: BTreeMap<String, BucketQuota>,
    bucket_lifecycle: BTreeMap<String, BucketLifecyclePolicy>,
    bucket_policy_documents: BTreeMap<String, BucketPolicyDocument>,
    bucket_tags: BTreeMap<String, BucketTagSet>,
    bucket_cors: BTreeMap<String, BucketCorsPolicy>,
    pub(crate) bucket_policy_receipts: BTreeMap<String, BucketPolicyMutationReceipt>,
    pub(crate) bucket_cors_receipts: BTreeMap<String, BucketCorsMutationReceipt>,
    multipart_uploads: BTreeMap<String, MultipartUpload>,
    pub(crate) multipart_complete_receipts: BTreeMap<String, MultipartCompleteReceipt>,
    pub(crate) multipart_abort_receipts: BTreeMap<String, MultipartAbortReceipt>,
    object_changes: Vec<ObjectChangeRecord>,
    object_change_cursors: BTreeMap<String, ObjectChangeCursor>,
    next_object_change_sequence: u64,
    pub(crate) meta_service: MatrixMetaService,
    metadata_route_cache: BTreeMap<String, MetadataRouteCacheEntry>,
    /// One route per shard, so the cache entries for that shard's keys share it
    /// rather than each holding a copy. Derived and bounded by the shard count,
    /// and cleared wherever the cache it serves is cleared.
    metadata_route_shared: BTreeMap<String, std::sync::Arc<MetadataShardRoute>>,
    /// One allocation per distinct checksum the cache entries name. A
    /// generation uses three; this holds the few in play and is cleared with
    /// the cache.
    metadata_route_checksums: BTreeMap<String, SharedText>,
    metadata_route_cache_hits: u64,
    metadata_route_cache_misses: u64,
    metadata_route_cache_invalidations: u64,
    metadata_route_cache_expired_invalidations: u64,
    metadata_route_cache_capacity_evictions: u64,
    metadata_route_cache_sequence: u64,
    pub(crate) chunks: BTreeMap<ChunkId, ChunkInfo>,
    /// The chunk a compaction is draining, which must not be allocated into.
    ///
    /// `ensure_writable_chunk` takes the first chunk with room, and with a chunk size that
    /// holds the whole corpus that is always the chunk being compacted -- so the rewrite
    /// landed back where it came from and the compaction then zeroed the live bytes it had
    /// just written. Measured before this: `live_bytes` 819,200 -> 0 with all 200 objects
    /// still readable, and a ratio of 0 because the formula reports zero when its
    /// denominator collapses.
    ///
    /// Transient on purpose: it is set and cleared inside one function and is not in the
    /// snapshot, because a store that is reopened is not mid-compaction.
    pub(crate) compacting_chunk: Option<ChunkId>,
    // Visible to the crate as `chunks` is, so that the invariant the targeted
    // undo derives from -- a stale range holds no payload entry -- can be
    // asserted from a test rather than argued for in a comment.
    pub(crate) payloads: PayloadIndex,
    pub(crate) chunk_service: MatrixChunkService,
    next_chunk_id: u64,
    next_upload_sequence: u64,
    logical_written_bytes: u64,
    physical_written_bytes: u64,
    last_object_manifest_checkpoint_index: u64,
    last_object_metadata_checkpoint_index: u64,
    last_applied_index: u64,
    last_applied_term: u64,
    applied_raft_receipts: BTreeMap<u64, RaftApplyReceipt>,
    raft_voter_node_ids: Vec<String>,
    raft_healthy_peer_count: u64,
    raft_membership_epoch: u64,
    raft_membership_receipts: BTreeMap<String, RaftMembershipReceipt>,
    consensus_read_refreshes: Cell<u64>,
    consensus_read_cache_hits: Cell<u64>,
    consensus_read_last_status: RefCell<String>,
    chunk_read_cache_max_bytes_per_replica: u64,
    chunk_read_cache_max_cacheable_read_bytes_per_replica: u64,
    snapshot_export_cache: RefCell<Option<StoreSnapshotExportCache>>,
    persistent_snapshot_durable_stamp: RefCell<Option<PersistentSnapshotDurableStamp>>,
    snapshot_assembler: StoreSnapshotChunkAssembler,
}

#[derive(Clone, Debug)]
/// What a put changed in the three containers it does not copy.
///
/// A put inserts one `buckets` entry, writes one `objects` key and appends one
/// `object_changes` record, so each can be undone exactly. Copying those three
/// maps instead is 83.7% of the allocations a put makes.
pub(crate) struct PutTargetedUndo {
    meta: MetaRecordUndo,
    bucket: String,
    bucket_existed: bool,
    object_id: ObjectId,
    previous_object: Option<ObjectMetadata>,
    /// The payload entries naming the previous object's extents.
    ///
    /// A put that replaces an object marks the old extents stale, and
    /// `mark_extents_stale` removes the payload entry for each one -- so these
    /// are the entries the mutation destroys, and they are not derivable
    /// afterwards: the entry holds a length and a digest, and the store keeps
    /// the bytes nowhere. One object's worth, so this does not grow with the
    /// store the way copying the whole index did.
    previous_payloads: Vec<((ChunkId, u64), StoredPayload)>,
}

pub(crate) struct ObjectMutationRollback {
    /// Present only for the put path, where the three containers below are
    /// undone by hand and so are captured empty.
    put_targeted: Option<PutTargetedUndo>,
    buckets: BTreeSet<SharedText>,
    bucket_versioning: BTreeSet<String>,
    bucket_retention: BTreeMap<String, u64>,
    bucket_quotas: BTreeMap<String, BucketQuota>,
    bucket_lifecycle: BTreeMap<String, BucketLifecyclePolicy>,
    bucket_policy_documents: BTreeMap<String, BucketPolicyDocument>,
    bucket_tags: BTreeMap<String, BucketTagSet>,
    bucket_cors: BTreeMap<String, BucketCorsPolicy>,
    bucket_policy_receipts: BTreeMap<String, BucketPolicyMutationReceipt>,
    bucket_cors_receipts: BTreeMap<String, BucketCorsMutationReceipt>,
    objects: BTreeMap<ObjectId, ObjectMetadata>,
    versions: BTreeMap<ObjectId, Vec<ObjectMetadata>>,
    object_changes: Vec<ObjectChangeRecord>,
    next_object_change_sequence: u64,
    multipart_uploads: BTreeMap<String, MultipartUpload>,
    multipart_complete_receipts: BTreeMap<String, MultipartCompleteReceipt>,
    multipart_abort_receipts: BTreeMap<String, MultipartAbortReceipt>,
    next_upload_sequence: u64,
    meta_service: Option<MatrixMetaService>,
    chunks: BTreeMap<ChunkId, ChunkInfo>,
    payloads: PayloadIndex,
    /// Absent on the put path, where the page service's own mutation batch
    /// holds the undo and the put drives its abort.
    chunk_service: Option<MatrixChunkService>,
    logical_written_bytes: u64,
    physical_written_bytes: u64,
    last_object_manifest_checkpoint_index: u64,
    last_object_metadata_checkpoint_index: u64,
}

fn chunk_catalog_service_mode(readiness: &ReplicaStorageReadiness) -> String {
    if readiness.draining {
        "draining".to_string()
    } else if !readiness.accepting_writes {
        "read_only".to_string()
    } else {
        "read_write".to_string()
    }
}

fn catalog_chunk_draining(readiness: &ReplicaStorageReadiness) -> bool {
    readiness.draining || !readiness.accepting_writes || readiness.frozen
}

fn catalog_capacity_bytes(readiness: &ReplicaStorageReadiness) -> u64 {
    if readiness.capacity_bytes == 0 {
        u64::MAX
    } else {
        readiness.capacity_bytes
    }
}

fn root_chunk_health_from_readiness(readiness: &ReplicaStorageReadiness) -> RootChunkHealth {
    let mut service_health_reasons = Vec::new();
    if !readiness.ready && !readiness.reason.is_empty() && readiness.reason != "ok" {
        service_health_reasons.push(readiness.reason.clone());
    }
    if !readiness.readable {
        service_health_reasons.push("read_unavailable".to_string());
    }
    if !readiness.writable {
        service_health_reasons.push("write_unavailable".to_string());
    }
    if !readiness.accepting_writes {
        service_health_reasons.push("not_accepting_writes".to_string());
    }
    if readiness.frozen {
        service_health_reasons.push("frozen".to_string());
    }
    if readiness.draining {
        service_health_reasons.push("draining".to_string());
    }
    if readiness.capacity_pressure {
        service_health_reasons.push("capacity_pressure".to_string());
    }
    if readiness.read_cache_corrupt_pages != 0 || readiness.read_cache_corrupt_bytes != 0 {
        service_health_reasons.push("read_cache_corruption".to_string());
    }
    if readiness.ec_reconstruction_failures != 0 {
        service_health_reasons.push("ec_reconstruction_failures".to_string());
    }
    if readiness.partial_reclaim_failures != 0 {
        service_health_reasons.push("partial_reclaim_failures".to_string());
    }
    if readiness.capacity_rejected_writes != 0 {
        service_health_reasons.push("capacity_rejected_writes".to_string());
    }
    if readiness.service_mode_rejected_mutations != 0 {
        service_health_reasons.push("service_mode_rejected_mutations".to_string());
    }
    service_health_reasons.sort();
    service_health_reasons.dedup();
    let service_fault_tolerant = readiness.ready
        && readiness.readable
        && readiness.writable
        && readiness.accepting_writes
        && !readiness.frozen
        && !readiness.draining
        && !readiness.capacity_pressure
        && readiness.read_cache_corrupt_pages == 0
        && readiness.read_cache_corrupt_bytes == 0
        && readiness.ec_reconstruction_failures == 0
        && readiness.partial_reclaim_failures == 0
        && readiness.capacity_rejected_writes == 0
        && readiness.service_mode_rejected_mutations == 0;
    let service_health = if service_fault_tolerant {
        "ok"
    } else if !readiness.readable {
        "unavailable"
    } else {
        "degraded"
    };
    RootChunkHealth {
        accepting_writes: readiness.accepting_writes,
        frozen: readiness.frozen,
        raft_writable: true,
        capacity_free_bytes: readiness.capacity_free_bytes,
        discarded_range_count: readiness.discarded_range_count,
        reclaimable_bytes: readiness.reclaimable_bytes,
        trailing_trim_bytes: readiness.trailing_trim_bytes,
        fragmented_chunk_count: readiness.fragmented_chunk_count,
        fragmented_discarded_bytes: readiness.fragmented_discarded_bytes,
        service_mode: chunk_catalog_service_mode(readiness),
        control_proof_format: readiness.replicaserver_control_proof_format.clone(),
        control_proof_checksum: readiness.replicaserver_control_proof_checksum.clone(),
        control_proof_bytes: readiness.replicaserver_control_proof_bytes,
        raft_commit_index: readiness.control_proof.chunk_mutation_control_commit_index,
        control_read_ready: readiness.readable,
        control_write_ready: readiness.writable,
        service_health: service_health.to_string(),
        service_health_reason_count: service_health_reasons.len() as u64,
        service_health_reasons: service_health_reasons.join(";"),
        service_fault_tolerant,
    }
}

fn catalog_page_draining(readiness: &ChunkStorageReadiness) -> bool {
    !readiness.writable || readiness.quarantined_replicas >= readiness.replica_count
}

fn catalog_page_capacity_bytes(readiness: &ChunkStorageReadiness) -> u64 {
    if readiness.total_capacity_bytes == 0 {
        u64::MAX
    } else {
        readiness.total_capacity_bytes
    }
}

fn root_page_health_from_readiness(readiness: &ChunkStorageReadiness) -> RootPageHealth {
    RootPageHealth {
        ready: readiness.writable,
        read_ready: readiness.readable,
        route_control_checksum: readiness.chunk_route_control_proof_checksum.clone(),
        route_control_commit_index: readiness
            .route_control_proof
            .chunk_route_control_commit_index,
        chunk_endpoint_count: readiness.chunk_endpoint_count,
        write_quorum: readiness.write_quorum,
        read_quorum: readiness.read_quorum,
        writable_chunk_endpoint_count: readiness.effective_writable_chunk_endpoints,
        healthy_chunk_endpoint_count: readiness.readable_replicas,
        raft_writable_chunk_endpoint_count: readiness.writable_replicas,
        raft_readable_chunk_endpoint_count: readiness.readable_replicas,
        capacity_ready_writable_chunk_endpoint_count: readiness
            .writable_replicas
            .saturating_sub(readiness.capacity_pressure_replicas),
        quarantined_chunk_replicas: readiness.quarantined_replicas,
        chunk_capacity_bytes: readiness.total_capacity_bytes,
        chunk_capacity_free_bytes: readiness.total_capacity_free_bytes,
        chunk_metadata_dirty_endpoint_count: readiness.metadata_dirty_replicas,
        chunk_metadata_dirty_mutations: readiness.metadata_dirty_mutations,
        chunk_metadata_checkpoint_interval: readiness.max_metadata_checkpoint_interval,
        deferred_metadata_checkpoint_endpoint_count: readiness
            .deferred_metadata_checkpoint_replicas,
        deferred_metadata_checkpoint_requests: readiness.deferred_metadata_checkpoint_requests,
        deferred_metadata_checkpoint_runs: readiness.deferred_metadata_checkpoint_runs,
        chunk_capacity_rejected_writes: readiness.capacity_rejected_writes,
        replica_service_mode_rejected_mutations: readiness.service_mode_rejected_mutations,
        chunk_physical_write_count: readiness.physical_write_count,
        chunk_physical_write_bytes: readiness.physical_write_bytes,
        chunk_physical_discard_range_count: readiness.physical_discard_ranges,
        chunk_physical_discard_bytes: readiness.physical_discard_bytes,
        total_reclaimable_bytes: readiness.total_reclaimable_bytes,
        total_trailing_trim_bytes: readiness.total_trailing_trim_bytes,
        total_fragmented_chunk_count: readiness.total_fragmented_chunk_count,
        total_fragmented_discarded_bytes: readiness.total_fragmented_discarded_bytes,
        allocation_receipt_count: readiness.allocation_receipt_count,
        batch_allocation_receipt_count: readiness.batch_allocation_receipt_count,
        release_allocation_receipt_count: readiness.release_allocation_receipt_count,
        write_receipt_count: readiness.write_receipt_count,
        batch_write_receipt_count: readiness.batch_write_receipt_count,
        discard_receipt_count: readiness.discard_receipt_count,
        batch_discard_receipt_count: readiness.batch_discard_receipt_count,
    }
}

include!("core/construction_control.inc.rs");
include!("core/readiness_catalog.inc.rs");
include!("core/metadata_object_api.inc.rs");
include!("core/object_read_lifecycle.inc.rs");
include!("core/storage_service_control.inc.rs");
include!("core/raft_apply_persistence.inc.rs");
include!("core/footprint_audit.rs");
