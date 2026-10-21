// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreOptions {
    pub chunk_size: u64,
    pub max_extent_bytes: u64,
    pub page_size: u64,
    pub volume_count: u64,
    pub persistent_snapshot_path: String,
    pub persistent_snapshot_flush_on_commit: bool,
    pub persistent_journal_path: String,
    pub replica_count: usize,
    pub write_quorum: usize,
    /// Units of redundancy in a stripe group, or 0 to copy every page to every replica.
    ///
    /// Non-zero makes the store hold `replica_count / (replica_count - stripe_redundancy_units)`
    /// of its logical payload instead of one copy per replica -- 1.5x instead of 3.0x at
    /// three replicas and one unit of redundancy -- and it survives that many losses
    /// instead of `replica_count - 1`. It is a durability trade and the default is
    /// replication, unchanged.
    ///
    /// `data_units` is not a separate setting: every replica holds exactly one unit of each
    /// group, so it is whatever the replica count leaves. A third knob could disagree with
    /// the replica set, and a durability setting that silently gives less than it was asked
    /// for is the one mistake this must not make.
    pub stripe_redundancy_units: usize,
    /// Bytes in one stripe unit, when `stripe_redundancy_units` is non-zero.
    ///
    /// Fixed for the life of a page, so byte `p` stays in unit `p / stripe_unit_bytes` and an
    /// append never moves a boundary. See `EcStripeLayout`.
    pub stripe_unit_bytes: usize,
    /// Take the space a superseded object leaves instead of growing the chunk.
    ///
    /// Off by default: reclaiming immediately means an overwrite stops
    /// accumulating stale bytes for the collector, which changes what
    /// `gc_report` reports.
    pub reuse_stale_chunk_space: bool,
    pub compaction_fragment_threshold: usize,
    pub object_manifest_checkpoint_interval: u64,
    pub object_metadata_checkpoint_interval: u64,
    pub metadata_route_cache_entries: usize,
    pub metadata_route_cache_ttl_ms: u64,
    pub page_read_parallelism: usize,
    pub page_write_parallelism: usize,
    pub page_batch_read_parallelism: usize,
    pub page_batch_write_parallelism: usize,
    pub page_batch_discard_parallelism: usize,
    pub page_repair_parallelism: usize,
    pub replica_fanout_parallelism: usize,
    pub local_replica_store_parallelism: usize,
    pub control_plane_mode: ControlPlaneMode,
    pub metaserver_catalog_endpoints: String,
    pub rootserver_catalog_endpoint: String,
}

impl StoreOptions {
    /// The stripe layout these options ask for, or `None` to replicate.
    ///
    /// Refused rather than clamped when the numbers cannot describe a layout: a store that
    /// asked for more redundancy than it has replicas would otherwise get silently less than
    /// it asked for.
    ///
    /// The write-quorum rule -- a striped write needs every replica, because a unit has no
    /// peer -- is NOT repeated here. `MatrixChunkService::configure_stripe_layout` owns it,
    /// and a store that asks for a layout with a short quorum is refused there, by name. A
    /// copy of the rule in this function cannot be made to fire by any input, so it would be
    /// a guard no test can reach.
    pub fn stripe_layout(&self) -> Result<Option<EcStripeLayout>, ObjectError> {
        if self.stripe_redundancy_units == 0 {
            return Ok(None);
        }
        Ok(Some(EcStripeLayout::for_replicas(
            self.replica_count,
            self.stripe_redundancy_units,
            self.stripe_unit_bytes,
        )?))
    }
}

