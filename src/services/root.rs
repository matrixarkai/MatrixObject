use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    checksum: String,
}

#[derive(Clone, Debug)]
pub struct MatrixRootService {
    pub(crate) nodes: BTreeMap<String, RootNode>,
    pub(crate) placements: BTreeMap<ObjectId, ObjectPlacement>,
    leader_lease: Option<RootLeaderLease>,
    leader_lease_receipts: BTreeMap<String, RootLeaderLeaseReceipt>,
    coordinator_config: MetadataCoordinatorConfig,
    pub(crate) next_commit_index: u64,
    snapshot_export_cache: RefCell<Option<RootSnapshotExportCache>>,
    root_state_fingerprint_cache: RefCell<Option<String>>,
}

include!("root/membership.rs");
include!("root/coverage_raft.rs");
include!("root/object_placement.rs");
include!("root/placement_migration_planning.rs");
include!("root/placement_migration_raft.rs");
include!("root/repair_rebalance.rs");
include!("root/queries_health.rs");
include!("root/leadership_snapshot.rs");
include!("root/placement_policy_internals.rs");
include!("root/placement_repair_internals.rs");
include!("root/placement_capacity_internals.rs");
include!("root/leader_validation.rs");

impl Default for MatrixRootService {
    fn default() -> Self {
        Self::new()
    }
}

impl MatrixRootService {
    fn invalidate_snapshot_export_cache(&self) {
        *self.snapshot_export_cache.borrow_mut() = None;
        *self.root_state_fingerprint_cache.borrow_mut() = None;
    }

    fn advance_commit_index(&mut self) {
        self.invalidate_snapshot_export_cache();
        self.next_commit_index = self.next_commit_index.saturating_add(1);
    }
}
