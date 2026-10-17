#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataIntegrityPlan {
    pub plan_format: String,
    pub now_ms: u64,
    pub max_sample_count: usize,
    pub tombstone_prefix: String,
    pub min_read_index: u64,
    pub readiness: MetadataRaftReadiness,
    pub control_proof: MetadataControlProof,
    pub live_record_count: u64,
    pub deleted_record_count: u64,
    pub compactable_tombstone_count: u64,
    pub compactable_tombstones: Vec<MetadataRecord>,
    pub active_session_count: u64,
    pub expired_session_count: u64,
    pub expired_sessions: Vec<CoordinatorSession>,
    pub active_lease_count: u64,
    pub expired_lease_count: u64,
    pub orphaned_lease_owner_count: u64,
    pub expired_leases: Vec<MetadataLease>,
    pub open_transaction_count: u64,
    pub expired_transaction_count: u64,
    pub expired_transactions: Vec<MetadataTransaction>,
    pub writable_shard_count: u64,
    pub draining_shard_count: u64,
    pub stale_owner_shard_count: u64,
    pub stale_owner_shards: Vec<MetadataShard>,
    pub change_log_count: u64,
    pub change_checkpoint_count: u64,
    pub safe_change_sequence: u64,
    pub compactable_change_count: u64,
    pub route_cache_entry_count: u64,
    pub route_cache_stale_entries: u64,
    pub route_cache_expired_entries: u64,
    pub needs_owner_failover: bool,
    pub needs_session_expiry: bool,
    pub needs_lease_expiry: bool,
    pub needs_transaction_gc: bool,
    pub needs_tombstone_compaction: bool,
    pub needs_changefeed_compaction: bool,
    pub needs_route_cache_eviction: bool,
    pub needs_snapshot_compaction: bool,
    pub needs_read_repair: bool,
    pub needs_action: bool,
    pub plan_checksum: String,
    pub plan_bytes: u64,
}

impl MetadataIntegrityPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("now_ms", self.now_ms),
            ("max_sample_count", self.max_sample_count as u64),
            ("min_read_index", self.min_read_index),
            ("live_record_count", self.live_record_count),
            ("deleted_record_count", self.deleted_record_count),
            (
                "compactable_tombstone_count",
                self.compactable_tombstone_count,
            ),
            ("active_session_count", self.active_session_count),
            ("expired_session_count", self.expired_session_count),
            ("active_lease_count", self.active_lease_count),
            ("expired_lease_count", self.expired_lease_count),
            (
                "orphaned_lease_owner_count",
                self.orphaned_lease_owner_count,
            ),
            ("open_transaction_count", self.open_transaction_count),
            ("expired_transaction_count", self.expired_transaction_count),
            ("writable_shard_count", self.writable_shard_count),
            ("draining_shard_count", self.draining_shard_count),
            ("stale_owner_shard_count", self.stale_owner_shard_count),
            ("change_log_count", self.change_log_count),
            ("change_checkpoint_count", self.change_checkpoint_count),
            ("safe_change_sequence", self.safe_change_sequence),
            ("compactable_change_count", self.compactable_change_count),
            ("route_cache_entry_count", self.route_cache_entry_count),
            ("route_cache_stale_entries", self.route_cache_stale_entries),
            (
                "route_cache_expired_entries",
                self.route_cache_expired_entries,
            ),
            ("plan_bytes", self.plan_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}plan_format"),
            self.plan_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}tombstone_prefix"),
            self.tombstone_prefix.clone(),
        );
        for (name, value) in [
            ("needs_owner_failover", self.needs_owner_failover),
            ("needs_session_expiry", self.needs_session_expiry),
            ("needs_lease_expiry", self.needs_lease_expiry),
            ("needs_transaction_gc", self.needs_transaction_gc),
            (
                "needs_tombstone_compaction",
                self.needs_tombstone_compaction,
            ),
            (
                "needs_changefeed_compaction",
                self.needs_changefeed_compaction,
            ),
            (
                "needs_route_cache_eviction",
                self.needs_route_cache_eviction,
            ),
            ("needs_snapshot_compaction", self.needs_snapshot_compaction),
            ("needs_read_repair", self.needs_read_repair),
            ("needs_action", self.needs_action),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        fields.extend(
            object_read_proof_from_readiness(&self.readiness).to_control_fields(field_prefix),
        );
        fields.extend(
            self.control_proof
                .to_control_fields(&format!("{field_prefix}control.")),
        );
        for (index, record) in self.compactable_tombstones.iter().enumerate() {
            fields.extend(
                record.to_control_fields(&format!("{field_prefix}compactable_tombstone.{index}.")),
            );
        }
        for (index, session) in self.expired_sessions.iter().enumerate() {
            fields.extend(
                session.to_control_fields(&format!("{field_prefix}expired_session.{index}.")),
            );
        }
        for (index, lease) in self.expired_leases.iter().enumerate() {
            fields
                .extend(lease.to_control_fields(&format!("{field_prefix}expired_lease.{index}.")));
        }
        for (index, transaction) in self.expired_transactions.iter().enumerate() {
            fields.extend(
                transaction
                    .to_control_fields(&format!("{field_prefix}expired_transaction.{index}.")),
            );
        }
        for (index, shard) in self.stale_owner_shards.iter().enumerate() {
            fields.extend(
                shard.to_control_fields(&format!("{field_prefix}stale_owner_shard.{index}.")),
            );
        }
        fields
    }
}

