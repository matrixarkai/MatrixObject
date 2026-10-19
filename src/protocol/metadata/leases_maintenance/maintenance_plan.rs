// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataMaintenanceAction {
    pub action: String,
    pub reason: String,
    pub priority: u64,
    pub key: String,
    pub marker: String,
    pub estimated_items: u64,
    pub estimated_bytes: u64,
}

impl MetadataMaintenanceAction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}action"), self.action.clone());
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields.insert(format!("{field_prefix}priority"), self.priority.to_string());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(format!("{field_prefix}marker"), self.marker.clone());
        fields.insert(
            format!("{field_prefix}estimated_items"),
            self.estimated_items.to_string(),
        );
        fields.insert(
            format!("{field_prefix}estimated_bytes"),
            self.estimated_bytes.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenancePlan {
    pub now_ms: u64,
    pub lease_prefix: String,
    pub lease_limit: usize,
    pub session_limit: usize,
    pub transaction_limit: usize,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub planned_actions: u64,
    pub planned_lease_expirations: u64,
    pub planned_session_expirations: u64,
    pub planned_transaction_expirations: u64,
    pub planned_tombstone_compactions: u64,
    pub planned_tombstone_compaction_bytes: u64,
    pub planned_route_cache_evictions: u64,
    pub route_cache_entries: u64,
    pub route_cache_stale_entries: u64,
    pub route_cache_expired_entries: u64,
    pub needs_maintenance: bool,
    pub can_apply: bool,
    pub reason: String,
    pub plan_checksum: String,
    pub expected_plan_checksum: String,
    pub actual_plan_checksum: String,
    pub readiness: MetadataRaftReadiness,
    pub maintenance_actions: Vec<MetadataMaintenanceAction>,
    pub continuation: MetadataMaintenanceContinuation,
    pub prepared: PreparedMetadataMaintenance,
}

impl MetadataMaintenancePlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("now_ms", self.now_ms),
            ("lease_limit", self.lease_limit as u64),
            ("session_limit", self.session_limit as u64),
            ("transaction_limit", self.transaction_limit as u64),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("planned_actions", self.planned_actions),
            ("planned_lease_expirations", self.planned_lease_expirations),
            (
                "planned_session_expirations",
                self.planned_session_expirations,
            ),
            (
                "planned_transaction_expirations",
                self.planned_transaction_expirations,
            ),
            (
                "planned_tombstone_compactions",
                self.planned_tombstone_compactions,
            ),
            (
                "planned_tombstone_compaction_bytes",
                self.planned_tombstone_compaction_bytes,
            ),
            (
                "planned_route_cache_evictions",
                self.planned_route_cache_evictions,
            ),
            ("route_cache_entries", self.route_cache_entries),
            ("route_cache_stale_entries", self.route_cache_stale_entries),
            (
                "route_cache_expired_entries",
                self.route_cache_expired_entries,
            ),
            ("action_count", self.maintenance_actions.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}lease_prefix"),
            self.lease_prefix.clone(),
        );
        fields.insert(
            format!("{field_prefix}needs_maintenance"),
            self.needs_maintenance.to_string(),
        );
        fields.insert(
            format!("{field_prefix}can_apply"),
            self.can_apply.to_string(),
        );
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_plan_checksum"),
            self.expected_plan_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}actual_plan_checksum"),
            self.actual_plan_checksum.clone(),
        );
        fields.extend(
            object_read_proof_from_readiness(&self.readiness).to_control_fields(field_prefix),
        );
        fields.extend(
            self.continuation
                .to_control_fields(&format!("{field_prefix}continuation.")),
        );
        for (index, action) in self.maintenance_actions.iter().enumerate() {
            fields.extend(action.to_control_fields(&format!("{field_prefix}action.{index}.")));
        }
        fields
    }
}

