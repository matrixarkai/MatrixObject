use super::*;
include!("meta/service_state.inc.rs");
include!("meta/records_leases_shards.inc.rs");
include!("meta/retention_transactions_routes.inc.rs");
include!("meta/sessions_changefeed_snapshot.inc.rs");
include!("meta/maintenance_persistence.inc.rs");

// The placement service, folded in: these live in this module because the
// state they operate on is private to it.
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
