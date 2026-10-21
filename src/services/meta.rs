// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
include!("meta/service_state.inc.rs");
include!("meta/records_leases_shards.inc.rs");
include!("meta/retention_transactions_routes.inc.rs");
include!("meta/sessions_changefeed_snapshot.inc.rs");
include!("meta/maintenance_persistence.inc.rs");
include!("meta/footprint.rs");

// The placement service, folded in: these live in this module because the
// state they operate on is private to it.
include!("meta/placement/membership.rs");
include!("meta/placement/coverage_raft.rs");
include!("meta/placement/object_placement.rs");
include!("meta/placement/placement_migration_planning.rs");
include!("meta/placement/placement_migration_raft.rs");
include!("meta/placement/repair_rebalance.rs");
include!("meta/placement/queries_health.rs");
include!("meta/placement/leadership_snapshot.rs");
include!("meta/placement/placement_policy_internals.rs");
include!("meta/placement/placement_repair_internals.rs");
include!("meta/placement/placement_capacity_internals.rs");
include!("meta/placement/leader_validation.rs");
