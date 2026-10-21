// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
include!("maintenance_persistence/maintenance_transactions.rs");
include!("maintenance_persistence/changefeed.rs");
include!("maintenance_persistence/snapshot_tombstone_retention.rs");
include!("maintenance_persistence/prefix_checkpoint.rs");
include!("maintenance_persistence/snapshot_lifecycle.rs");
include!("maintenance_persistence/mutation_preflight.rs");

impl Default for MatrixMetaService {
    fn default() -> Self {
        Self::new()
    }
}
