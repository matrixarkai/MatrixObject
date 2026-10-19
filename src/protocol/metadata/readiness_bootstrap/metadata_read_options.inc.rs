// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataReadOptions {
    pub min_read_index: u64,
    pub now_ms: u64,
    pub require_shard_owner_serving: bool,
    pub expected_shard_owner: Option<String>,
    pub expected_shard_epoch: Option<u64>,
}

impl MetadataRaftReadiness {
    pub(crate) fn finalize(&mut self) {
        self.safe_read_fence = self
            .raft_log_commit_index
            .min(self.applied_commit_index)
            .min(self.last_applied_metadata_mutation_commit_index);
        self.read_fence_commit_lag = self
            .min_read_index
            .saturating_sub(self.raft_log_commit_index);
        self.read_fence_read_index_lag = self
            .min_read_index
            .saturating_sub(self.applied_commit_index);
        self.read_fence_applied_lag = self
            .min_read_index
            .saturating_sub(self.last_applied_metadata_mutation_commit_index);
        self.read_lag = self
            .min_read_index
            .saturating_sub(self.last_applied_metadata_mutation_commit_index);
        self.raft_applied_lag = self
            .raft_log_commit_index
            .saturating_sub(self.applied_commit_index);
        self.metadata_apply_commit_lag = self
            .applied_commit_index
            .saturating_sub(self.last_applied_metadata_mutation_commit_index);
        self.metadata_apply_caught_up = self.metadata_apply_commit_lag == 0;
        if !self.external_coordinator_required {
            self.external_coordinator_endpoints_ready = true;
        }
        self.external_coordinator_owner_sessions_ready = !self.external_coordinator_required
            || self.total_shards == 0
            || self.stale_owner_shards == 0;
        self.external_coordinator_ready = self.external_coordinator_endpoints_ready
            && self.external_coordinator_owner_sessions_ready;
        self.readable = self.read_lag == 0;
        self.read_fence_covered = self.readable
            && self.read_fence_commit_lag == 0
            && self.read_fence_read_index_lag == 0
            && self.read_fence_applied_lag == 0;
        self.caught_up = self.raft_applied_lag == 0;
        self.writable = self.stale_owner_shards == 0;
        self.metadata_commit_bounds_valid =
            self.max_retained_metadata_commit_index <= self.applied_commit_index;
        self.ready = self.readable
            && self.writable
            && self.caught_up
            && self.metadata_commit_bounds_valid
            && self.metadata_apply_caught_up
            && self.external_coordinator_ready;
        self.reason = if self.ready {
            "ready".to_string()
        } else if !self.readable {
            "read_index_lag".to_string()
        } else if !self.caught_up {
            "raft_apply_lag".to_string()
        } else if !self.metadata_apply_caught_up {
            "metadata_apply_lag".to_string()
        } else if !self.metadata_commit_bounds_valid {
            "metadata_commit_bounds_exceed_applied_index".to_string()
        } else if !self.external_coordinator_ready {
            "external_coordinator_not_ready".to_string()
        } else {
            "stale_shard_owner_session".to_string()
        };
    }

    pub(crate) fn with_raft_position(mut self, raft_term: u64, raft_log_commit_index: u64) -> Self {
        self.raft_term = raft_term;
        self.raft_log_commit_index = raft_log_commit_index.max(self.applied_commit_index);
        self.finalize();
        self
    }
}
