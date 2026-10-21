// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn plan_oversized_metadata_shard_splits(
        &self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        self.plan_oversized_metadata_shard_splits_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            None,
            None,
        )
    }

    pub fn plan_oversized_metadata_shard_splits_fenced(
        &self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        self.meta_service.plan_oversized_shard_splits_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )
    }

    pub fn split_oversized_metadata_shards(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        self.split_oversized_metadata_shards_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            "",
            None,
            None,
        )
    }

    pub fn split_oversized_metadata_shards_fenced(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<MetadataShardSplitReport, ObjectError> {
        let report = self.meta_service.split_oversized_shards_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            expected_plan_checksum,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )?;
        if report.split_shards > 0 {
            self.invalidate_metadata_route_cache();
        }
        Ok(report)
    }

    pub fn metadata_shards(&self) -> Vec<MetadataShard> {
        self.meta_service.metadata_shards()
    }

    pub fn metadata_shard(&self, shard_id: &str) -> Result<MetadataShard, ObjectError> {
        self.meta_service.metadata_shard(shard_id)
    }

    pub fn metadata_raft_readiness(
        &self,
        min_read_index: u64,
        now_ms: u64,
    ) -> MetadataRaftReadiness {
        let mut readiness = self.meta_service.raft_readiness(min_read_index, now_ms);
        readiness.raft_membership_receipt_count = self.raft_membership_receipts.len() as u64;
        let route_cache = self.metadata_route_cache_status_at(now_ms);
        let metadata_replication_quorum = raft_quorum(self.raft_voter_node_ids.len() as u64);
        let metadata_replication_acks = self
            .raft_healthy_peer_count
            .min(self.raft_voter_node_ids.len() as u64);
        let proof = metadata_control_proof(
            &readiness,
            &route_cache,
            self.raft_membership_epoch,
            self.raft_membership_receipts.len() as u64,
            metadata_replication_quorum,
            metadata_replication_acks,
        );
        readiness.metadata_control_proof_format = proof.state_format;
        readiness.metadata_control_proof_checksum = proof.state_checksum;
        readiness.metadata_control_proof_bytes = proof.state_bytes;
        readiness
    }
}
