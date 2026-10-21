// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_split_oversized_meta_shards(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SplitOversizedMetaShards {
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            expected_plan_checksum: String::new(),
            expected_shard_topology_epoch: None,
            expected_shard_topology_checksum: String::new(),
        })
    }

    pub fn propose_split_oversized_meta_shards_fenced(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SplitOversizedMetaShards {
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            expected_plan_checksum: expected_plan_checksum.to_string(),
            expected_shard_topology_epoch,
            expected_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

    pub fn plan_oversized_meta_shard_splits_with_read_proof_linearizable(
        &self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardSplitReport>, ObjectError> {
        self.plan_oversized_meta_shard_splits_with_read_proof_linearizable_committed_at(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_oversized_meta_shard_splits_with_read_proof_linearizable_committed_at(
        &self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataShardSplitReport>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "oversized metadata shard split plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.store.plan_oversized_metadata_shard_splits(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_proven_split_oversized_meta_shards_fenced(
        &mut self,
        proven_plan: ProvenMetadataRead<MetadataShardSplitReport>,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if !expected_plan_checksum.is_empty()
            && proven_plan.value.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "oversized metadata shard split proven plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, proven_plan.value.plan_checksum
            )));
        }
        if self.store.last_metadata_commit_index() < proven_plan.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven oversized metadata shard split proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_plan.proof.metadata_commit_index
            )));
        }
        self.propose_split_oversized_meta_shards_fenced(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            &proven_plan.value.plan_checksum,
            Some(proven_plan.value.metadata_shard_topology_epoch),
            Some(&proven_plan.value.metadata_shard_topology_checksum),
        )
    }

    pub fn propose_split_oversized_meta_shards_linearizable_fenced(
        &mut self,
        max_records_per_shard: usize,
        max_payload_bytes_per_shard: u64,
        max_splits: usize,
        now_ms: u64,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let proven_plan = self.plan_oversized_meta_shard_splits_with_read_proof_linearizable(
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
        )?;
        self.propose_proven_split_oversized_meta_shards_fenced(
            proven_plan,
            max_records_per_shard,
            max_payload_bytes_per_shard,
            max_splits,
            now_ms,
            expected_plan_checksum,
        )
    }
}
