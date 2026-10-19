// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_root_capacity_rebalance_prepared(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_root_capacity_rebalance_raft_entry(max_objects, min_free_bytes);
        self.propose_prepared_root_capacity_rebalance(prepared)
    }

    pub fn propose_root_capacity_rebalance_prepared_linearizable(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_root_capacity_rebalance_raft_entry_linearizable(max_objects, min_free_bytes)?;
        self.propose_prepared_root_capacity_rebalance_linearizable(prepared)
    }

    pub fn propose_root_capacity_rebalance_prepared_with_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_root_capacity_rebalance_prepared_with_read_proof_linearizable_committed_at(
            max_objects,
            min_free_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_root_capacity_rebalance_prepared_with_read_proof_linearizable_committed_at(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose root capacity rebalance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self
            .propose_root_capacity_rebalance_prepared_linearizable(max_objects, min_free_bytes)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose root capacity rebalance post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_root_capacity_rebalance_prepared_with_route_proof_linearizable_fenced(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_root_capacity_rebalance_prepared_with_route_proof_linearizable_fenced_committed_at(
            max_objects,
            min_free_bytes,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_root_capacity_rebalance_prepared_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "root capacity rebalance route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_root_capacity_rebalance_prepared_with_read_proof_linearizable_committed_at(
                max_objects,
                min_free_bytes,
                now_ms,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            value: proven.value,
            proof: proven.proof,
            route_control_proof,
            expected_route_control_checksum: expected_checksum,
        })
    }

}
