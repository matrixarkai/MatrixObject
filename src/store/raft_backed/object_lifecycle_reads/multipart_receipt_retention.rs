impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_trim_multipart_receipts(
        &mut self,
        retain_complete_receipts: usize,
        retain_abort_receipts: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::TrimMultipartReceipts {
            retain_complete_receipts,
            retain_abort_receipts,
            update_time_ms,
        })
    }

    pub fn propose_trim_multipart_receipts_with_read_proof_linearizable(
        &mut self,
        retain_complete_receipts: usize,
        retain_abort_receipts: usize,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_trim_multipart_receipts_with_read_proof_linearizable_committed_at(
            retain_complete_receipts,
            retain_abort_receipts,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_trim_multipart_receipts_with_read_proof_linearizable_committed_at(
        &mut self,
        retain_complete_receipts: usize,
        retain_abort_receipts: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_requested_read_index(
            "multipart receipt retention trim",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_trim_multipart_receipts(
            retain_complete_receipts,
            retain_abort_receipts,
            update_time_ms,
        )?;
        let readiness = self.ensure_metadata_requested_read_index(
            "multipart receipt retention trim post-commit",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_trim_multipart_receipts_from_meta_shard(
        &mut self,
        retain_complete_receipts: usize,
        retain_abort_receipts: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            "_multipartreceipts",
            "trim",
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "multipart receipt retention trim from meta shard",
            min_read_index,
            min_commit_index,
        )?;
        self.propose_trim_multipart_receipts_with_read_proof_linearizable_committed_at(
            retain_complete_receipts,
            retain_abort_receipts,
            update_time_ms,
            min_read_index,
            min_commit_index,
        )
    }
}
