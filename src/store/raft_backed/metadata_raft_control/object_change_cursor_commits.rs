// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn commit_object_change_cursor_linearizable(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
        now_ms: u64,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.commit_object_change_cursor_linearizable_committed_at(
            consumer_id,
            sequence,
            update_time_ms,
            now_ms,
            self.raft.commit_index(),
        )
    }

    pub fn commit_object_change_cursor_linearizable_committed_at(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "object change cursor commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let metadata =
            self.propose_commit_object_change_cursor(consumer_id, sequence, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "object change cursor commit result",
            now_ms,
            self.raft.commit_index(),
            self.raft.commit_index(),
        )?;
        Ok((metadata, readiness))
    }

    pub fn commit_object_change_cursor_with_read_proof_linearizable(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        let (metadata, readiness) = self.commit_object_change_cursor_linearizable(
            consumer_id,
            sequence,
            update_time_ms,
            now_ms,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn commit_object_change_cursor_with_read_proof_linearizable_committed_at(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        let (metadata, readiness) = self.commit_object_change_cursor_linearizable_committed_at(
            consumer_id,
            sequence,
            update_time_ms,
            now_ms,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }
}
