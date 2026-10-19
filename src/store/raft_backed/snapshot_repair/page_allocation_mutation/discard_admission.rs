// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn can_accept_page_batch_discard_linearizable(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_discard_from_linearizable(requests, max_discard_bytes, 0)
    }

    pub fn can_accept_page_batch_discard_from_linearizable(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_discard_linearizable_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_discard_linearizable_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_discard_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            None,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_discard_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_discard_from_linearizable_fenced(
            requests,
            max_discard_bytes,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard_from_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_discard_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_discard_linearizable_fenced_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page batch discard admission requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page batch discard admission requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "page batch discard admission requires applied commit index {} but store is at {}",
                required_read_index,
                self.store.last_applied_index()
            )));
        }
        let mut admission = self.store.can_accept_page_batch_discard_from_fenced(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
        )?;
        admission.min_read_index = required_read_index;
        admission.min_commit_index = min_commit_index;
        Ok(admission)
    }

    pub fn can_accept_page_batch_discard_with_read_proof_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_from_with_read_proof_linearizable_fenced(
            requests,
            max_discard_bytes,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard_from_with_read_proof_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_with_read_proof_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_discard_with_route_proof_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_from_with_route_proof_linearizable_fenced(
            requests,
            max_discard_bytes,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard_from_with_route_proof_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_with_route_proof_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_discard_with_route_proof_linearizable_fenced_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let proven = self
            .can_accept_page_batch_discard_with_read_proof_linearizable_fenced_committed_at(
                requests,
                max_discard_bytes,
                discard_start_range_index,
                Some(&expected_checksum),
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenPageRouteFencedAdmissionRead {
            route_control_proof: proven.value.route_control_proof.clone(),
            expected_route_control_checksum: expected_checksum,
            value: proven.value,
            proof: proven.proof,
        })
    }

    pub fn can_accept_page_batch_discard_with_read_proof_linearizable_fenced_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let value = self.can_accept_page_batch_discard_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let proof =
            self.page_admission_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenAdmissionRead { value, proof })
    }

    pub fn can_accept_page_batch_discard_strict_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_from_strict_linearizable_fenced(
            requests,
            max_discard_bytes,
            0,
            expected_route_control_checksum,
            expected_admission_proof_checksum,
        )
    }

    pub fn can_accept_page_batch_discard_from_strict_linearizable_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_strict_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
            expected_admission_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_discard_strict_linearizable_fenced_committed_at(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<ChunkRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_discard_with_read_proof_linearizable_fenced_committed_at(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?
        .require_page_admission_proof_checksum(expected_admission_proof_checksum)
    }
}
