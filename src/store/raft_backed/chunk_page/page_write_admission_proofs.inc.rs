impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn can_accept_page_batch_write_with_read_proof_linearizable_fenced(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_read_proof_linearizable_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_write_with_read_proof_linearizable_fenced_from(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_read_proof_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_write_with_route_proof_linearizable_fenced(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_route_proof_linearizable_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_write_with_route_proof_linearizable_fenced_from(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_route_proof_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_write_with_route_proof_linearizable_fenced_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_route_proof_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_write_with_route_proof_linearizable_fenced_from_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPageRouteFencedAdmissionRead<PageRangeAdmission>, ObjectError> {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let proven = self
            .can_accept_page_batch_write_with_read_proof_linearizable_fenced_from_committed_at(
                writes,
                max_write_bytes,
                max_chunk_write_count,
                write_start_range_index,
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

    pub fn can_accept_page_batch_write_with_read_proof_linearizable_fenced_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_read_proof_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_write_with_read_proof_linearizable_fenced_from_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let value = self.can_accept_page_batch_write_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let proof =
            self.page_admission_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenAdmissionRead { value, proof })
    }

    pub fn can_accept_page_batch_write_strict_linearizable_fenced(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_strict_linearizable_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
            expected_admission_proof_checksum,
        )
    }

    pub fn can_accept_page_batch_write_strict_linearizable_fenced_from(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_strict_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
            expected_admission_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_write_strict_linearizable_fenced_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_strict_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
            expected_admission_proof_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_write_strict_linearizable_fenced_from_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        expected_admission_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmissionRead<PageRangeAdmission>, ObjectError> {
        self.can_accept_page_batch_write_with_read_proof_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?
        .require_page_admission_proof_checksum(expected_admission_proof_checksum)
    }
}
