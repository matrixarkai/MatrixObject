impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn can_accept_page_batch_write_linearizable(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_from(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_from_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            None,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_fenced(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_fenced_from(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_fenced_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_linearizable_fenced_from_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn can_accept_page_batch_write_linearizable_fenced_from_committed_at(
        &self,
        writes: Vec<PageRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        let required_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        if self.raft.commit_index() < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "block batch write admission requires raft commit index at least {} but raft is at {}",
                min_commit_index,
                self.raft.commit_index()
            )));
        }
        if self.raft.commit_index() < min_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "block batch write admission requires raft commit index at least {} but raft is at {}",
                min_read_index,
                self.raft.commit_index()
            )));
        }
        if self.store.last_applied_index() < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "block batch write admission requires applied commit index {} but store is at {}",
                required_read_index,
                self.store.last_applied_index()
            )));
        }
        let mut admission = self.store.can_accept_page_batch_write_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
        )?;
        admission.min_read_index = required_read_index;
        admission.min_commit_index = min_commit_index;
        Ok(admission)
    }
}
