impl MatrixObjectStore {
    pub fn can_accept_chunk_batch_write(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        self.chunk_service.can_accept_chunk_replica_batch_write(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
        )
    }

    pub fn can_accept_chunk_batch_write_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        self.chunk_service
            .can_accept_chunk_replica_batch_write_fenced(
                replica_index,
                chunk_id,
                writes,
                max_write_bytes,
                expected_control_checksum,
            )
    }

    pub fn can_accept_chunk_write(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        let mut admission = replica.can_accept_chunk_write(chunk_id, offset, length)?;
        admission.max_write_bytes = max_write_bytes;
        admission.budget_limited = max_write_bytes > 0 && length > max_write_bytes;
        admission.can_accept = admission.can_accept && !admission.budget_limited;
        Ok(admission)
    }

    pub fn can_accept_chunk_write_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_write(replica_index, chunk_id, offset, length, max_write_bytes)
    }

    pub fn can_accept_chunk_batch_discard(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.chunk_service.can_accept_chunk_replica_batch_discard(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
        )
    }

    pub fn can_accept_chunk_batch_discard_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.chunk_service
            .can_accept_chunk_replica_batch_discard_fenced(
                replica_index,
                chunk_id,
                ranges,
                max_discard_bytes,
                expected_control_checksum,
            )
    }

    pub fn can_accept_chunk_discard(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.can_accept_chunk_discards(
            chunk_id,
            vec![DataRange { offset, length }],
            max_discard_bytes,
        )
    }

    pub fn can_accept_chunk_discard_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_discard(replica_index, chunk_id, offset, length, max_discard_bytes)
    }

    pub fn can_accept_chunk_batch_read(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
    ) -> Result<ReplicaReadAdmission, ObjectError> {
        self.chunk_service.can_accept_chunk_replica_batch_read(
            replica_index,
            chunk_id,
            ranges,
            max_payload_bytes,
        )
    }

    pub fn can_accept_chunk_batch_read_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaReadAdmission, ObjectError> {
        self.chunk_service
            .can_accept_chunk_replica_batch_read_fenced(
                replica_index,
                chunk_id,
                ranges,
                max_payload_bytes,
                expected_control_checksum,
            )
    }

    pub fn can_accept_chunk_read(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ReplicaReadAdmission, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.can_accept_chunk_reads(
            chunk_id,
            vec![DataRange { offset, length }],
            max_payload_bytes,
        )
    }

    pub fn can_accept_chunk_read_fenced(
        &self,
        replica_index: usize,
        chunk_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaReadAdmission, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_read(replica_index, chunk_id, offset, length, max_payload_bytes)
    }

    pub fn chunk_replica_mutation_qos_configs(&self) -> Vec<ReplicaMutationQosConfig> {
        self.chunk_service.chunk_replica_mutation_qos_configs()
    }

    pub fn chunk_replica_mutation_qos_states(&self) -> Vec<ReplicaMutationQosState> {
        self.chunk_service.chunk_replica_mutation_qos_states()
    }

}
