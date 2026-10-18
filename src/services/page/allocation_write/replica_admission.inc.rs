impl MatrixPageService {
    pub fn can_accept_chunk_replica_batch_write(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        self.can_accept_chunk_replica_batch_write_from(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_write_from(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let mut borrowed = Vec::with_capacity(writes.len());
        for (offset, data) in &writes {
            borrowed.push((*offset, data.as_slice()));
        }
        self.can_accept_chunk_replica_batch_write_from_borrowed(
            replica_index,
            chunk_id,
            &borrowed,
            max_write_bytes,
            write_start_range_index,
        )
    }

    pub fn can_accept_chunk_replica_batch_write_from_borrowed(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        writes: &[(u64, &[u8])],
        max_write_bytes: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        let mut admission = replica.can_accept_chunk_writes_from_borrowed(
            chunk_id,
            writes,
            max_write_bytes,
            write_start_range_index,
        )?;
        if !self.replica_writable(replica_index) {
            admission.can_accept = false;
            admission.service_mode_limited = true;
        }
        Ok(admission)
    }

    pub fn can_accept_chunk_replica_batch_write_fenced(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        let mut borrowed = Vec::with_capacity(writes.len());
        for (offset, data) in &writes {
            borrowed.push((*offset, data.as_slice()));
        }
        self.can_accept_chunk_replica_batch_write_from_borrowed(
            replica_index,
            chunk_id,
            &borrowed,
            max_write_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_write_fenced_borrowed(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        writes: &[(u64, &[u8])],
        max_write_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ChunkWriteAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_write_from_borrowed(
            replica_index,
            chunk_id,
            writes,
            max_write_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        self.can_accept_chunk_replica_batch_discard_from(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard_from(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        self.can_accept_chunk_replica_batch_discard_from_borrowed(
            replica_index,
            chunk_id,
            &ranges,
            max_discard_bytes,
            discard_start_range_index,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard_from_borrowed(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: &[DataRange],
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        let mut admission = replica.can_accept_chunk_discards_from_borrowed(
            chunk_id,
            ranges,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        if !self.replica_writable(replica_index) {
            admission.can_accept = false;
            admission.service_mode_limited = true;
        }
        Ok(admission)
    }

    pub fn can_accept_chunk_replica_batch_discard_fenced(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_discard(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard_fenced_borrowed(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: &[DataRange],
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ChunkDiscardAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_discard_from_borrowed(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_read(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
    ) -> Result<ChunkReadAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        let mut admission = replica.can_accept_chunk_reads(chunk_id, ranges, max_payload_bytes)?;
        if !self.replica_active(replica_index) {
            admission.can_accept = false;
            admission.partial_status = "chunk replica is not active".to_string();
        }
        Ok(admission)
    }

    pub fn can_accept_chunk_replica_batch_read_fenced(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ChunkReadAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_read(replica_index, chunk_id, ranges, max_payload_bytes)
    }

    pub fn configure_chunk_replica_receipt_retention(
        &mut self,
        replica_index: usize,
        retention: usize,
    ) -> Result<u64, ObjectError> {
        self.replicas
            .get_mut(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .configure_receipt_retention(retention)
    }

    pub fn configure_chunk_receipt_retention(
        &mut self,
        retention: usize,
    ) -> Result<(u64, u64), ObjectError> {
        validate_storage_receipt_retention(retention)?;
        let mut trimmed = 0_u64;
        let mut configured = 0_u64;
        for replica in &mut self.replicas {
            trimmed = trimmed.saturating_add(replica.configure_receipt_retention(retention)?);
            configured = configured.saturating_add(1);
        }
        Ok((trimmed, configured))
    }
}
