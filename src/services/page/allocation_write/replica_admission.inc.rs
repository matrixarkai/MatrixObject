// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn can_accept_chunk_replica_batch_write(
        &self,
        replica_index: usize,
        page_id: &PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        self.can_accept_chunk_replica_batch_write_from(
            replica_index,
            page_id,
            writes,
            max_write_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_write_from(
        &self,
        replica_index: usize,
        page_id: &PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        let mut borrowed = Vec::with_capacity(writes.len());
        for (offset, data) in &writes {
            borrowed.push((*offset, data.as_slice()));
        }
        self.can_accept_chunk_replica_batch_write_from_borrowed(
            replica_index,
            page_id,
            &borrowed,
            max_write_bytes,
            write_start_range_index,
        )
    }

    pub fn can_accept_chunk_replica_batch_write_from_borrowed(
        &self,
        replica_index: usize,
        page_id: &PageId,
        writes: &[(u64, &[u8])],
        max_write_bytes: u64,
        write_start_range_index: u64,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        let mut admission = replica.can_accept_chunk_writes_from_borrowed(
            page_id,
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
        page_id: &PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
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
            page_id,
            &borrowed,
            max_write_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_write_fenced_borrowed(
        &self,
        replica_index: usize,
        page_id: &PageId,
        writes: &[(u64, &[u8])],
        max_write_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaWriteAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_write_from_borrowed(
            replica_index,
            page_id,
            writes,
            max_write_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.can_accept_chunk_replica_batch_discard_from(
            replica_index,
            page_id,
            ranges,
            max_discard_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard_from(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        self.can_accept_chunk_replica_batch_discard_from_borrowed(
            replica_index,
            page_id,
            &ranges,
            max_discard_bytes,
            discard_start_range_index,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard_from_borrowed(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: &[DataRange],
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        let mut admission = replica.can_accept_chunk_discards_from_borrowed(
            page_id,
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
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_discard(
            replica_index,
            page_id,
            ranges,
            max_discard_bytes,
        )
    }

    pub fn can_accept_chunk_replica_batch_discard_fenced_borrowed(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: &[DataRange],
        max_discard_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaDiscardAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_discard_from_borrowed(
            replica_index,
            page_id,
            ranges,
            max_discard_bytes,
            0,
        )
    }

    pub fn can_accept_chunk_replica_batch_read(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
    ) -> Result<ReplicaReadAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        let mut admission = replica.can_accept_chunk_reads(page_id, ranges, max_payload_bytes)?;
        if !self.replica_active(replica_index) {
            admission.can_accept = false;
            admission.partial_status = "chunk replica is not active".to_string();
        }
        Ok(admission)
    }

    pub fn can_accept_chunk_replica_batch_read_fenced(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<ReplicaReadAdmission, ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        self.can_accept_chunk_replica_batch_read(replica_index, page_id, ranges, max_payload_bytes)
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
