impl MatrixObjectStore {
    pub fn list_chunk_write_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaWriteReceiptPage, ObjectError> {
        self.chunk_service
            .list_chunk_write_receipts(replica_index, marker, limit, page_id)
    }

    pub fn list_chunk_batch_write_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaBatchWriteReceiptPage, ObjectError> {
        self.chunk_service
            .list_chunk_batch_write_receipts(replica_index, marker, limit, page_id)
    }

    pub fn list_chunk_discard_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaDiscardReceiptPage, ObjectError> {
        self.chunk_service
            .list_chunk_discard_receipts(replica_index, marker, limit, page_id)
    }

    pub fn list_chunk_batch_discard_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaBatchDiscardReceiptPage, ObjectError> {
        self.chunk_service
            .list_chunk_batch_discard_receipts(replica_index, marker, limit, page_id)
    }

    pub fn list_chunk_repair_receipts(
        &self,
        replica_index: usize,
        marker: Option<&str>,
        limit: usize,
        page_id: Option<&PageId>,
    ) -> Result<ReplicaRepairReceiptPage, ObjectError> {
        self.chunk_service
            .list_chunk_repair_receipts(replica_index, marker, limit, page_id)
    }

    pub fn read_direct_chunk_range(
        &self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<ReplicaReadResult, ObjectError> {
        self.read_direct_chunk_range_bounded(replica_index, page_id, offset, length, 0)
    }

    pub fn read_direct_chunk_range_bounded(
        &self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ReplicaReadResult, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.read_chunk_bounded(page_id, offset, length, max_payload_bytes)
    }

    pub fn read_direct_chunk_ranges(
        &self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        max_payload_bytes: u64,
    ) -> Result<(Vec<ReplicaRangeReadResult>, ReplicaReadBatchStats), ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.read_chunk_ranges(page_id, ranges, max_payload_bytes)
    }

    pub fn head_direct_chunk(
        &self,
        replica_index: usize,
        page_id: &PageId,
    ) -> Result<ReplicaHead, ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.head_chunk(page_id)
    }

    pub fn head_direct_chunks(
        &self,
        replica_index: usize,
        page_ids: Vec<PageId>,
    ) -> Result<(Vec<ReplicaHead>, ReplicaHeadBatchStats), ObjectError> {
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.head_chunk_batch(page_ids)
    }

    pub fn remove_direct_replica_chunk_for_repair(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .remove_replica_chunk_for_repair(replica_index, page_id)
    }

    pub fn write_direct_chunk_range_idempotent(
        &mut self,
        replica_index: usize,
        page_id: PageId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
    ) -> Result<ReplicaWriteReceipt, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let receipt = {
            let replica = self
                .chunk_service
                .replicas
                .get_mut(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
            replica.write_chunk_idempotent(page_id.clone(), offset, data, idempotency_key)?
        };
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunk_write_journal_record(
            replica_index,
            &page_id,
            offset,
            data,
            idempotency_key,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(receipt)
    }

    pub fn write_direct_chunk_ranges_idempotent(
        &mut self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        idempotency_key: &str,
    ) -> Result<(ReplicaWriteBatchStats, ReplicaBatchWriteReceipt), ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let borrowed_writes = writes
            .iter()
            .map(|(offset, data)| (*offset, data.as_slice()))
            .collect::<Vec<_>>();
        let (stats, receipt) = {
            let replica = self
                .chunk_service
                .replicas
                .get_mut(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
            replica.write_chunk_ranges_idempotent_borrowed(
                page_id.clone(),
                &borrowed_writes,
                idempotency_key,
            )?
        };
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunk_batch_write_journal_record(
            replica_index,
            &page_id,
            &writes,
            idempotency_key,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok((stats, receipt))
    }

    pub fn discard_direct_chunk_range_idempotent(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<ReplicaDiscardReceipt, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let receipt = {
            let replica = self
                .chunk_service
                .replicas
                .get_mut(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
            replica.discard_chunk_idempotent(page_id, offset, length, idempotency_key)?
        };
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunk_discard_journal_record(
            replica_index,
            page_id,
            offset,
            length,
            idempotency_key,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(receipt)
    }

    pub fn discard_direct_chunk_ranges_idempotent(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
        ranges: Vec<DataRange>,
        idempotency_key: &str,
    ) -> Result<(ReplicaDiscardBatchStats, ReplicaBatchDiscardReceipt), ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let (stats, receipt) = {
            let replica = self
                .chunk_service
                .replicas
                .get_mut(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
            replica.discard_chunk_ranges_idempotent_borrowed(page_id, &ranges, idempotency_key)?
        };
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_chunk_batch_discard_journal_record(
            replica_index,
            page_id,
            &ranges,
            idempotency_key,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok((stats, receipt))
    }

}
