impl MatrixObjectStore {
    pub fn read_direct_page_range(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        self.read_direct_page_range_bounded(chunk_id, offset, length, 0)
    }

    pub fn read_direct_page_range_bounded(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        self.chunk_service
            .read_chunk_range_bounded(chunk_id, offset, length, max_payload_bytes)
    }

    pub fn read_direct_page_range_bounded_by_chunks(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        self.chunk_service.read_chunk_range_bounded_by_chunks(
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn read_direct_page_ranges(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
    ) -> Result<(Vec<ChunkRangeReadResult>, ChunkBatchStats), ObjectError> {
        self.chunk_service
            .read_chunk_ranges_bounded(requests, max_payload_bytes)
    }

    pub fn write_page_range_idempotent(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
    ) -> Result<ChunkWriteReceipt, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let receipt = self.chunk_service.write_chunk_range_idempotent(
            chunk_id,
            offset,
            data,
            idempotency_key,
        )?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_page_write_journal_record(chunk_id, offset, data, idempotency_key)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(receipt)
    }

    pub fn write_page_ranges_idempotent(
        &mut self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
    ) -> Result<(ChunkBatchStats, ChunkBatchWriteReceipt), ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let (stats, receipt) = self
            .chunk_service
            .write_slab_ranges_idempotent_borrowed(&writes, idempotency_key)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_page_batch_write_journal_record(&writes, idempotency_key) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok((stats, receipt))
    }

    pub fn discard_page_range_idempotent(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<ChunkDiscardReceipt, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let receipt = self.chunk_service.discard_chunk_range_idempotent(
            chunk_id,
            offset,
            length,
            idempotency_key,
        )?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_page_discard_journal_record(chunk_id, offset, length, idempotency_key)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(receipt)
    }

    pub fn discard_page_ranges_idempotent(
        &mut self,
        requests: Vec<ChunkRangeRequest>,
        idempotency_key: &str,
    ) -> Result<(ChunkBatchStats, ChunkBatchDiscardReceipt), ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let (stats, receipt) = self
            .chunk_service
            .discard_slab_ranges_idempotent_borrowed(&requests, idempotency_key)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_page_batch_discard_journal_record(&requests, idempotency_key)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok((stats, receipt))
    }

}
