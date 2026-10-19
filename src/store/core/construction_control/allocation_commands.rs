// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn release_page_allocation_idempotent(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<ChunkAllocationReceipt, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let receipt = self
            .chunk_service
            .release_page_allocation_idempotent(allocation_id, idempotency_key)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_page_allocation_release_journal_record(allocation_id, idempotency_key)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(receipt)
    }

    pub fn allocate_page_ranges_idempotent(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ChunkBatchAllocationReceipt, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let receipt = self.chunk_service.allocate_page_ranges_idempotent(
            lengths.clone(),
            now_ms,
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
            self.append_page_batch_allocation_journal_record(&lengths, now_ms, idempotency_key)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(receipt)
    }

}
