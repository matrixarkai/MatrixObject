impl MatrixChunkService {
    pub fn release_page_allocation(
        &mut self,
        allocation_id: &str,
    ) -> Result<ChunkAllocation, ObjectError> {
        let allocation = self
            .pending_allocations
            .remove(allocation_id)
            .ok_or_else(|| ObjectError::NotFound("page allocation missing".to_string()))?;
        self.released_allocation_count = self.released_allocation_count.saturating_add(1);
        Ok(allocation)
    }

    pub fn release_page_allocation_idempotent(
        &mut self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<ChunkAllocationReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        validate_idempotency_key(allocation_id)?;
        if let Some(existing) = self
            .release_allocation_request_receipts
            .get(idempotency_key)
        {
            if existing.allocation_id != allocation_id {
                return Err(ObjectError::PreconditionFailed(
                    "page allocation release idempotency key allocation mismatch".to_string(),
                ));
            }
            return Ok(ChunkAllocationReceipt {
                allocation: existing.clone(),
                idempotency_key: idempotency_key.to_string(),
                released: true,
                deduped: true,
            });
        }
        let allocation = self.release_page_allocation(allocation_id)?;
        self.release_allocation_request_receipts
            .insert(idempotency_key.to_string(), allocation.clone());
        self.trim_receipts_to_retention();
        Ok(ChunkAllocationReceipt {
            allocation,
            idempotency_key: idempotency_key.to_string(),
            released: true,
            deduped: false,
        })
    }
}
