impl MatrixChunkService {
    pub fn configure_allocator(&mut self, chunk_size: u64) -> Result<(), ObjectError> {
        if chunk_size == 0 || chunk_size < self.page_size {
            return Err(ObjectError::InvalidArgument(
                "chunk allocator chunk_size must be >= page_size".to_string(),
            ));
        }
        if !self.pending_allocations.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block allocator cannot be reconfigured with pending allocations".to_string(),
            ));
        }
        self.allocator_chunk_size = chunk_size;
        if self.allocator_next_offset >= self.allocator_chunk_size {
            self.allocator_next_chunk_id = self.allocator_next_chunk_id.saturating_add(1);
            self.allocator_next_offset = 0;
        }
        Ok(())
    }

    pub fn seal_chunk(&mut self, chunk_id: ChunkId) -> Result<bool, ObjectError> {
        self.validate_page_chunk_id(&chunk_id)?;
        if self.pending_allocations.values().any(|allocation| {
            allocation.chunk_id == chunk_id && allocation.offset < self.allocator_chunk_size
        }) {
            return Err(ObjectError::PreconditionFailed(
                "block segment has pending allocations".to_string(),
            ));
        }
        let inserted = self.sealed_chunks.insert(chunk_id.clone());
        if self.allocator_next_chunk_id == chunk_id.id
            && self.allocator_next_offset < self.allocator_chunk_size
        {
            self.roll_allocator_to_next_unsealed_chunk()?;
        }
        Ok(inserted)
    }

    pub fn sealed_chunk_count(&self) -> u64 {
        self.sealed_chunks.len() as u64
    }

    pub fn is_chunk_sealed(&self, chunk_id: &ChunkId) -> bool {
        self.sealed_chunks.contains(chunk_id)
    }

    pub fn allocator_active_chunk_id(&self) -> ChunkId {
        ChunkId {
            volume_id: 0,
            id: self.allocator_next_chunk_id,
        }
    }

    pub fn allocator_active_chunk_sealed(&self) -> bool {
        self.is_chunk_sealed(&self.allocator_active_chunk_id())
    }

    pub fn list_sealed_chunks(
        &self,
        marker: Option<&ChunkId>,
        limit: usize,
    ) -> Result<SealedChunkPage, ObjectError> {
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "sealed segment list limit must be positive".to_string(),
            ));
        }
        let mut chunks = Vec::new();
        let mut truncated = false;
        let mut next_marker = None;
        for slab in &self.sealed_chunks {
            if marker.map(|item| slab <= item).unwrap_or(false) {
                continue;
            }
            if chunks.len() >= limit {
                truncated = true;
                break;
            }
            next_marker = Some(slab.clone());
            chunks.push(slab.clone());
        }
        Ok(SealedChunkPage {
            chunks,
            sealed_chunk_count: self.sealed_chunks.len() as u64,
            limit: limit as u64,
            truncated,
            next_marker: if truncated { next_marker } else { None },
        })
    }

    fn validate_page_chunk_id(&self, chunk_id: &ChunkId) -> Result<(), ObjectError> {
        if chunk_id.volume_id != 0 {
            return Err(ObjectError::InvalidArgument(
                "block segment volume_id must be zero".to_string(),
            ));
        }
        if chunk_id.id == 0 {
            return Err(ObjectError::InvalidArgument(
                "block segment id must be positive".to_string(),
            ));
        }
        Ok(())
    }

    fn roll_allocator_to_next_unsealed_chunk(&mut self) -> Result<(), ObjectError> {
        let mut next_id = self.allocator_next_chunk_id;
        loop {
            next_id = next_id.checked_add(1).ok_or_else(|| {
                ObjectError::InvalidArgument("block allocator segment id overflow".to_string())
            })?;
            let candidate = ChunkId {
                volume_id: 0,
                id: next_id,
            };
            if !self.sealed_chunks.contains(&candidate) {
                self.allocator_next_chunk_id = next_id;
                self.allocator_next_offset = 0;
                return Ok(());
            }
        }
    }

    pub fn allocate_page_range(
        &mut self,
        length: u64,
        now_ms: u64,
    ) -> Result<ChunkAllocation, ObjectError> {
        if length == 0 {
            return Err(ObjectError::InvalidArgument(
                "block allocation length must be positive".to_string(),
            ));
        }
        if length > self.allocator_chunk_size {
            return Err(ObjectError::InvalidArgument(
                "block allocation length exceeds allocator segment size".to_string(),
            ));
        }
        if self.allocator_active_chunk_sealed() {
            self.roll_allocator_to_next_unsealed_chunk()?;
        }
        let next_end = self
            .allocator_next_offset
            .checked_add(length)
            .ok_or_else(|| ObjectError::InvalidArgument("block allocation overflow".to_string()))?;
        if next_end > self.allocator_chunk_size {
            self.allocator_next_chunk_id = self
                .allocator_next_chunk_id
                .checked_add(1)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("block allocator segment id overflow".to_string())
                })?;
            self.allocator_next_offset = 0;
            if self.allocator_active_chunk_sealed() {
                self.roll_allocator_to_next_unsealed_chunk()?;
            }
        }
        let chunk_id = ChunkId {
            volume_id: 0,
            id: self.allocator_next_chunk_id,
        };
        let offset = self.allocator_next_offset;
        self.allocator_next_offset = self
            .allocator_next_offset
            .checked_add(length)
            .ok_or_else(|| ObjectError::InvalidArgument("block allocation overflow".to_string()))?;
        let allocation_id = format!(
            "{}:{}:{}:{}",
            chunk_id.volume_id, chunk_id.id, offset, length
        );
        if self.pending_allocations.contains_key(&allocation_id) {
            return Err(ObjectError::Corruption(
                "block allocation id collision".to_string(),
            ));
        }
        let allocation = ChunkAllocation {
            allocation_id: allocation_id.clone(),
            chunk_id,
            offset,
            length,
            create_time_ms: now_ms,
        };
        self.pending_allocations
            .insert(allocation_id, allocation.clone());
        Ok(allocation)
    }

    pub fn allocate_page_range_idempotent(
        &mut self,
        length: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ChunkAllocationReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if let Some(existing) = self.allocation_request_receipts.get(idempotency_key) {
            if existing.length != length {
                return Err(ObjectError::PreconditionFailed(
                    "block allocation idempotency key length mismatch".to_string(),
                ));
            }
            return Ok(ChunkAllocationReceipt {
                allocation: existing.clone(),
                idempotency_key: idempotency_key.to_string(),
                released: false,
                deduped: true,
            });
        }
        let allocation = self.allocate_page_range(length, now_ms)?;
        self.allocation_request_receipts
            .insert(idempotency_key.to_string(), allocation.clone());
        self.trim_receipts_to_retention();
        Ok(ChunkAllocationReceipt {
            allocation,
            idempotency_key: idempotency_key.to_string(),
            released: false,
            deduped: false,
        })
    }

    pub fn allocate_page_ranges_idempotent(
        &mut self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ChunkBatchAllocationReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        validate_page_allocation_batch_lengths(&lengths, self.allocator_chunk_size)?;
        if let Some(existing) = self.batch_allocation_request_receipts.get(idempotency_key) {
            if existing.len() != lengths.len()
                || existing
                    .iter()
                    .zip(&lengths)
                    .any(|(allocation, length)| allocation.length != *length)
            {
                return Err(ObjectError::PreconditionFailed(
                    "block batch allocation idempotency key image mismatch".to_string(),
                ));
            }
            return Ok(ChunkBatchAllocationReceipt {
                allocations: existing.clone(),
                idempotency_key: idempotency_key.to_string(),
                deduped: true,
            });
        }
        let mut allocations = Vec::with_capacity(lengths.len());
        for length in lengths {
            allocations.push(self.allocate_page_range(length, now_ms)?);
        }
        self.batch_allocation_request_receipts
            .insert(idempotency_key.to_string(), allocations.clone());
        self.trim_receipts_to_retention();
        Ok(ChunkBatchAllocationReceipt {
            allocations,
            idempotency_key: idempotency_key.to_string(),
            deduped: false,
        })
    }

    pub fn pending_page_allocations(&self) -> Vec<ChunkAllocation> {
        self.pending_allocations.values().cloned().collect()
    }

    pub fn released_page_allocation_count(&self) -> u64 {
        self.released_allocation_count
    }
}
