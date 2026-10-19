impl MatrixChunkService {
    pub fn configure_allocator(&mut self, slab_size: u64) -> Result<(), ObjectError> {
        if slab_size == 0 || slab_size < self.page_size {
            return Err(ObjectError::InvalidArgument(
                "block allocator segment_size must be >= chunk_size".to_string(),
            ));
        }
        if !self.pending_allocations.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block allocator cannot be reconfigured with pending allocations".to_string(),
            ));
        }
        self.allocator_slab_size = slab_size;
        if self.allocator_next_offset >= self.allocator_slab_size {
            self.allocator_next_slab_id = self.allocator_next_slab_id.saturating_add(1);
            self.allocator_next_offset = 0;
        }
        Ok(())
    }

    pub fn seal_slab(&mut self, slab_id: SlabId) -> Result<bool, ObjectError> {
        self.validate_page_slab_id(&slab_id)?;
        if self.pending_allocations.values().any(|allocation| {
            allocation.slab_id == slab_id && allocation.offset < self.allocator_slab_size
        }) {
            return Err(ObjectError::PreconditionFailed(
                "block segment has pending allocations".to_string(),
            ));
        }
        let inserted = self.sealed_slabs.insert(slab_id.clone());
        if self.allocator_next_slab_id == slab_id.id
            && self.allocator_next_offset < self.allocator_slab_size
        {
            self.roll_allocator_to_next_unsealed_slab()?;
        }
        Ok(inserted)
    }

    pub fn sealed_slab_count(&self) -> u64 {
        self.sealed_slabs.len() as u64
    }

    pub fn is_slab_sealed(&self, slab_id: &SlabId) -> bool {
        self.sealed_slabs.contains(slab_id)
    }

    pub fn allocator_active_slab_id(&self) -> SlabId {
        SlabId {
            volume_id: 0,
            id: self.allocator_next_slab_id,
        }
    }

    pub fn allocator_active_slab_sealed(&self) -> bool {
        self.is_slab_sealed(&self.allocator_active_slab_id())
    }

    pub fn list_sealed_slabs(
        &self,
        marker: Option<&SlabId>,
        limit: usize,
    ) -> Result<SealedSlabPage, ObjectError> {
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "sealed segment list limit must be positive".to_string(),
            ));
        }
        let mut slabs = Vec::new();
        let mut truncated = false;
        let mut next_marker = None;
        for slab in &self.sealed_slabs {
            if marker.map(|item| slab <= item).unwrap_or(false) {
                continue;
            }
            if slabs.len() >= limit {
                truncated = true;
                break;
            }
            next_marker = Some(slab.clone());
            slabs.push(slab.clone());
        }
        Ok(SealedSlabPage {
            slabs,
            sealed_slab_count: self.sealed_slabs.len() as u64,
            limit: limit as u64,
            truncated,
            next_marker: if truncated { next_marker } else { None },
        })
    }

    fn validate_page_slab_id(&self, slab_id: &SlabId) -> Result<(), ObjectError> {
        if slab_id.volume_id != 0 {
            return Err(ObjectError::InvalidArgument(
                "block segment volume_id must be zero".to_string(),
            ));
        }
        if slab_id.id == 0 {
            return Err(ObjectError::InvalidArgument(
                "block segment id must be positive".to_string(),
            ));
        }
        Ok(())
    }

    fn roll_allocator_to_next_unsealed_slab(&mut self) -> Result<(), ObjectError> {
        let mut next_id = self.allocator_next_slab_id;
        loop {
            next_id = next_id.checked_add(1).ok_or_else(|| {
                ObjectError::InvalidArgument("block allocator segment id overflow".to_string())
            })?;
            let candidate = SlabId {
                volume_id: 0,
                id: next_id,
            };
            if !self.sealed_slabs.contains(&candidate) {
                self.allocator_next_slab_id = next_id;
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
        if length > self.allocator_slab_size {
            return Err(ObjectError::InvalidArgument(
                "block allocation length exceeds allocator segment size".to_string(),
            ));
        }
        if self.allocator_active_slab_sealed() {
            self.roll_allocator_to_next_unsealed_slab()?;
        }
        let next_end = self
            .allocator_next_offset
            .checked_add(length)
            .ok_or_else(|| ObjectError::InvalidArgument("block allocation overflow".to_string()))?;
        if next_end > self.allocator_slab_size {
            self.allocator_next_slab_id = self
                .allocator_next_slab_id
                .checked_add(1)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("block allocator segment id overflow".to_string())
                })?;
            self.allocator_next_offset = 0;
            if self.allocator_active_slab_sealed() {
                self.roll_allocator_to_next_unsealed_slab()?;
            }
        }
        let slab_id = SlabId {
            volume_id: 0,
            id: self.allocator_next_slab_id,
        };
        let offset = self.allocator_next_offset;
        self.allocator_next_offset = self
            .allocator_next_offset
            .checked_add(length)
            .ok_or_else(|| ObjectError::InvalidArgument("block allocation overflow".to_string()))?;
        let allocation_id = format!(
            "{}:{}:{}:{}",
            slab_id.volume_id, slab_id.id, offset, length
        );
        if self.pending_allocations.contains_key(&allocation_id) {
            return Err(ObjectError::Corruption(
                "block allocation id collision".to_string(),
            ));
        }
        let allocation = ChunkAllocation {
            allocation_id: allocation_id.clone(),
            slab_id,
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
        validate_page_allocation_batch_lengths(&lengths, self.allocator_slab_size)?;
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
