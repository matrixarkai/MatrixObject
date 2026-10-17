impl MatrixPageService {
    pub fn write_allocated_range(
        &mut self,
        allocation_id: &str,
        data: &[u8],
    ) -> Result<PageAllocation, ObjectError> {
        let allocation = self
            .pending_allocations
            .get(allocation_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("block allocation missing".to_string()))?;
        if data.len() as u64 != allocation.length {
            return Err(ObjectError::InvalidArgument(
                "block allocation write length mismatch".to_string(),
            ));
        }
        self.write_slab_range(&allocation.slab_id, allocation.offset, data)?;
        self.pending_allocations.remove(allocation_id);
        Ok(allocation)
    }

    pub fn map_range(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
    ) -> Result<Vec<PageLocation>, ObjectError> {
        validate_pipeline_range("block range", offset, length)?;
        let capacity = usize::try_from(self.mapped_chunk_count(offset, length)?).map_err(|_| {
            ObjectError::InvalidArgument("block range maps too many chunks".to_string())
        })?;
        let mut out = Vec::with_capacity(capacity);
        let mut cursor = 0u64;
        while cursor < length {
            let absolute = offset + cursor;
            let chunk_index = absolute / self.chunk_size;
            let chunk_offset = absolute % self.chunk_size;
            let available = self.chunk_size - chunk_offset;
            let n = available.min(length - cursor);
            out.push(PageLocation {
                chunk_id: ChunkId {
                    slab_id: slab_id.clone(),
                    chunk_index,
                },
                chunk_offset,
                length: n,
            });
            cursor += n;
        }
        Ok(out)
    }

    pub(crate) fn mapped_chunk_count(&self, offset: u64, length: u64) -> Result<u64, ObjectError> {
        validate_pipeline_range("block range", offset, length)?;
        if length == 0 {
            return Ok(0);
        }
        let first_chunk = offset / self.chunk_size;
        let last_chunk = (offset + length - 1) / self.chunk_size;
        Ok(last_chunk - first_chunk + 1)
    }

}
