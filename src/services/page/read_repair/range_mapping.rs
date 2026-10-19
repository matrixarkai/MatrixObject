// SPDX-License-Identifier: Apache-2.0
impl MatrixChunkService {
    pub fn write_allocated_range(
        &mut self,
        allocation_id: &str,
        data: &[u8],
    ) -> Result<ChunkAllocation, ObjectError> {
        let allocation = self
            .pending_allocations
            .get(allocation_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("page allocation missing".to_string()))?;
        if data.len() as u64 != allocation.length {
            return Err(ObjectError::InvalidArgument(
                "page allocation write length mismatch".to_string(),
            ));
        }
        self.write_chunk_range(&allocation.chunk_id, allocation.offset, data)?;
        self.pending_allocations.remove(allocation_id);
        Ok(allocation)
    }

    pub fn map_range(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ChunkLocation>, ObjectError> {
        validate_pipeline_range("block range", offset, length)?;
        let capacity = usize::try_from(self.mapped_chunk_count(offset, length)?).map_err(|_| {
            ObjectError::InvalidArgument("page range maps too many chunks".to_string())
        })?;
        let mut out = Vec::with_capacity(capacity);
        let mut cursor = 0u64;
        while cursor < length {
            let absolute = offset + cursor;
            let page_index = absolute / self.page_size;
            let page_offset = absolute % self.page_size;
            let available = self.page_size - page_offset;
            let n = available.min(length - cursor);
            out.push(ChunkLocation {
                page_id: PageId {
                    chunk_id: chunk_id.clone(),
                    page_index,
                },
                page_offset,
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
        let first_chunk = offset / self.page_size;
        let last_chunk = (offset + length - 1) / self.page_size;
        Ok(last_chunk - first_chunk + 1)
    }

}
