impl MatrixReplicaService {
    pub fn remove_chunk_for_repair(&mut self, page_id: &PageId) -> bool {
        self.invalidate_chunk_cache(page_id, 0, u64::MAX);
        self.chunks.remove(page_id).is_some()
    }

    pub fn corrupt_chunk_payload_for_repair(
        &mut self,
        page_id: &PageId,
    ) -> Result<(), ObjectError> {
        let chunk = self
            .chunks
            .get_mut(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        let chunk = std::sync::Arc::make_mut(chunk);
        if chunk.data.is_empty() {
            std::sync::Arc::make_mut(&mut chunk.data).push(0xff);
        } else {
            std::sync::Arc::make_mut(&mut chunk.data)[0] ^= 0xff;
        }
        self.invalidate_chunk_cache(page_id, 0, u64::MAX);
        Ok(())
    }

    #[deprecated(note = "use remove_chunk_for_repair")]
    pub fn remove_chunk_for_repair_simulation(&mut self, page_id: &PageId) -> bool {
        self.remove_chunk_for_repair(page_id)
    }

    #[deprecated(note = "use corrupt_chunk_payload_for_repair")]
    pub fn corrupt_chunk_payload_for_repair_simulation(
        &mut self,
        page_id: &PageId,
    ) -> Result<(), ObjectError> {
        self.corrupt_chunk_payload_for_repair(page_id)
    }
}
