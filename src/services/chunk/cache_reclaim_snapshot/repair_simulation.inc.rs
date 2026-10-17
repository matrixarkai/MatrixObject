impl MatrixChunkService {
    pub fn remove_chunk_for_repair(&mut self, chunk_id: &ChunkId) -> bool {
        self.invalidate_chunk_cache(chunk_id, 0, u64::MAX);
        self.chunks.remove(chunk_id).is_some()
    }

    pub fn corrupt_chunk_payload_for_repair(
        &mut self,
        chunk_id: &ChunkId,
    ) -> Result<(), ObjectError> {
        let chunk = self
            .chunks
            .get_mut(chunk_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        if chunk.data.is_empty() {
            std::sync::Arc::make_mut(&mut chunk.data).push(0xff);
        } else {
            std::sync::Arc::make_mut(&mut chunk.data)[0] ^= 0xff;
        }
        self.invalidate_chunk_cache(chunk_id, 0, u64::MAX);
        Ok(())
    }

    #[deprecated(note = "use remove_chunk_for_repair")]
    pub fn remove_chunk_for_repair_simulation(&mut self, chunk_id: &ChunkId) -> bool {
        self.remove_chunk_for_repair(chunk_id)
    }

    #[deprecated(note = "use corrupt_chunk_payload_for_repair")]
    pub fn corrupt_chunk_payload_for_repair_simulation(
        &mut self,
        chunk_id: &ChunkId,
    ) -> Result<(), ObjectError> {
        self.corrupt_chunk_payload_for_repair(chunk_id)
    }
}
