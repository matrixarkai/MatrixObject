impl StoreSnapshot {
    fn validate_chunks(&self) -> Result<(), ObjectError> {
        let mut max_chunk_id = 0u64;
        for (chunk_id, chunk) in &self.chunks {
            if chunk_id != &chunk.chunk_id {
                return Err(ObjectError::Corruption(
                    "snapshot segment key does not match segment info".to_string(),
                ));
            }
            max_chunk_id = max_chunk_id.max(chunk_id.id);
            if chunk.next_offset > self.options.chunk_size
                || chunk.live_bytes.saturating_add(chunk.stale_bytes) < chunk.live_bytes
                || chunk.discarded_bytes > chunk.stale_bytes
            {
                return Err(ObjectError::Corruption(
                    "snapshot segment accounting is invalid".to_string(),
                ));
            }
            for range in chunk
                .stale_ranges
                .iter()
                .chain(chunk.discarded_ranges.iter())
            {
                if range.end()? > chunk.next_offset {
                    return Err(ObjectError::Corruption(
                        "snapshot segment range exceeds segment length".to_string(),
                    ));
                }
            }
        }
        if max_chunk_id >= self.next_chunk_id {
            return Err(ObjectError::Corruption(
                "snapshot next segment id is behind retained segments".to_string(),
            ));
        }
        Ok(())
    }
}
