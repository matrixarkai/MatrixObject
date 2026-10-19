impl StoreSnapshot {
    fn validate_chunks(&self) -> Result<(), ObjectError> {
        let mut max_chunk_id = 0u64;
        for (chunk_id, slab) in &self.chunks {
            if chunk_id != &slab.chunk_id {
                return Err(ObjectError::Corruption(
                    "snapshot segment key does not match segment info".to_string(),
                ));
            }
            max_chunk_id = max_chunk_id.max(chunk_id.id);
            if slab.next_offset > self.options.chunk_size
                || slab.live_bytes.saturating_add(slab.stale_bytes) < slab.live_bytes
                || slab.discarded_bytes > slab.stale_bytes
            {
                return Err(ObjectError::Corruption(
                    "snapshot segment accounting is invalid".to_string(),
                ));
            }
            for range in slab
                .stale_ranges
                .iter()
                .chain(slab.discarded_ranges.iter())
            {
                if range.end()? > slab.next_offset {
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
