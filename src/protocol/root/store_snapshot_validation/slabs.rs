impl StoreSnapshot {
    fn validate_slabs(&self) -> Result<(), ObjectError> {
        let mut max_slab_id = 0u64;
        for (slab_id, slab) in &self.slabs {
            if slab_id != &slab.slab_id {
                return Err(ObjectError::Corruption(
                    "snapshot segment key does not match segment info".to_string(),
                ));
            }
            max_slab_id = max_slab_id.max(slab_id.id);
            if slab.next_offset > self.options.slab_size
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
        if max_slab_id >= self.next_slab_id {
            return Err(ObjectError::Corruption(
                "snapshot next segment id is behind retained segments".to_string(),
            ));
        }
        Ok(())
    }
}
