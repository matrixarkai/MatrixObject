impl MatrixChunkService {
    pub fn read_chunk_state(&self, chunk_id: &ChunkId) -> Result<ChunkState, ObjectError> {
        self.chunks
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))
    }

    pub fn head_chunk(&self, chunk_id: &ChunkId) -> Result<ChunkHead, ObjectError> {
        let state = self
            .chunks
            .get(chunk_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        Ok(ChunkHead {
            logical_length: state.logical_length,
            write_sequence: state.write_sequence,
            checksum: state.checksum.clone(),
            state_checksum: chunk_state_handoff_checksum(chunk_id, state, &state.checksum),
            discarded_ranges: state.discarded_ranges.clone(),
        })
    }

    pub fn head_chunk_batch(
        &self,
        chunk_ids: Vec<ChunkId>,
    ) -> Result<(Vec<ChunkHead>, ChunkHeadBatchStats), ObjectError> {
        self.head_chunk_batch_refs(&chunk_ids)
    }

    pub(crate) fn head_chunk_batch_refs(
        &self,
        chunk_ids: &[ChunkId],
    ) -> Result<(Vec<ChunkHead>, ChunkHeadBatchStats), ObjectError> {
        if chunk_ids.len() <= 1 {
            return self.head_unique_chunk_batch_refs(chunk_ids);
        }
        let mut stats = ChunkHeadBatchStats {
            input_heads: chunk_ids.len() as u64,
            ..ChunkHeadBatchStats::default()
        };
        let mut cached_heads: BTreeMap<&ChunkId, (usize, Option<ChunkHead>)> = BTreeMap::new();
        for chunk_id in chunk_ids {
            cached_heads
                .entry(chunk_id)
                .and_modify(|(remaining, _)| *remaining = remaining.saturating_add(1))
                .or_insert((1, None));
        }
        let mut heads = Vec::with_capacity(chunk_ids.len());
        for chunk_id in chunk_ids {
            let Some((remaining, cached_head)) = cached_heads.get_mut(chunk_id) else {
                return Err(ObjectError::Corruption(
                    "chunk head batch lost duplicate accounting".to_string(),
                ));
            };
            *remaining = remaining.saturating_sub(1);
            if let Some(head) = cached_head {
                stats.coalesced_heads = stats.coalesced_heads.saturating_add(1);
                stats.metadata_cache_hits = stats.metadata_cache_hits.saturating_add(1);
                heads.push(head.clone());
                continue;
            }
            let head = self.head_chunk(&chunk_id)?;
            stats.physical_heads = stats.physical_heads.saturating_add(1);
            stats.metadata_cache_misses = stats.metadata_cache_misses.saturating_add(1);
            if *remaining > 0 {
                *cached_head = Some(head.clone());
            }
            heads.push(head);
        }
        Ok((heads, stats))
    }

    pub(crate) fn head_unique_chunk_batch_refs(
        &self,
        chunk_ids: &[ChunkId],
    ) -> Result<(Vec<ChunkHead>, ChunkHeadBatchStats), ObjectError> {
        let mut stats = ChunkHeadBatchStats {
            input_heads: chunk_ids.len() as u64,
            ..ChunkHeadBatchStats::default()
        };
        let mut heads = Vec::with_capacity(chunk_ids.len());
        for chunk_id in chunk_ids {
            heads.push(self.head_chunk(chunk_id)?);
            stats.physical_heads = stats.physical_heads.saturating_add(1);
            stats.metadata_cache_misses = stats.metadata_cache_misses.saturating_add(1);
        }
        Ok((heads, stats))
    }

    pub(crate) fn head_unique_chunk_batch_stats_refs(
        &self,
        chunk_ids: &[ChunkId],
    ) -> Result<ChunkHeadBatchStats, ObjectError> {
        let mut stats = ChunkHeadBatchStats {
            input_heads: chunk_ids.len() as u64,
            ..ChunkHeadBatchStats::default()
        };
        for chunk_id in chunk_ids {
            self.chunks
                .get(chunk_id)
                .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
            stats.physical_heads = stats.physical_heads.saturating_add(1);
            stats.metadata_cache_misses = stats.metadata_cache_misses.saturating_add(1);
        }
        Ok(stats)
    }

    pub fn read_chunk_state_validated(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<ChunkState, ObjectError> {
        let state = self.read_chunk_state(chunk_id)?;
        validate_chunk_state(&state)?;
        Ok(state)
    }

    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.keys().cloned().collect()
    }
}
