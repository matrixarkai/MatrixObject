impl MatrixReplicaService {
    pub fn read_chunk_state(&self, page_id: &PageId) -> Result<PageState, ObjectError> {
        self.pages
            .get(page_id)
            .map(|state| state.as_ref().clone())
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))
    }

    pub fn head_chunk(&self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        let state = self
            .pages
            .get(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        Ok(ReplicaHead {
            logical_length: state.logical_length,
            write_sequence: state.write_sequence,
            checksum: state.checksum.clone(),
            state_checksum: chunk_state_handoff_checksum(page_id, state, &state.checksum),
            discarded_ranges: state.discarded_ranges.clone(),
        })
    }

    pub fn head_chunk_batch(
        &self,
        page_ids: Vec<PageId>,
    ) -> Result<(Vec<ReplicaHead>, ReplicaHeadBatchStats), ObjectError> {
        self.head_chunk_batch_refs(&page_ids)
    }

    pub(crate) fn head_chunk_batch_refs(
        &self,
        page_ids: &[PageId],
    ) -> Result<(Vec<ReplicaHead>, ReplicaHeadBatchStats), ObjectError> {
        if page_ids.len() <= 1 {
            return self.head_unique_chunk_batch_refs(page_ids);
        }
        let mut stats = ReplicaHeadBatchStats {
            input_heads: page_ids.len() as u64,
            ..ReplicaHeadBatchStats::default()
        };
        let mut cached_heads: BTreeMap<&PageId, (usize, Option<ReplicaHead>)> = BTreeMap::new();
        for page_id in page_ids {
            cached_heads
                .entry(page_id)
                .and_modify(|(remaining, _)| *remaining = remaining.saturating_add(1))
                .or_insert((1, None));
        }
        let mut heads = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            let Some((remaining, cached_head)) = cached_heads.get_mut(page_id) else {
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
            let head = self.head_chunk(&page_id)?;
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
        page_ids: &[PageId],
    ) -> Result<(Vec<ReplicaHead>, ReplicaHeadBatchStats), ObjectError> {
        let mut stats = ReplicaHeadBatchStats {
            input_heads: page_ids.len() as u64,
            ..ReplicaHeadBatchStats::default()
        };
        let mut heads = Vec::with_capacity(page_ids.len());
        for page_id in page_ids {
            heads.push(self.head_chunk(page_id)?);
            stats.physical_heads = stats.physical_heads.saturating_add(1);
            stats.metadata_cache_misses = stats.metadata_cache_misses.saturating_add(1);
        }
        Ok((heads, stats))
    }

    pub(crate) fn head_unique_chunk_batch_stats_refs(
        &self,
        page_ids: &[PageId],
    ) -> Result<ReplicaHeadBatchStats, ObjectError> {
        let mut stats = ReplicaHeadBatchStats {
            input_heads: page_ids.len() as u64,
            ..ReplicaHeadBatchStats::default()
        };
        for page_id in page_ids {
            self.pages
                .get(page_id)
                .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
            stats.physical_heads = stats.physical_heads.saturating_add(1);
            stats.metadata_cache_misses = stats.metadata_cache_misses.saturating_add(1);
        }
        Ok(stats)
    }

    pub fn read_chunk_state_validated(
        &self,
        page_id: &PageId,
    ) -> Result<PageState, ObjectError> {
        let state = self.read_chunk_state(page_id)?;
        validate_chunk_state(&state)?;
        Ok(state)
    }

    pub fn page_ids(&self) -> Vec<PageId> {
        self.pages.keys().cloned().collect()
    }
}
