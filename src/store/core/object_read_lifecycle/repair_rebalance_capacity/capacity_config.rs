impl MatrixObjectStore {
    fn plan_object_repair_work(
        &self,
        metadata: &ObjectMetadata,
    ) -> Result<(u64, u64), ObjectError> {
        let mut chunks: BTreeMap<PageId, u64> = BTreeMap::new();
        for extent in &metadata.extents {
            for loc in
                self.chunk_service
                    .map_range(&extent.chunk_id, extent.offset, extent.length)?
            {
                chunks
                    .entry(loc.page_id)
                    .and_modify(|bytes| *bytes = bytes.saturating_add(loc.length))
                    .or_insert(loc.length);
            }
        }
        let mut degraded_chunks = 0u64;
        let mut degraded_bytes = 0u64;
        for (page_id, bytes) in chunks {
            let repair = self.chunk_service.plan_repair_chunk_replicas(&page_id)?;
            if repair.degraded_chunks > 0 {
                degraded_chunks = degraded_chunks.saturating_add(1);
                degraded_bytes = degraded_bytes.saturating_add(bytes);
            }
        }
        Ok((degraded_chunks, degraded_bytes))
    }

    pub fn set_page_replica_quarantine(
        &mut self,
        replica_index: usize,
        quarantined: bool,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .set_replica_quarantine(replica_index, quarantined)
    }

    pub fn set_page_replica_frozen(
        &mut self,
        replica_index: usize,
        frozen: bool,
    ) -> Result<(), ObjectError> {
        self.chunk_service.set_replica_frozen(replica_index, frozen)
    }

    pub fn configure_chunk_replica_capacity(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<(), ObjectError> {
        self.chunk_service
            .set_chunk_replica_capacity(replica_index, capacity_bytes)
    }

    pub fn configure_all_eligible_chunk_replica_capacity(
        &mut self,
        capacity_bytes: u64,
    ) -> Result<u64, ObjectError> {
        self.chunk_service
            .configure_all_eligible_chunk_replica_capacity(capacity_bytes)
    }

}
