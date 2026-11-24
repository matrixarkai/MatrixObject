// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixReplicaService {
    pub(crate) fn resident_page_capacity(&self, id: &PageId) -> Option<u64> {
        self.pages.get(id).map(|p| p.data.capacity() as u64)
    }
    pub(crate) fn resident_page_stamp(&self, id: &PageId) -> Option<String> {
        self.pages
            .get(id)
            .map(|p| chunk_local_state_checksum_with_data_etag(p, &p.checksum()))
    }
    /// Runtime cache admission is not a logical mutation or a repair operation.
    pub(crate) fn admit_resident_page(
        &mut self,
        id: PageId,
        state: PageState,
    ) -> Result<(), ObjectError> {
        if self.keep_ec_sidecar
            || self.active_mutation_batch.is_some()
            || self.pages.contains_key(&id)
        {
            return Err(ObjectError::PreconditionFailed(
                "invalid resident page admission".into(),
            ));
        }
        validate_chunk_state(&state)?;
        let before = self.page_accounting(&id);
        self.invalidate_chunk_cache(&id, 0, state.logical_length);
        self.page_validation.borrow_mut().remove(&id);
        self.materialise_page_undo(&id);
        self.pages.insert(id.clone(), std::sync::Arc::new(state));
        self.reaccount_page(&id, before);
        *self.snapshot_export_cache.borrow_mut() = None;
        Ok(())
    }
    /// The caller first checks this version has durable canonical backing.
    pub(crate) fn evict_resident_page(&mut self, id: &PageId) -> Result<(), ObjectError> {
        if self.keep_ec_sidecar || self.active_mutation_batch.is_some() {
            return Err(ObjectError::PreconditionFailed(
                "invalid resident page eviction".into(),
            ));
        }
        let before = self.page_accounting(id);
        let length = self.pages.get(id).map_or(0, |p| p.logical_length);
        self.invalidate_chunk_cache(id, 0, length);
        self.page_validation.borrow_mut().remove(id);
        self.materialise_page_undo(id);
        self.pages.remove(id);
        self.reaccount_page(id, before);
        *self.snapshot_export_cache.borrow_mut() = None;
        Ok(())
    }
    pub(crate) fn reject_global_capacity_write(&mut self) -> ObjectError {
        self.capacity_rejected_writes = self.capacity_rejected_writes.saturating_add(1);
        ObjectError::PreconditionFailed("replica byte capacity exceeded".into())
    }
}
