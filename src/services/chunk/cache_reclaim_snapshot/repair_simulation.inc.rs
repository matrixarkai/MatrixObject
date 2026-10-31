// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixReplicaService {
    pub fn remove_chunk_for_repair(&mut self, page_id: &PageId) -> bool {
        self.invalidate_chunk_cache(page_id, 0, u64::MAX);
        // THE PAGE ITSELF GOES, so the batch undo has to hold its whole pre-image
        // before it does -- a length cannot describe a page that is not there.
        self.materialise_page_undo(page_id);
        let accounting_before = self.page_accounting(page_id);
        let removed = self.pages.remove(page_id).is_some();
        self.reaccount_page(page_id, accounting_before);
        removed
    }

    pub fn corrupt_chunk_payload_for_repair(
        &mut self,
        page_id: &PageId,
    ) -> Result<(), ObjectError> {
        // NOT AN APPEND -- it damages byte zero -- so the batch undo cannot be a
        // length here.
        self.materialise_page_undo(page_id);
        let accounting_before = self.page_accounting(page_id);
        let chunk = self
            .pages
            .get_mut(page_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        let chunk = std::sync::Arc::make_mut(chunk);
        if chunk.data.is_empty() {
            std::sync::Arc::make_mut(&mut chunk.data).push(0xff);
        } else {
            std::sync::Arc::make_mut(&mut chunk.data)[0] ^= 0xff;
        }
        self.invalidate_chunk_cache(page_id, 0, u64::MAX);
        self.reaccount_page(page_id, accounting_before);
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
