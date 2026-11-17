// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixReplicaService {
    /// Apply a node recovery policy after restoring its checkpoint. Payload
    /// bytes and their checksums are unchanged; only local recovery data changes.
    pub(crate) fn configure_journal_recovery(
        &mut self,
        keep_sidecar: bool,
    ) -> Result<(), ObjectError> {
        if self.active_mutation_batch.is_some() {
            return Err(ObjectError::PreconditionFailed(
                "recovery policy cannot change during a mutation batch".into(),
            ));
        }
        self.snapshot_export_cache.borrow_mut().take();
        self.keep_ec_sidecar = keep_sidecar;
        let changed = self
            .pages
            .iter()
            .filter(|(_, state)| {
                if keep_sidecar {
                    !chunk_keeps_ec_sidecar(state)
                } else {
                    !state.fragment_bytes().is_empty() || !state.fragment_digests().is_empty()
                }
            })
            .map(|(page, _)| page.clone())
            .collect::<Vec<_>>();
        for page in changed {
            self.materialise_page_undo(&page);
            let state = self.pages.get_mut(&page).unwrap();
            if keep_sidecar {
                refresh_chunk_ec_sidecars(std::sync::Arc::make_mut(state));
            } else {
                clear_chunk_ec_sidecars(std::sync::Arc::make_mut(state));
            }
        }
        Ok(())
    }

    pub(crate) fn prepare_journal_append(
        &self,
        page: &PageId,
        offset: u64,
        suffix: &[u8],
    ) -> Result<Option<crate::outcome::PreparedPageAppend>, ObjectError> {
        if self.keep_ec_sidecar
            || self.active_mutation_batch.is_some()
            || self.writable_admission_error().is_some()
            || self.ensure_namespace_open(&page.chunk_id).is_err()
        {
            return Ok(None);
        }
        let Some(state) = self.pages.get(page) else {
            return Ok(None);
        };
        let length = state.data.len();
        if length == 0
            || suffix.is_empty()
            || offset != length as u64
            || state.logical_length != offset
            || !state.discarded_ranges.is_empty()
            || !state.fragment_bytes().is_empty()
            || !state.fragment_digests().is_empty()
        {
            return Ok(None);
        }
        let Some(end) = offset
            .checked_add(suffix.len() as u64)
            .filter(|end| *end <= 256 * 1024 * 1024)
        else {
            return Ok(None);
        };
        let projected = self.projected_physical_bytes_for_chunk_len(page, end)?;
        if self.capacity_bytes > 0 && projected > self.capacity_bytes {
            return Ok(None);
        }
        // A cold identity validates the complete prefix once. A warm identity
        // checks the old tail's validation block. Other blocks retain their
        // checksums and are verified when read or scrubbed, as ordinary reads do.
        // All service-controlled mutations invalidate or correctly extend this
        // memo. No unvalidated external state is accepted through this path.
        if !self.chunk_pages_valid(page, state, length - 1, length) {
            return Err(ObjectError::Corruption(
                "journal append prefix checksum mismatch".into(),
            ));
        }
        crate::outcome::PageOutcome::after_validated_append(page, state, suffix).map(Some)
    }

    pub(crate) fn verify_journal_append(
        &self,
        append: &crate::outcome::PreparedPageAppend,
    ) -> Result<(), ObjectError> {
        let outcome = &append.outcome;
        let page = outcome.page_id()?;
        let state = self
            .pages
            .get(&page)
            .ok_or_else(|| ObjectError::Corruption("journal append page disappeared".into()))?;
        if state.data.len() as u64 != outcome.data_length
            || state.logical_length != outcome.logical_length
            || state.write_sequence != outcome.write_sequence
            || state.checksum_fnv1a64 != outcome.data_checksum
            || !state.discarded_ranges.is_empty()
            || !state.fragment_bytes().is_empty()
            || !state.fragment_digests().is_empty()
            || append.offset > state.data.len()
            || ring::digest::digest(&ring::digest::SHA256, &state.data[append.offset..]).as_ref()
                != append.suffix_sha256.as_slice()
        {
            return Err(ObjectError::Corruption(
                "journal append resulting state mismatch".into(),
            ));
        }
        Ok(())
    }
}
