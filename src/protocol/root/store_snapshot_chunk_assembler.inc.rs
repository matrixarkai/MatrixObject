// SPDX-License-Identifier: Apache-2.0
impl StoreSnapshotChunkAssembler {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            self.expected_total_bytes.is_some(),
            self.buffer.len() as u64,
            self.expected_total_bytes.unwrap_or_default(),
            self.expected_full_checksum.clone().unwrap_or_default(),
            self.expected_last_applied_index.unwrap_or_default(),
            self.expected_term.unwrap_or_default(),
        )
    }

    pub fn push_chunk(
        &mut self,
        chunk: StoreSnapshotChunk,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        Ok(self
            .push_chunk_with_checksum(chunk)?
            .map(|assembled| assembled.bytes))
    }

    pub(crate) fn push_chunk_with_checksum(
        &mut self,
        chunk: StoreSnapshotChunk,
    ) -> Result<Option<StoreSnapshotAssembledImage>, ObjectError> {
        if chunk.total_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "store snapshot total bytes must be positive".to_string(),
            ));
        }
        if !etag_matches(&chunk.payload, &chunk.page_checksum) {
            return Err(ObjectError::Corruption(
                "store snapshot chunk checksum mismatch".to_string(),
            ));
        }
        if chunk.full_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "store snapshot full checksum is required".to_string(),
            ));
        }
        if chunk.last_applied_index > 0 && chunk.term == 0 {
            return Err(ObjectError::InvalidArgument(
                "store snapshot term is required with applied index".to_string(),
            ));
        }
        if self.expected_total_bytes.is_none() {
            if chunk.offset != 0 {
                return Err(ObjectError::InvalidArgument(
                    "store snapshot chunk stream must start at offset zero".to_string(),
                ));
            }
            self.expected_total_bytes = Some(chunk.total_bytes);
            self.expected_full_checksum = Some(chunk.full_checksum.clone());
            self.expected_last_applied_index = Some(chunk.last_applied_index);
            self.expected_term = Some(chunk.term);
        }
        if self.expected_total_bytes != Some(chunk.total_bytes)
            || self.expected_full_checksum.as_deref() != Some(chunk.full_checksum.as_str())
            || self.expected_last_applied_index != Some(chunk.last_applied_index)
            || self.expected_term != Some(chunk.term)
        {
            return Err(ObjectError::InvalidArgument(
                "store snapshot chunk stream identity changed".to_string(),
            ));
        }
        let chunk_end = chunk
            .offset
            .checked_add(chunk.payload.len() as u64)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("store snapshot chunk overflow".to_string())
            })?;
        if chunk.offset < self.buffer.len() as u64 {
            if chunk_end > self.buffer.len() as u64 {
                return Err(ObjectError::InvalidArgument(
                    "store snapshot chunk overlaps committed stream tail".to_string(),
                ));
            }
            let start = chunk.offset as usize;
            let end = chunk_end as usize;
            if self.buffer[start..end] != chunk.payload {
                return Err(ObjectError::InvalidArgument(
                    "store snapshot retry payload mismatch".to_string(),
                ));
            }
            return Ok(None);
        }
        if chunk.offset != self.buffer.len() as u64 {
            return Err(ObjectError::InvalidArgument(
                "store snapshot chunks must be contiguous".to_string(),
            ));
        }
        if chunk_end > chunk.total_bytes {
            return Err(ObjectError::InvalidArgument(
                "store snapshot chunk exceeds total size".to_string(),
            ));
        }
        if self.buffer.is_empty()
            && chunk.offset == 0
            && chunk.eof
            && chunk.payload.len() as u64 == chunk.total_bytes
        {
            if !etag_matches(&chunk.payload, &chunk.full_checksum) {
                return Err(ObjectError::Corruption(
                    "store snapshot full checksum mismatch".to_string(),
                ));
            }
            let full_checksum = chunk.full_checksum;
            let image = chunk.payload;
            self.expected_total_bytes = None;
            self.expected_full_checksum = None;
            self.expected_last_applied_index = None;
            self.expected_term = None;
            return Ok(Some(StoreSnapshotAssembledImage {
                bytes: image,
                full_checksum,
            }));
        }
        if self.buffer.is_empty() {
            reserve_snapshot_buffer(
                &mut self.buffer,
                chunk.total_bytes,
                "store",
            )?;
        }
        self.buffer.extend_from_slice(&chunk.payload);
        if !chunk.eof {
            return Ok(None);
        }
        if self.buffer.len() as u64 != chunk.total_bytes {
            return Err(ObjectError::InvalidArgument(
                "store snapshot eof chunk does not complete image".to_string(),
            ));
        }
        if !etag_matches(&self.buffer, &chunk.full_checksum) {
            return Err(ObjectError::Corruption(
                "store snapshot full checksum mismatch".to_string(),
            ));
        }
        let full_checksum = chunk.full_checksum;
        let image = std::mem::take(&mut self.buffer);
        self.expected_total_bytes = None;
        self.expected_full_checksum = None;
        self.expected_last_applied_index = None;
        self.expected_term = None;
        Ok(Some(StoreSnapshotAssembledImage {
            bytes: image,
            full_checksum,
        }))
    }

    pub fn reset(&mut self) {
        self.expected_total_bytes = None;
        self.expected_full_checksum = None;
        self.expected_last_applied_index = None;
        self.expected_term = None;
        self.buffer.clear();
    }

    pub fn abort_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        let report = abort_snapshot_install_progress(self.install_progress(), expected_stream_id)?;
        self.reset();
        Ok(report)
    }
}
