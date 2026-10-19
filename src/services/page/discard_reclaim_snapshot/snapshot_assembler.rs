// SPDX-License-Identifier: Apache-2.0
impl ChunkServiceSnapshotAssembler {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            self.expected_total_bytes.is_some(),
            self.buffer.len() as u64,
            self.expected_total_bytes.unwrap_or_default(),
            self.expected_full_checksum.clone().unwrap_or_default(),
            0,
            0,
        )
    }

    pub fn push_chunk(
        &mut self,
        chunk: ChunkServiceSnapshotChunk,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        Ok(self
            .push_chunk_with_checksum(chunk)?
            .map(|assembled| assembled.bytes))
    }

    pub(crate) fn push_chunk_with_checksum(
        &mut self,
        chunk: ChunkServiceSnapshotChunk,
    ) -> Result<Option<ChunkServiceSnapshotAssembledImage>, ObjectError> {
        if chunk.total_bytes == 0 {
            return Err(ObjectError::Corruption(
                "page service snapshot chunk has zero total bytes".to_string(),
            ));
        }
        if !etag_matches(&chunk.payload, &chunk.page_checksum) {
            return Err(ObjectError::Corruption(
                "page service snapshot chunk checksum mismatch".to_string(),
            ));
        }
        if let Some(expected_total) = self.expected_total_bytes {
            if expected_total != chunk.total_bytes {
                return Err(ObjectError::Corruption(
                    "page service snapshot total bytes changed".to_string(),
                ));
            }
        } else {
            self.expected_total_bytes = Some(chunk.total_bytes);
        }
        if let Some(expected_full) = &self.expected_full_checksum {
            if expected_full != &chunk.full_checksum {
                return Err(ObjectError::Corruption(
                    "page service snapshot full checksum changed".to_string(),
                ));
            }
        } else {
            self.expected_full_checksum = Some(chunk.full_checksum.clone());
        }
        let end = chunk
            .offset
            .checked_add(chunk.payload.len() as u64)
            .ok_or_else(|| {
                ObjectError::Corruption("page service snapshot chunk offset overflow".to_string())
            })?;
        if chunk.offset < self.buffer.len() as u64 {
            if end > self.buffer.len() as u64 {
                return Err(ObjectError::Corruption(
                    "page service snapshot chunk overlaps committed stream tail".to_string(),
                ));
            }
            let start = usize::try_from(chunk.offset).map_err(|_| {
                ObjectError::Corruption(
                    "page service snapshot chunk offset exceeds memory size".to_string(),
                )
            })?;
            let end_index = usize::try_from(end).map_err(|_| {
                ObjectError::Corruption(
                    "page service snapshot chunk end exceeds memory size".to_string(),
                )
            })?;
            if self.buffer[start..end_index] != chunk.payload {
                return Err(ObjectError::Corruption(
                    "page service snapshot retry payload mismatch".to_string(),
                ));
            }
            return Ok(None);
        }
        if chunk.offset != self.buffer.len() as u64 {
            return Err(ObjectError::Corruption(
                "page service snapshot chunk offset is not contiguous".to_string(),
            ));
        }
        if end > chunk.total_bytes {
            return Err(ObjectError::Corruption(
                "page service snapshot chunk exceeds total bytes".to_string(),
            ));
        }
        if self.buffer.is_empty()
            && chunk.offset == 0
            && chunk.eof
            && chunk.payload.len() as u64 == chunk.total_bytes
        {
            if !etag_matches(&chunk.payload, &chunk.full_checksum) {
                return Err(ObjectError::Corruption(
                    "page service snapshot full checksum mismatch".to_string(),
                ));
            }
            let full_checksum = chunk.full_checksum;
            let image = chunk.payload;
            self.expected_total_bytes = None;
            self.expected_full_checksum = None;
            return Ok(Some(ChunkServiceSnapshotAssembledImage {
                bytes: image,
                full_checksum,
            }));
        }
        if self.buffer.is_empty() {
            reserve_snapshot_buffer(
                &mut self.buffer,
                chunk.total_bytes,
                "block service",
            )?;
        }
        self.buffer.extend_from_slice(&chunk.payload);
        if chunk.eof {
            if self.buffer.len() as u64 != chunk.total_bytes {
                return Err(ObjectError::Corruption(
                    "page service snapshot eof before total bytes".to_string(),
                ));
            }
            if !etag_matches(&self.buffer, &chunk.full_checksum) {
                return Err(ObjectError::Corruption(
                    "page service snapshot full checksum mismatch".to_string(),
                ));
            }
            let full_checksum = chunk.full_checksum;
            let image = std::mem::take(&mut self.buffer);
            self.expected_total_bytes = None;
            self.expected_full_checksum = None;
            return Ok(Some(ChunkServiceSnapshotAssembledImage {
                bytes: image,
                full_checksum,
            }));
        }
        Ok(None)
    }

    pub fn reset(&mut self) {
        self.expected_total_bytes = None;
        self.expected_full_checksum = None;
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
