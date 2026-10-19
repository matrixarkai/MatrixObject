impl RootSnapshotChunkAssembler {
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

    pub fn push_chunk(&mut self, chunk: RootSnapshotChunk) -> Result<Option<Vec<u8>>, ObjectError> {
        if chunk.total_bytes == 0 {
            return Err(ObjectError::Corruption(
                "root snapshot chunk has zero total bytes".to_string(),
            ));
        }
        if !etag_matches(&chunk.payload, &chunk.page_checksum) {
            return Err(ObjectError::Corruption(
                "root snapshot chunk checksum mismatch".to_string(),
            ));
        }
        if let Some(expected_total) = self.expected_total_bytes {
            if expected_total != chunk.total_bytes {
                return Err(ObjectError::Corruption(
                    "root snapshot total bytes changed".to_string(),
                ));
            }
        } else {
            self.expected_total_bytes = Some(chunk.total_bytes);
        }
        if let Some(expected_full) = &self.expected_full_checksum {
            if expected_full != &chunk.full_checksum {
                return Err(ObjectError::Corruption(
                    "root snapshot full checksum changed".to_string(),
                ));
            }
        } else {
            self.expected_full_checksum = Some(chunk.full_checksum.clone());
        }
        let end = chunk
            .offset
            .checked_add(chunk.payload.len() as u64)
            .ok_or_else(|| {
                ObjectError::Corruption("root snapshot chunk offset overflow".to_string())
            })?;
        if chunk.offset < self.buffer.len() as u64 {
            if end > self.buffer.len() as u64 {
                return Err(ObjectError::Corruption(
                    "root snapshot chunk overlaps committed stream tail".to_string(),
                ));
            }
            let start = chunk.offset as usize;
            let end_index = end as usize;
            if self.buffer[start..end_index] != chunk.payload {
                return Err(ObjectError::Corruption(
                    "root snapshot retry payload mismatch".to_string(),
                ));
            }
            return Ok(None);
        }
        if chunk.offset != self.buffer.len() as u64 {
            return Err(ObjectError::Corruption(
                "root snapshot chunk offset is not contiguous".to_string(),
            ));
        }
        if end > chunk.total_bytes {
            return Err(ObjectError::Corruption(
                "root snapshot chunk exceeds total bytes".to_string(),
            ));
        }
        if self.buffer.is_empty() {
            reserve_snapshot_buffer(
                &mut self.buffer,
                chunk.total_bytes,
                "root",
            )?;
        }
        self.buffer.extend_from_slice(&chunk.payload);
        if chunk.eof {
            if self.buffer.len() as u64 != chunk.total_bytes {
                return Err(ObjectError::Corruption(
                    "root snapshot eof before total bytes".to_string(),
                ));
            }
            if !etag_matches(&self.buffer, &chunk.full_checksum) {
                return Err(ObjectError::Corruption(
                    "root snapshot full checksum mismatch".to_string(),
                ));
            }
            let image = std::mem::take(&mut self.buffer);
            self.expected_total_bytes = None;
            self.expected_full_checksum = None;
            return Ok(Some(image));
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

pub(crate) fn export_root_snapshot_chunk_bytes_with_checksum(
    snapshot_bytes: &[u8],
    full_checksum: &str,
    offset: u64,
    max_payload_bytes: usize,
) -> Result<RootSnapshotChunk, ObjectError> {
    if max_payload_bytes == 0 {
        return Err(ObjectError::InvalidArgument(
            "max_payload_bytes must be positive".to_string(),
        ));
    }
    if offset > snapshot_bytes.len() as u64 {
        return Err(ObjectError::InvalidArgument(
            "root snapshot offset exceeds payload".to_string(),
        ));
    }
    let start = offset as usize;
    let end = (start + max_payload_bytes).min(snapshot_bytes.len());
    let payload_slice = &snapshot_bytes[start..end];
    let page_checksum = if offset == 0 && end == snapshot_bytes.len() {
        full_checksum.to_string()
    } else {
        etag(payload_slice)
    };
    Ok(RootSnapshotChunk {
        offset,
        total_bytes: snapshot_bytes.len() as u64,
        page_checksum,
        full_checksum: full_checksum.to_string(),
        eof: end == snapshot_bytes.len(),
        payload: payload_slice.to_vec(),
    })
}
