impl ReplicaServiceSnapshotAssembler {
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
        chunk: ReplicaServiceSnapshotChunk,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        Ok(self
            .push_chunk_with_checksum(chunk)?
            .map(|assembled| assembled.bytes))
    }

    pub(crate) fn push_chunk_with_checksum(
        &mut self,
        chunk: ReplicaServiceSnapshotChunk,
    ) -> Result<Option<ReplicaServiceSnapshotAssembledImage>, ObjectError> {
        if chunk.total_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot total bytes must be positive".to_string(),
            ));
        }
        if !etag_matches(&chunk.payload, &chunk.chunk_checksum) {
            return Err(ObjectError::Corruption(
                "chunk service snapshot chunk checksum mismatch".to_string(),
            ));
        }
        if chunk.full_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot full checksum is required".to_string(),
            ));
        }
        if self.expected_total_bytes.is_none() {
            if chunk.offset != 0 {
                return Err(ObjectError::InvalidArgument(
                    "chunk service snapshot stream must start at offset zero".to_string(),
                ));
            }
            self.expected_total_bytes = Some(chunk.total_bytes);
            self.expected_full_checksum = Some(chunk.full_checksum.clone());
        }
        if self.expected_total_bytes != Some(chunk.total_bytes)
            || self.expected_full_checksum.as_deref() != Some(chunk.full_checksum.as_str())
        {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot stream identity changed".to_string(),
            ));
        }
        let chunk_end = chunk
            .offset
            .checked_add(chunk.payload.len() as u64)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("chunk service snapshot overflow".to_string())
            })?;
        if chunk.offset < self.buffer.len() as u64 {
            if chunk_end > self.buffer.len() as u64 {
                return Err(ObjectError::InvalidArgument(
                    "chunk service snapshot chunk overlaps committed stream tail".to_string(),
                ));
            }
            let start = usize::try_from(chunk.offset).map_err(|_| {
                ObjectError::InvalidArgument(
                    "chunk service snapshot offset exceeds memory size".to_string(),
                )
            })?;
            let end = usize::try_from(chunk_end).map_err(|_| {
                ObjectError::InvalidArgument(
                    "chunk service snapshot end exceeds memory size".to_string(),
                )
            })?;
            if self.buffer[start..end] != chunk.payload {
                return Err(ObjectError::InvalidArgument(
                    "chunk service snapshot retry payload mismatch".to_string(),
                ));
            }
            return Ok(None);
        }
        if chunk.offset != self.buffer.len() as u64 {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot chunks must be contiguous".to_string(),
            ));
        }
        if chunk_end > chunk.total_bytes {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot chunk exceeds total size".to_string(),
            ));
        }
        if self.buffer.is_empty()
            && chunk.offset == 0
            && chunk.eof
            && chunk.payload.len() as u64 == chunk.total_bytes
        {
            if !etag_matches(&chunk.payload, &chunk.full_checksum) {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot full checksum mismatch".to_string(),
                ));
            }
            let full_checksum = chunk.full_checksum;
            let image = chunk.payload;
            self.expected_total_bytes = None;
            self.expected_full_checksum = None;
            return Ok(Some(ReplicaServiceSnapshotAssembledImage {
                bytes: image,
                full_checksum,
            }));
        }
        if self.buffer.is_empty() {
            reserve_snapshot_buffer(
                &mut self.buffer,
                chunk.total_bytes,
                "chunk service",
            )?;
        }
        self.buffer.extend_from_slice(&chunk.payload);
        if !chunk.eof {
            return Ok(None);
        }
        if self.buffer.len() as u64 != chunk.total_bytes {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot eof does not complete image".to_string(),
            ));
        }
        if !etag_matches(&self.buffer, &chunk.full_checksum) {
            return Err(ObjectError::Corruption(
                "chunk service snapshot full checksum mismatch".to_string(),
            ));
        }
        let full_checksum = chunk.full_checksum;
        let image = std::mem::take(&mut self.buffer);
        self.expected_total_bytes = None;
        self.expected_full_checksum = None;
        Ok(Some(ReplicaServiceSnapshotAssembledImage {
            bytes: image,
            full_checksum,
        }))
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

fn export_replica_service_snapshot_chunk_slice_with_checksum(
    snapshot_bytes: &[u8],
    full_checksum: &str,
    offset: u64,
    max_payload_bytes: usize,
) -> Result<ReplicaServiceSnapshotChunk, ObjectError> {
    if max_payload_bytes == 0 {
        return Err(ObjectError::InvalidArgument(
            "chunk service snapshot chunk budget must be positive".to_string(),
        ));
    }
    let total_bytes = snapshot_bytes.len() as u64;
    if offset > total_bytes {
        return Err(ObjectError::InvalidArgument(
            "chunk service snapshot offset exceeds image size".to_string(),
        ));
    }
    let remaining = usize::try_from(total_bytes - offset).map_err(|_| {
        ObjectError::InvalidArgument(
            "chunk service snapshot remaining bytes exceed memory size".to_string(),
        )
    })?;
    let emit = remaining.min(max_payload_bytes);
    let start = usize::try_from(offset).map_err(|_| {
        ObjectError::InvalidArgument("chunk service snapshot offset exceeds memory size".to_string())
    })?;
    let end = start.checked_add(emit).ok_or_else(|| {
        ObjectError::InvalidArgument("chunk service snapshot chunk range overflows".to_string())
    })?;
    let payload_slice = &snapshot_bytes[start..end];
    let chunk_checksum = if offset == 0 && emit as u64 == total_bytes {
        full_checksum.to_string()
    } else {
        etag(payload_slice)
    };
    Ok(ReplicaServiceSnapshotChunk {
        offset,
        total_bytes,
        chunk_checksum,
        full_checksum: full_checksum.to_string(),
        eof: offset + emit as u64 >= total_bytes,
        payload: payload_slice.to_vec(),
    })
}
