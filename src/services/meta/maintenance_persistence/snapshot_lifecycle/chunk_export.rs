impl MatrixMetaService {
    fn snapshot_cache_stamp(&self) -> String {
        format!(
            "meta:{}:{}",
            self.next_commit_index.saturating_sub(1),
            self.metadata_state_fingerprint()
        )
    }

    fn export_snapshot_image(&self) -> Result<MetaSnapshotExportCache, ObjectError> {
        let stamp = self.snapshot_cache_stamp();
        if let Some(cache) = self.snapshot_export_cache.borrow().as_ref() {
            if cache.stamp == stamp {
                return Ok(cache.clone());
            }
        }
        let bytes = self.export_snapshot().to_bytes()?;
        let cache = MetaSnapshotExportCache {
            stamp,
            full_checksum: etag(&bytes),
            last_applied_index: self.next_commit_index.saturating_sub(1),
            bytes: std::sync::Arc::from(bytes.into_boxed_slice()),
        };
        *self.snapshot_export_cache.borrow_mut() = Some(cache.clone());
        Ok(cache)
    }

    pub fn export_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<MetaSnapshotChunk, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let chunk = export_meta_snapshot_chunk_slice_with_checksum(
            image.bytes.as_ref(),
            &image.full_checksum,
            image.last_applied_index,
            offset,
            max_payload_bytes,
        )?;
        if chunk.eof {
            let mut cache = self.snapshot_export_cache.borrow_mut();
            if cache
                .as_ref()
                .map(|cached| cached.stamp == image.stamp)
                .unwrap_or(false)
            {
                *cache = None;
            }
        }
        Ok(chunk)
    }

    pub fn export_snapshot_chunk_stream<F>(
        &self,
        max_payload_bytes: usize,
        mut emit: F,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        F: FnMut(MetaSnapshotChunk) -> Result<(), ObjectError>,
    {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut offset = 0;
        loop {
            let chunk = export_meta_snapshot_chunk_slice_with_checksum(
                image.bytes.as_ref(),
                &image.full_checksum,
                image.last_applied_index,
                offset,
                max_payload_bytes,
            )?;
            offset = offset.saturating_add(chunk.payload.len() as u64);
            let eof = chunk.eof;
            let progress = chunk.install_progress();
            const EMPTY_PROGRESS_MESSAGE: &str = "metadata snapshot chunk export made no progress";
            if !eof && chunk.payload.is_empty() {
                return Err(ObjectError::Corruption(EMPTY_PROGRESS_MESSAGE.to_string()));
            }
            emit(chunk)?;
            if eof {
                let mut cache = self.snapshot_export_cache.borrow_mut();
                if cache
                    .as_ref()
                    .map(|cached| cached.stamp == image.stamp)
                    .unwrap_or(false)
                {
                    *cache = None;
                }
                return Ok(progress);
            }
        }
    }

    pub fn export_snapshot_chunks(
        &self,
        max_payload_bytes: usize,
    ) -> Result<Vec<MetaSnapshotChunk>, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut pages = Vec::with_capacity(snapshot_chunk_vec_capacity(
            image.bytes.len() as u64,
            max_payload_bytes,
        ));
        let mut offset = 0;
        loop {
            let chunk = export_meta_snapshot_chunk_slice_with_checksum(
                image.bytes.as_ref(),
                &image.full_checksum,
                image.last_applied_index,
                offset,
                max_payload_bytes,
            )?;
            offset = offset.saturating_add(chunk.payload.len() as u64);
            let eof = chunk.eof;
            if !eof && chunk.payload.is_empty() {
                return Err(ObjectError::Corruption(
                    "metadata snapshot chunk export made no progress".to_string(),
                ));
            }
            pages.push(chunk);
            if eof {
                let mut cache = self.snapshot_export_cache.borrow_mut();
                if cache
                    .as_ref()
                    .map(|cached| cached.stamp == image.stamp)
                    .unwrap_or(false)
                {
                    *cache = None;
                }
                break;
            }
        }
        Ok(pages)
    }

}
