impl StoreSnapshot {
    fn validate_objects(&self) -> Result<(), ObjectError> {
        for (object_id, metadata) in &self.objects {
            if &metadata.object_id != object_id {
                return Err(ObjectError::Corruption(
                    "snapshot object map key does not match metadata identity".to_string(),
                ));
            }
        }
        for (object_id, versions) in &self.versions {
            if versions.is_empty() {
                return Err(ObjectError::Corruption(
                    "snapshot object version index stores empty history".to_string(),
                ));
            }
            for metadata in versions {
                if &metadata.object_id != object_id {
                    return Err(ObjectError::Corruption(
                        "snapshot object version key does not match metadata identity".to_string(),
                    ));
                }
            }
        }
        for ((chunk_id, offset), payload) in &self.payloads {
            let chunk = self.chunks.get(chunk_id).ok_or_else(|| {
                ObjectError::Corruption("snapshot payload references missing segment".to_string())
            })?;
            let payload_end = offset.checked_add(payload.len() as u64).ok_or_else(|| {
                ObjectError::Corruption("snapshot payload address overflows".to_string())
            })?;
            if payload_end > chunk.next_offset {
                return Err(ObjectError::Corruption(
                    "snapshot payload exceeds segment length".to_string(),
                ));
            }
        }
        for metadata in self
            .objects
            .values()
            .chain(self.versions.values().flat_map(|versions| versions.iter()))
        {
            validate_bucket(&metadata.object_id.bucket)?;
            if !metadata.object_id.key.is_empty() {
                validate_key(&metadata.object_id.key)?;
            }
            if metadata.version == 0 {
                return Err(ObjectError::Corruption(
                    "snapshot object version is zero".to_string(),
                ));
            }
            validate_user_metadata(&metadata.user_metadata)?;
            validate_tags(&metadata.tags)?;
            validate_storage_class(&metadata.storage_class)?;
            if !is_cold_storage_class(&metadata.storage_class) && metadata.lock.restore_until_ms != 0 {
                return Err(ObjectError::Corruption(
                    "snapshot warm object carries restore window".to_string(),
                ));
            }
            if metadata.deleted
                && (metadata.lock.legal_hold
                    || metadata.lock.retention_until_ms != 0
                    || metadata.lock.restore_until_ms != 0
                    || metadata.storage_class != k_standard_storage_class())
            {
                return Err(ObjectError::Corruption(
                    "snapshot delete marker carries governance or storage state".to_string(),
                ));
            }
            let extent_bytes = metadata.extents.iter().try_fold(
                0u64,
                |acc, extent| -> Result<u64, ObjectError> {
                    extent.offset.checked_add(extent.length).ok_or_else(|| {
                        ObjectError::Corruption("snapshot extent address overflows".to_string())
                    })?;
                    let chunk = self.chunks.get(&extent.chunk_id).ok_or_else(|| {
                        ObjectError::Corruption(
                            "snapshot object references missing segment".to_string(),
                        )
                    })?;
                    if extent.offset + extent.length > chunk.next_offset {
                        return Err(ObjectError::Corruption(
                            "snapshot object extent exceeds segment length".to_string(),
                        ));
                    }
                    let payload = self
                        .payloads
                        .get(&(extent.chunk_id.clone(), extent.offset))
                        .ok_or_else(|| {
                            ObjectError::Corruption(
                                "snapshot object extent payload missing".to_string(),
                            )
                        })?;
                    if payload.len() as u64 != extent.length {
                        return Err(ObjectError::Corruption(
                            "snapshot payload length does not match extent".to_string(),
                        ));
                    }
                    acc.checked_add(extent.length).ok_or_else(|| {
                        ObjectError::Corruption("snapshot object length overflows".to_string())
                    })
                },
            )?;
            if metadata.deleted {
                if metadata.length != 0 || !metadata.extents.is_empty() {
                    return Err(ObjectError::Corruption(
                        "snapshot delete marker carries payload".to_string(),
                    ));
                }
            } else if extent_bytes != metadata.length {
                return Err(ObjectError::Corruption(
                    "snapshot object extents do not match object length".to_string(),
                ));
            }
        }
        Ok(())
    }
}
