impl StoreSnapshot {
    fn validate_slabs_and_objects(&self) -> Result<(), ObjectError> {
        let mut max_slab_id = 0u64;
        for (slab_id, slab) in &self.slabs {
            if slab_id != &slab.slab_id {
                return Err(ObjectError::Corruption(
                    "snapshot segment key does not match segment info".to_string(),
                ));
            }
            max_slab_id = max_slab_id.max(slab_id.id);
            if slab.next_offset > self.options.slab_size
                || slab.live_bytes.saturating_add(slab.stale_bytes) < slab.live_bytes
                || slab.discarded_bytes > slab.stale_bytes
            {
                return Err(ObjectError::Corruption(
                    "snapshot segment accounting is invalid".to_string(),
                ));
            }
            for range in slab
                .stale_ranges
                .iter()
                .chain(slab.discarded_ranges.iter())
            {
                if range.end()? > slab.next_offset {
                    return Err(ObjectError::Corruption(
                        "snapshot segment range exceeds segment length".to_string(),
                    ));
                }
            }
        }
        if max_slab_id >= self.next_slab_id {
            return Err(ObjectError::Corruption(
                "snapshot next segment id is behind retained segments".to_string(),
            ));
        }
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
        for ((slab_id, offset), payload) in &self.payloads {
            let slab = self.slabs.get(slab_id).ok_or_else(|| {
                ObjectError::Corruption("snapshot payload references missing segment".to_string())
            })?;
            let payload_end = offset.checked_add(payload.len() as u64).ok_or_else(|| {
                ObjectError::Corruption("snapshot payload address overflows".to_string())
            })?;
            if payload_end > slab.next_offset {
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
            if !is_cold_storage_class(&metadata.storage_class) && metadata.restore_until_ms != 0 {
                return Err(ObjectError::Corruption(
                    "snapshot warm object carries restore window".to_string(),
                ));
            }
            if metadata.deleted
                && (metadata.legal_hold
                    || metadata.retention_until_ms != 0
                    || metadata.restore_until_ms != 0
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
                    let slab = self.slabs.get(&extent.slab_id).ok_or_else(|| {
                        ObjectError::Corruption(
                            "snapshot object references missing segment".to_string(),
                        )
                    })?;
                    if extent.offset + extent.length > slab.next_offset {
                        return Err(ObjectError::Corruption(
                            "snapshot object extent exceeds segment length".to_string(),
                        ));
                    }
                    let payload = self
                        .payloads
                        .get(&(extent.slab_id.clone(), extent.offset))
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
