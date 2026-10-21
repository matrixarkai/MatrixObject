// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn apply_lifecycle(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
    ) -> Result<u64, ObjectError> {
        let now_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let expired = self.apply_lifecycle_at(bucket, noncurrent_versions_to_keep, now_ms)?;
        if expired > 0 {
            if let Err(err) = self.append_legacy_lifecycle_journal_record(
                bucket,
                noncurrent_versions_to_keep,
                now_ms,
            ) {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        Ok(expired)
    }

    pub fn apply_lifecycle_with_metadata_read(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        options: MetadataReadOptions,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(&meta_bucket_key(bucket), &options)?;
        let expired = self.apply_lifecycle(bucket, noncurrent_versions_to_keep)?;
        Ok((expired, readiness))
    }

    pub(crate) fn apply_lifecycle_at(
        &mut self,
        bucket: &str,
        noncurrent_versions_to_keep: usize,
        now_ms: u64,
    ) -> Result<u64, ObjectError> {
        validate_bucket(bucket)?;
        let mut expired = 0u64;
        let keys: Vec<ObjectId> = self
            .versions
            .keys()
            .filter(|object_id| object_id.bucket == bucket)
            .cloned()
            .collect();
        for object_id in keys {
            let mut stale_extents = Vec::new();
            let mut remove_key = false;
            if let Some(versions) = self.versions.get_mut(&object_id) {
                versions.sort_by(|a, b| b.version.cmp(&a.version));
                if versions.len() > noncurrent_versions_to_keep {
                    let candidates = versions.split_off(noncurrent_versions_to_keep);
                    let mut retained = Vec::new();
                    for metadata in candidates {
                        if metadata.deleted
                            || (!metadata.lock.legal_hold
                                && !retention_active(&self.bucket_retention, &metadata, now_ms))
                        {
                            expired = expired.saturating_add(1);
                            stale_extents.extend(metadata.extents);
                        } else {
                            retained.push(metadata);
                        }
                    }
                    versions.extend(retained);
                    versions.sort_by(|a, b| b.version.cmp(&a.version));
                }
                remove_key = versions.is_empty();
            }
            for extents in stale_extents.chunks(1024) {
                self.mark_extents_stale(extents)?;
            }
            if remove_key {
                self.versions.remove(&object_id);
            }
        }
        Ok(expired)
    }

}
