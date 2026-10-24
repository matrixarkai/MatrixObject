// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_bucket_lifecycle_noncurrent(
        &mut self,
        bucket: &str,
        policy: &BucketLifecyclePolicy,
        now_ms: u64,
        limit: usize,
        result: &mut LifecycleApplyResult,
    ) -> Result<(), ObjectError> {
        if policy.noncurrent_versions_to_keep == 0 && policy.noncurrent_older_than_ms == 0 {
            return Ok(());
        }
        let keys: Vec<ObjectId> = self
            .versions
            .keys()
            .filter(|object_id| object_id.bucket == bucket)
            .cloned()
            .collect();
        let bucket_retention = self.bucket_retention.clone();
        for object_id in keys {
            if result.total_actions() as usize >= limit {
                result.truncated = true;
                break;
            }
            let mut stale_extents = Vec::new();
            let mut remove_key = false;
            if let Some(versions) = self.versions.get_mut(&object_id) {
                versions.sort_by(|a, b| b.version.cmp(&a.version));
                let candidates = std::mem::take(versions);
                let mut retained = Vec::with_capacity(candidates.len());
                for (index, metadata) in candidates.into_iter().enumerate() {
                    if result.total_actions() as usize >= limit {
                        retained.push(metadata);
                        result.truncated = true;
                        continue;
                    }
                    let beyond_keep = index >= policy.noncurrent_versions_to_keep;
                    let old_enough = policy.noncurrent_older_than_ms == 0
                        || lifecycle_age_reached(
                            metadata.update_time_ms,
                            policy.noncurrent_older_than_ms,
                            now_ms,
                        );
                    let protected = !metadata.deleted
                        && (metadata.lock.legal_hold
                            || retention_active(&bucket_retention, &metadata, now_ms));
                    if beyond_keep
                        && old_enough
                        && lifecycle_policy_matches(policy, &metadata)
                        && !protected
                    {
                        result.expired_noncurrent_versions =
                            result.expired_noncurrent_versions.saturating_add(1);
                        stale_extents.extend(metadata.extents.iter().cloned());
                    } else {
                        retained.push(metadata);
                    }
                }
                *versions = retained;
                versions.sort_by(|a, b| b.version.cmp(&a.version));
                remove_key = versions.is_empty();
            }
            for extents in stale_extents.chunks(1024) {
                self.mark_extents_stale(extents)?;
            }
            if remove_key {
                self.versions.remove(&object_id);
            }
        }
        Ok(())
    }

}
