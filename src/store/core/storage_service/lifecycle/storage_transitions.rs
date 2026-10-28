// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    // Lifecycle follows the object's existing storage route. A local object has
    // no placement to migrate; a placed object must keep placement validation.
    fn apply_lifecycle_transition(
        &mut self,
        bucket: String,
        key: String,
        storage_class: String,
        update_time_ms: u64,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match self
            .meta_service
            .object_placement(&ObjectId::new(&bucket, &key))
        {
            Ok(placement) => self.apply_transition_object_with_placement(
                bucket,
                key,
                storage_class,
                update_time_ms,
                placement.node_ids.len(),
                if_match_etag,
            ),
            Err(ObjectError::NotFound(_)) => self.apply_transition_object(
                bucket,
                key,
                storage_class,
                update_time_ms,
                if_match_etag,
            ),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn apply_bucket_lifecycle_transitions_at(
        &mut self,
        max_actions: usize,
        now_ms: u64,
    ) -> Result<LifecycleApplyResult, ObjectError> {
        let mut result = LifecycleApplyResult::default();
        let limit = if max_actions == 0 {
            usize::MAX
        } else {
            max_actions
        };
        let policies: Vec<(String, BucketLifecyclePolicy)> = self
            .bucket_lifecycle
            .iter()
            .filter(|(_, policy)| {
                policy.transition_after_ms > 0 && policy.transition_storage_class.is_some()
            })
            .map(|(bucket, policy)| (bucket.clone(), policy.clone()))
            .collect();
        for (bucket, policy) in policies {
            if result.total_actions() as usize >= limit {
                result.truncated = true;
                break;
            }
            let target = policy.transition_storage_class.clone().unwrap();
            let object_ids: Vec<ObjectId> = self
                .objects_iter()
                .map(|(object_id, _)| object_id)
                .filter(|object_id| object_id.bucket == bucket)
                .cloned()
                .collect();
            for object_id in object_ids {
                result.scanned_objects = result.scanned_objects.saturating_add(1);
                if result.total_actions() as usize >= limit {
                    result.truncated = true;
                    break;
                }
                let Some(current) = self.object(&object_id).cloned() else {
                    continue;
                };
                if current.deleted
                    || current.storage_class == target
                    || !lifecycle_policy_matches(&policy, &current)
                    || !lifecycle_age_reached(
                        current.update_time_ms,
                        policy.transition_after_ms,
                        now_ms,
                    )
                {
                    continue;
                }
                match self.apply_lifecycle_transition(
                    object_id.bucket.to_string(),
                    object_id.key.to_string(),
                    target.clone(),
                    now_ms,
                    Some(current.etag().to_string()),
                ) {
                    Ok(_) => {
                        result.transitioned_current_objects =
                            result.transitioned_current_objects.saturating_add(1);
                    }
                    Err(ObjectError::NotFound(_)) | Err(ObjectError::PreconditionFailed(_)) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(result)
    }

}
