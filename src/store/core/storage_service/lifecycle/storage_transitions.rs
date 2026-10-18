impl MatrixObjectStore {
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
                .objects
                .keys()
                .filter(|object_id| object_id.bucket == bucket)
                .cloned()
                .collect();
            for object_id in object_ids {
                result.scanned_objects = result.scanned_objects.saturating_add(1);
                if result.total_actions() as usize >= limit {
                    result.truncated = true;
                    break;
                }
                let Some(current) = self.objects.get(&object_id).cloned() else {
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
                match self.apply_transition_object_with_root_placement(
                    object_id.bucket.to_string(),
                    object_id.key.to_string(),
                    target.clone(),
                    now_ms,
                    self.options.replica_count,
                    Some(current.etag.clone()),
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
