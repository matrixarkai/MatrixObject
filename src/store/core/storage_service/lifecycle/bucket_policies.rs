impl MatrixObjectStore {
    pub(crate) fn apply_bucket_lifecycle_policies_at(
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
            .map(|(bucket, policy)| (bucket.clone(), policy.clone()))
            .collect();
        for (bucket, policy) in policies {
            if result.total_actions() as usize >= limit {
                result.truncated = true;
                break;
            }
            if policy.abort_incomplete_multipart_after_ms > 0 {
                let upload_ids: Vec<String> = self
                    .multipart_uploads
                    .iter()
                    .filter(|(_, upload)| upload.bucket == bucket)
                    .map(|(upload_id, _)| upload_id.clone())
                    .collect();
                for upload_id in upload_ids {
                    result.scanned_objects = result.scanned_objects.saturating_add(1);
                    if result.total_actions() as usize >= limit {
                        result.truncated = true;
                        break;
                    }
                    let Some(upload) = self.multipart_uploads.get(&upload_id).cloned() else {
                        continue;
                    };
                    if upload.create_time_ms > now_ms
                        || !lifecycle_policy_matches_upload(&policy, &upload)
                        || !lifecycle_age_reached(
                            upload.create_time_ms,
                            policy.abort_incomplete_multipart_after_ms,
                            now_ms,
                        )
                    {
                        continue;
                    }
                    self.apply_abort_multipart_upload(upload_id)?;
                    result.aborted_multipart_uploads =
                        result.aborted_multipart_uploads.saturating_add(1);
                }
                if result.total_actions() as usize >= limit {
                    result.truncated = true;
                    break;
                }
            }
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
                if current.deleted {
                    if policy.expire_delete_markers
                        && lifecycle_policy_matches(&policy, &current)
                        && self.versions.get(&object_id).map_or(true, |versions| {
                            !versions.iter().any(|version| !version.deleted)
                        })
                    {
                        self.objects.remove(&object_id);
                        result.removed_delete_markers =
                            result.removed_delete_markers.saturating_add(1);
                    }
                    continue;
                }
                if !lifecycle_policy_matches(&policy, &current) {
                    continue;
                }
                if policy.transition_after_ms > 0
                    && policy.transition_storage_class.is_some()
                    && lifecycle_age_reached(
                        current.update_time_ms,
                        policy.transition_after_ms,
                        now_ms,
                    )
                {
                    let target = policy.transition_storage_class.clone().unwrap();
                    if current.storage_class != target {
                        match self.apply_transition_object_with_root_placement(
                            object_id.bucket.clone(),
                            object_id.key.clone(),
                            target,
                            now_ms,
                            self.options.replica_count,
                            Some(current.etag.clone()),
                        ) {
                            Ok(_) => {
                                result.transitioned_current_objects =
                                    result.transitioned_current_objects.saturating_add(1);
                                if result.total_actions() as usize >= limit {
                                    result.truncated = true;
                                    break;
                                }
                            }
                            Err(ObjectError::NotFound(_))
                            | Err(ObjectError::PreconditionFailed(_)) => {}
                            Err(err) => return Err(err),
                        }
                    }
                }
                let latest = self.objects.get(&object_id).cloned();
                if policy.expire_current_after_ms > 0
                    && latest.as_ref().map_or(false, |metadata| {
                        !metadata.deleted
                            && lifecycle_policy_matches(&policy, metadata)
                            && lifecycle_age_reached(
                                metadata.update_time_ms,
                                policy.expire_current_after_ms,
                                now_ms,
                            )
                    })
                {
                    match self.apply_delete(
                        object_id.bucket.clone(),
                        object_id.key.clone(),
                        now_ms,
                        latest.as_ref().map(|metadata| metadata.etag.clone()),
                    ) {
                        Ok(_) => {
                            result.expired_current_objects =
                                result.expired_current_objects.saturating_add(1);
                        }
                        Err(ObjectError::PreconditionFailed(_)) => {}
                        Err(err) => return Err(err),
                    }
                }
            }
            if result.total_actions() as usize >= limit {
                result.truncated = true;
                break;
            }
            self.apply_bucket_lifecycle_noncurrent(&bucket, &policy, now_ms, limit, &mut result)?;
        }
        Ok(result)
    }

}
