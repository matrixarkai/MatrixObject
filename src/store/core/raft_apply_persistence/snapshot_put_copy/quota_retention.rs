impl MatrixObjectStore {
    fn live_bucket_usage(&self, bucket: &str) -> (u64, u64) {
        self.objects
            .values()
            .filter(|metadata| metadata.object_id.bucket == bucket && !metadata.deleted)
            .fold((0u64, 0u64), |(bytes, objects), metadata| {
                (
                    bytes.saturating_add(metadata.length),
                    objects.saturating_add(1),
                )
            })
    }

    fn check_bucket_quota_for_put(
        &self,
        object_id: &ObjectId,
        new_length: u64,
    ) -> Result<(), ObjectError> {
        let Some(quota) = self.bucket_quotas.get(object_id.bucket.as_str()) else {
            return Ok(());
        };
        if quota.disabled() {
            return Ok(());
        }
        let (live_bytes, live_objects) = self.live_bucket_usage(&object_id.bucket);
        let previous_live = self
            .objects
            .get(object_id)
            .filter(|metadata| !metadata.deleted);
        let projected_bytes = live_bytes
            .saturating_sub(previous_live.map_or(0, |metadata| metadata.length))
            .checked_add(new_length)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("bucket live bytes overflow".to_string())
            })?;
        let projected_objects = if previous_live.is_some() {
            live_objects
        } else {
            live_objects.saturating_add(1)
        };
        if quota.max_live_bytes > 0 && projected_bytes > quota.max_live_bytes {
            return Err(ObjectError::PreconditionFailed(format!(
                "bucket quota live bytes exceeded: projected={projected_bytes} limit={}",
                quota.max_live_bytes
            )));
        }
        if quota.max_live_objects > 0 && projected_objects > quota.max_live_objects {
            return Err(ObjectError::PreconditionFailed(format!(
                "bucket quota live objects exceeded: projected={projected_objects} limit={}",
                quota.max_live_objects
            )));
        }
        Ok(())
    }

    fn check_retention_for_mutation(
        &self,
        metadata: &ObjectMetadata,
        operation: &str,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if metadata.deleted {
            return Ok(());
        }
        if metadata.lock.legal_hold {
            return Err(ObjectError::PreconditionFailed(format!(
                "object legal hold blocks {operation}"
            )));
        }
        if retention_active(&self.bucket_retention, metadata, now_ms) {
            return Err(ObjectError::PreconditionFailed(format!(
                "object retention blocks {operation}"
            )));
        }
        Ok(())
    }

}
