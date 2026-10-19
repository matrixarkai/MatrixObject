// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn restore_object_version_source(
        &self,
        bucket: String,
        key: String,
        version: u64,
        update_time_ms: u64,
    ) -> Result<(ObjectId, ObjectMetadata), ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        if version == 0 {
            return Err(ObjectError::InvalidArgument(
                "version must be positive".to_string(),
            ));
        }
        if !self.bucket_versioning.contains(&bucket) {
            return Err(ObjectError::PreconditionFailed(format!(
                "bucket versioning is not enabled: {}",
                bucket
            )));
        }
        if !self.buckets.contains(bucket.as_str()) {
            return Err(ObjectError::NotFound(format!(
                "bucket not found: {}",
                bucket
            )));
        }
        let object_id = ObjectId::new(bucket, key);
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service
            .check_lease_fence(&meta_key, None, None, update_time_ms)?;
        let source = self
            .versions
            .get(&object_id)
            .and_then(|versions| {
                versions
                    .iter()
                    .find(|metadata| metadata.version == version && !metadata.deleted)
            })
            .cloned()
            .ok_or_else(|| {
                ObjectError::NotFound(format!(
                    "{}/{}#{}",
                    object_id.bucket, object_id.key, version
                ))
            })?;
        Ok((object_id, source))
    }
}
