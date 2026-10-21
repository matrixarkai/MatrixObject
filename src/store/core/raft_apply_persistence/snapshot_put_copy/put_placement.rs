// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_put_with_root_placement(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
        content_type: String,
        update_time_ms: u64,
        options: PutOptions,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let rollback = self.capture_object_mutation_rollback();
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        let object_id = ObjectId::new(bucket.clone(), key.clone());
        let plan = self.meta_service.plan_object_placement(
            object_id.clone(),
            data.len() as u64,
            replica_count,
        )?;
        let previous = self
            .objects
            .get(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned();
        let metadata = match self.apply_put_storage_batched(
            bucket,
            key,
            data,
            content_type,
            update_time_ms,
            options,
            "put_object_with_root_placement",
        ) {
            Ok(metadata) => metadata,
            Err(err) => {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        };
        let placement = ObjectPlacement {
            object_id: metadata.object_id.clone(),
            version: metadata.version,
            length: metadata.length,
            node_ids: plan.node_ids,
            update_time_ms,
        };
        let committed = match self
            .meta_service
            .commit_object_placement(placement, &plan.plan_checksum)
        {
            Ok(committed) => committed,
            Err(err) => {
                self.restore_object_mutation_rollback(rollback);
                return Err(err);
            }
        };
        if previous.as_ref().map_or(false, |old| {
            !self.bucket_versioning.contains(old.object_id.bucket.as_str())
        }) {
            self.release_root_placement_if_present(&metadata.object_id, Some(&committed));
        }
        Ok(metadata)
    }

}
