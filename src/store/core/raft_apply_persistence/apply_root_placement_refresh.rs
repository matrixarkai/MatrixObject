// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_refresh_object_placement(
        &mut self,
        bucket: String,
        key: String,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.apply_refresh_object_placement_inner(bucket, key, replica_count, update_time_ms)
            .map(|(metadata, _)| metadata)
    }

    fn apply_refresh_object_placement_inner(
        &mut self,
        bucket: String,
        key: String,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<(ObjectMetadata, bool), ObjectError> {
        validate_bucket(&bucket)?;
        validate_key(&key)?;
        let object_id = ObjectId::new(bucket, key);
        let meta_key = meta_object_key(&object_id);
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service
            .check_lease_fence(&meta_key, None, None, update_time_ms)?;
        let metadata = self
            .object(&object_id)
            .filter(|metadata| !metadata.deleted)
            .cloned()
            .ok_or_else(|| {
                ObjectError::NotFound(format!("{}/{}", object_id.bucket, object_id.key))
            })?;
        let plan = self.meta_service.plan_object_placement(
            object_id.clone(),
            metadata.length,
            replica_count,
        )?;
        let desired = ObjectPlacement {
            object_id: metadata.object_id.clone(),
            version: metadata.version,
            length: metadata.length,
            node_ids: plan.node_ids,
            update_time_ms,
        };
        if self
            .meta_service
            .object_placement(&metadata.object_id)
            .map_or(false, |current| {
                current.version == desired.version
                    && current.length == desired.length
                    && current.node_ids == desired.node_ids
            })
        {
            return Ok((metadata, false));
        }
        // Captured here, not on entry: everything above only reads -- both fence
        // checks, `plan_object_placement` and `object_placement` all take `&self` --
        // and the comparison just above returns Ok when the placement already
        // matches. Capturing on entry made that successful no-op, and every
        // rejected refresh, copy every map in the store and discard the copy.
        let rollback = self.capture_object_mutation_rollback();
        if let Err(err) = self
            .meta_service
            .commit_object_placement(desired, &plan.plan_checksum)
        {
            self.restore_object_mutation_rollback(rollback);
            return Err(err);
        }
        self.append_object_change("refresh_object_root_placement", &metadata);
        Ok((metadata, true))
    }

    pub(crate) fn apply_refresh_object_placements_page(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<PlacementRefreshReport, ObjectError> {
        let rollback = self.capture_object_mutation_rollback();
        let limit = if max_objects == 0 {
            usize::MAX
        } else {
            max_objects
        };
        let object_ids = self
            .objects_iter()
            .filter(|(object_id, metadata)| {
                !metadata.deleted && marker.as_ref().map_or(true, |marker| *object_id > marker)
            })
            .map(|(object_id, _)| object_id.clone())
            .take(limit)
            .collect::<Vec<_>>();
        let mut report = PlacementRefreshReport {
            marker,
            scanned_objects: object_ids.len() as u64,
            ..PlacementRefreshReport::default()
        };
        for object_id in object_ids.iter().cloned() {
            match self.apply_refresh_object_placement_inner(
                object_id.bucket.to_string(),
                object_id.key.to_string(),
                replica_count,
                update_time_ms,
            ) {
                Ok((_, changed)) => {
                    if changed {
                        report.refreshed_objects = report.refreshed_objects.saturating_add(1);
                    }
                }
                Err(err) => {
                    self.restore_object_mutation_rollback(rollback);
                    return Err(err);
                }
            }
        }
        report.next_marker = object_ids.last().cloned();
        report.has_more = limit != usize::MAX
            && object_ids.len() == limit
            && report.next_marker.as_ref().map_or(false, |marker| {
                self.objects_iter()
                    .any(|(object_id, metadata)| !metadata.deleted && object_id > marker)
            });
        Ok(report)
    }

}
