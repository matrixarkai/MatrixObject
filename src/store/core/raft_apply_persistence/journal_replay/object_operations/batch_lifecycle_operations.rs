// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_persistent_journal_batch_lifecycle_operation(
        &mut self,
        op: u64,
        mut cursor: &mut SnapshotByteCursor<'_>,
    ) -> Result<bool, ObjectError> {
        match op {
            18 => {
                let max_items = cursor.read_count("persistent journal batch put max_items")?;
                let max_payload_bytes = cursor.read_u64()?;
                let update_time_ms = cursor.read_u64()?;
                let replica_count = read_journal_optional_replica_count(
                    &mut cursor,
                    "persistent journal batch put root replica count",
                )?;
                let request_count =
                    cursor.read_count("persistent journal batch put request count")?;
                let mut requests = Vec::with_capacity(request_count);
                for _ in 0..request_count {
                    requests.push(read_object_put_request(&mut cursor)?);
                }
                if let Some(replica_count) = replica_count {
                    self.batch_put_objects_with_root_placement_at(
                        requests,
                        max_items,
                        max_payload_bytes,
                        update_time_ms,
                        replica_count,
                        None,
                    )?;
                } else {
                    self.batch_put_objects_at(
                        requests,
                        max_items,
                        max_payload_bytes,
                        update_time_ms,
                        None,
                    )?;
                }
            }
            19 => {
                let source_bucket = cursor.read_string()?;
                let source_key = cursor.read_string()?;
                let source_version = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_u64()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal copy source-version presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let destination_bucket = cursor.read_string()?;
                let destination_key = cursor.read_string()?;
                let content_type = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_string()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal copy content-type presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let update_time_ms = cursor.read_u64()?;
                let options = read_journal_put_options(&mut cursor)?;
                let replica_count = read_journal_optional_replica_count(
                    &mut cursor,
                    "persistent journal copy root replica count",
                )?;
                if let Some(replica_count) = replica_count {
                    self.apply_copy_with_root_placement(
                        source_bucket,
                        source_key,
                        source_version,
                        destination_bucket,
                        destination_key,
                        content_type,
                        update_time_ms,
                        options,
                        replica_count,
                    )?;
                } else {
                    self.apply_copy(
                        source_bucket,
                        source_key,
                        source_version,
                        destination_bucket,
                        destination_key,
                        content_type,
                        update_time_ms,
                        options,
                    )?;
                }
            }
            20 => {
                let max_items = cursor.read_count("persistent journal batch copy max_items")?;
                let max_copied_bytes = cursor.read_u64()?;
                let update_time_ms = cursor.read_u64()?;
                let replica_count = read_journal_optional_replica_count(
                    &mut cursor,
                    "persistent journal batch copy root replica count",
                )?;
                let request_count =
                    cursor.read_count("persistent journal batch copy request count")?;
                let mut requests = Vec::with_capacity(request_count);
                for _ in 0..request_count {
                    requests.push(read_object_copy_request(&mut cursor)?);
                }
                if let Some(replica_count) = replica_count {
                    self.batch_copy_objects_with_root_placement_fenced_internal(
                        requests,
                        max_items,
                        max_copied_bytes,
                        replica_count,
                        update_time_ms,
                        None,
                        None,
                        None,
                    )?;
                } else {
                    self.batch_copy_objects_fenced_internal(
                        requests,
                        max_items,
                        max_copied_bytes,
                        update_time_ms,
                        None,
                        None,
                        None,
                    )?;
                }
            }
            21 => {
                let max_items = cursor.read_count("persistent journal batch delete max_items")?;
                let release_root_placements = cursor.read_bool()?;
                let update_time_ms = cursor.read_u64()?;
                let request_count =
                    cursor.read_count("persistent journal batch delete request count")?;
                let mut requests = Vec::with_capacity(request_count);
                for _ in 0..request_count {
                    requests.push(read_object_delete_request(&mut cursor)?);
                }
                self.batch_delete_objects_fenced(
                    requests,
                    max_items,
                    release_root_placements,
                    update_time_ms,
                    None,
                )?;
            }
            22 => {
                let max_actions = cursor.read_count("persistent journal lifecycle max_actions")?;
                let now_ms = cursor.read_u64()?;
                let transitions_only = cursor.read_bool()?;
                if transitions_only {
                    self.apply_bucket_lifecycle_transitions_at(max_actions, now_ms)?;
                } else {
                    self.apply_bucket_lifecycle_policies_at(max_actions, now_ms)?;
                }
            }
            23 => {
                let bucket = cursor.read_string()?;
                let noncurrent_versions_to_keep =
                    cursor.read_count("persistent journal legacy lifecycle retention count")?;
                let now_ms = cursor.read_u64()?;
                self.apply_lifecycle_at(&bucket, noncurrent_versions_to_keep, now_ms)?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
