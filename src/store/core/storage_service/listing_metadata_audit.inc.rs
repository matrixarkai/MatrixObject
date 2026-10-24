// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn audit_object_metadata(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<ObjectMetadataAudit, ObjectError> {
        self.audit_object_metadata_route_fenced(
            bucket,
            prefix,
            marker,
            limit,
            max_payload_bytes,
            options,
            None,
            None,
        )
    }

    pub fn audit_object_metadata_route_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
    ) -> Result<ObjectMetadataAudit, ObjectError> {
        check_expected_metadata_shard_topology_fence(
            "AuditObjectMetadata",
            &self.meta_service,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )?;
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let limit = limit.max(1);
        let meta_prefix = format!("object/{bucket}/");
        let read_key = if prefix.is_empty() {
            meta_prefix.clone()
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let readiness = self.prepare_metadata_read(&read_key, &options)?;
        let projection_prefix = if prefix.is_empty() {
            meta_prefix.clone()
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let mut audit = ObjectMetadataAudit {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            marker: marker.map(ToString::to_string),
            max_payload_bytes,
            readiness,
            expected_metadata_shard_topology_epoch: expected_shard_topology_epoch,
            expected_metadata_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            ..ObjectMetadataAudit::default()
        };
        let mut audited_ids = BTreeSet::new();

        let emit_issue = |audit: &mut ObjectMetadataAudit,
                          object_id: ObjectId,
                          issue: String,
                          projection: Option<MetadataRecord>,
                          metadata: Option<ObjectMetadata>|
         -> Result<(), ObjectError> {
            let payload = projection
                .as_ref()
                .map_or(0, metadata_record_payload_bytes)
                .saturating_add(metadata.as_ref().map_or(0, object_metadata_payload_bytes))
                .saturating_add(object_id.bucket.len() as u64)
                .saturating_add(object_id.key.len() as u64)
                .saturating_add(issue.len() as u64);
            if max_payload_bytes > 0
                && audit.payload_bytes.saturating_add(payload) > max_payload_bytes
            {
                if audit.items.is_empty() {
                    return Err(ObjectError::InvalidArgument(
                        "object metadata audit first issue exceeds max_payload_bytes".to_string(),
                    ));
                }
                audit.has_more = true;
                audit.next_marker = Some(object_id.key.to_string());
                return Ok(());
            }
            audit.payload_bytes = audit.payload_bytes.saturating_add(payload);
            audit.issue_count = audit.issue_count.saturating_add(1);
            audit.items.push(ObjectMetadataAuditItem {
                object_id,
                issue,
                projection,
                metadata,
            });
            Ok(())
        };

        for (record_key, projection) in self
                .meta_service
                .records_range((
                std::ops::Bound::Included(projection_prefix.as_str()),
                std::ops::Bound::Unbounded,
            ))
        {
            if !record_key.starts_with(&meta_prefix) {
                break;
            }
            let object_key = record_key[meta_prefix.len()..].to_string();
            if !object_key.starts_with(prefix) {
                break;
            }
            if marker.map_or(false, |m| object_key.as_str() <= m) {
                continue;
            }
            audit.scanned_projection_records = audit.scanned_projection_records.saturating_add(1);
            audit.audited_objects = audit.audited_objects.saturating_add(1);
            let object_id = ObjectId::new(bucket, object_key.clone());
            audited_ids.insert(object_id.clone());
            let metadata = self.objects.get(&object_id).cloned();
            let mut issues = Vec::new();
            if projection.deleted {
                audit.deleted_projection_count = audit.deleted_projection_count.saturating_add(1);
                issues.push("deleted_projection");
            } else if projection.value.is_empty() {
                audit.empty_projection_count = audit.empty_projection_count.saturating_add(1);
                issues.push("empty_projection");
            }
            match metadata.as_ref() {
                Some(current) if !current.deleted => {
                    let expected = metadata_projection_value(current);
                    if !projection.deleted && projection.value != expected.into() {
                        audit.stale_projection_count =
                            audit.stale_projection_count.saturating_add(1);
                        issues.push("projection_payload_mismatch");
                    }
                }
                _ => {
                    audit.missing_object_count = audit.missing_object_count.saturating_add(1);
                    issues.push("missing_object");
                }
            }
            if !issues.is_empty() {
                emit_issue(
                    &mut audit,
                    object_id,
                    issues.join(","),
                    Some(projection.clone()),
                    metadata,
                )?;
                if audit.has_more || audit.items.len() >= limit {
                    audit.has_more = true;
                    audit.next_marker = Some(object_key);
                    return Ok(audit);
                }
            }
        }

        for (object_id, metadata) in &self.objects {
            if object_id.bucket != bucket || metadata.deleted {
                continue;
            }
            if !object_id.key.starts_with(prefix) {
                continue;
            }
            if marker.map_or(false, |m| object_id.key.as_str() <= m) {
                continue;
            }
            audit.scanned_objects = audit.scanned_objects.saturating_add(1);
            if audited_ids.contains(object_id) {
                continue;
            }
            audit.audited_objects = audit.audited_objects.saturating_add(1);
            let projection_key = meta_object_key(object_id);
            let projection = self
                .meta_service
                .record(projection_key.as_str())
                .cloned();
            if projection.as_ref().map_or(true, |record| record.deleted) {
                audit.missing_projection_count = audit.missing_projection_count.saturating_add(1);
                emit_issue(
                    &mut audit,
                    object_id.clone(),
                    "missing_projection".to_string(),
                    projection,
                    Some(metadata.clone()),
                )?;
                if audit.has_more || audit.items.len() >= limit {
                    audit.has_more = true;
                    audit.next_marker = Some(object_id.key.to_string());
                    return Ok(audit);
                }
            }
        }
        Ok(audit)
    }

}
