impl MatrixObjectStore {
    fn apply_chunk_maintenance_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ApplyDiscard { chunk_id, ranges } => {
                self.apply_discard(&chunk_id, ranges)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", chunk_id.volume_id, chunk_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new().into(),
                    content_type: String::new().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_millis(),
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyPreparedChunkDiscard { prepared } => {
                let chunk_id = prepared.chunk_id.clone();
                self.apply_prepared_chunk_discard(prepared)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", chunk_id.volume_id, chunk_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new().into(),
                    content_type: "segment/prepared-discard".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_millis(),
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::CompactChunk {
                chunk_id,
                update_time_ms,
            } => {
                self.compact_chunk_at(&chunk_id, update_time_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", chunk_id.volume_id, chunk_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new().into(),
                    content_type: "segment/compaction".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::CompactAll {
                min_stale_bytes,
                update_time_ms,
                expected_page_route_control_checksum,
                expected_object_gc_report_proof_checksum,
                expected_full_compaction_plan_checksum,
            } => {
                self.chunk_service.check_expected_route_control_checksum(
                    if expected_page_route_control_checksum.is_empty() {
                        None
                    } else {
                        Some(expected_page_route_control_checksum.as_str())
                    },
                )?;
                if !expected_object_gc_report_proof_checksum.is_empty() {
                    self.gc_report_with_discard_policy_proof_fenced(
                        min_stale_bytes,
                        0,
                        if expected_page_route_control_checksum.is_empty() {
                            None
                        } else {
                            Some(expected_page_route_control_checksum.as_str())
                        },
                        &expected_object_gc_report_proof_checksum,
                    )?;
                }
                self.plan_full_compaction_fenced(
                    min_stale_bytes,
                    if expected_page_route_control_checksum.is_empty() {
                        None
                    } else {
                        Some(expected_page_route_control_checksum.as_str())
                    },
                    &expected_object_gc_report_proof_checksum,
                )?
                .verify_expected_proof_checksum(&expected_full_compaction_plan_checksum)?;
                let compacted_chunks = self.compact_all_at(min_stale_bytes, update_time_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_compact", "all"),
                    version: 0,
                    length: compacted_chunks,
                    etag: String::new().into(),
                    content_type: "segment/full-compaction".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyPreparedChunkCompaction { prepared } => {
                let chunk_id = prepared.chunk_id.clone();
                let update_time_ms = prepared.update_time_ms;
                self.apply_prepared_chunk_compaction(prepared)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", chunk_id.volume_id, chunk_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new().into(),
                    content_type: "segment/prepared-compaction".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            _ => unreachable!("command classified outside SegmentMaintenance"),
        }
    }
}
