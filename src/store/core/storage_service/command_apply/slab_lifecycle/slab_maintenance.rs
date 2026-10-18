impl MatrixObjectStore {
    fn apply_slab_maintenance_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ApplyDiscard { slab_id, ranges } => {
                self.apply_discard(&slab_id, ranges)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", slab_id.volume_id, slab_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new(),
                    content_type: String::new(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_millis(),
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyPreparedSlabDiscard { prepared } => {
                let slab_id = prepared.slab_id.clone();
                self.apply_prepared_slab_discard(prepared)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", slab_id.volume_id, slab_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new(),
                    content_type: "segment/prepared-discard".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_millis(),
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::CompactSlab {
                slab_id,
                update_time_ms,
            } => {
                self.compact_slab_at(&slab_id, update_time_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", slab_id.volume_id, slab_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new(),
                    content_type: "segment/compaction".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
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
                self.page_service.check_expected_route_control_checksum(
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
                let compacted_slabs = self.compact_all_at(min_stale_bytes, update_time_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_compact", "all"),
                    version: 0,
                    length: compacted_slabs,
                    etag: String::new(),
                    content_type: "segment/full-compaction".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyPreparedSlabCompaction { prepared } => {
                let slab_id = prepared.slab_id.clone();
                let update_time_ms = prepared.update_time_ms;
                self.apply_prepared_slab_compaction(prepared)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(
                        "_segment",
                        format!("{}:{}", slab_id.volume_id, slab_id.id),
                    ),
                    version: 0,
                    length: 0,
                    etag: String::new(),
                    content_type: "segment/prepared-compaction".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            _ => unreachable!("command classified outside SegmentMaintenance"),
        }
    }
}
