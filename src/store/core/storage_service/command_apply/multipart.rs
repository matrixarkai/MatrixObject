impl MatrixObjectStore {
    fn apply_multipart_command(
        &mut self,
        command: ObjectCommand,
        raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::RenewRootLeaderIdempotent {
                node_id,
                epoch,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let (lease, _) = self.renew_root_leader_idempotent(
                    &node_id,
                    epoch,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(root_leader_lease_metadata(
                    &lease,
                    "rootserver/leader-renew-idempotent",
                ))
            }
            ObjectCommand::CreateMultipartUpload {
                upload_id,
                bucket,
                key,
                content_type,
                options,
                create_time_ms,
            } => {
                let upload = self.apply_create_multipart_upload(
                    upload_id,
                    bucket,
                    key,
                    content_type,
                    options,
                    create_time_ms,
                )?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload.upload_id),
                    version: 0,
                    length: upload.total_bytes,
                    etag: String::new(),
                    content_type: "multipart/create".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: upload.update_time_ms,
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::CreateMultipartUploadIdempotent {
                upload_id,
                bucket,
                key,
                content_type,
                options,
                create_time_ms,
                idempotency_key,
            } => {
                let upload = self.apply_create_multipart_upload_idempotent(
                    upload_id,
                    bucket,
                    key,
                    content_type,
                    options,
                    create_time_ms,
                    idempotency_key,
                )?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload.upload_id),
                    version: 0,
                    length: upload.total_bytes,
                    etag: String::new(),
                    content_type: "multipart/create-idempotent".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: upload.update_time_ms,
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::UploadPart {
                upload_id,
                part_number,
                data,
                update_time_ms,
            } => {
                let part =
                    self.apply_upload_part(upload_id.clone(), part_number, data, update_time_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload_id),
                    version: part.part_number as u64,
                    length: part.length,
                    etag: part.etag,
                    content_type: "multipart/part".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: part.update_time_ms,
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::UploadPartIdempotent {
                upload_id,
                part_number,
                data,
                update_time_ms,
                idempotency_key,
            } => {
                let part = self.apply_upload_part_idempotent(
                    upload_id.clone(),
                    part_number,
                    data,
                    update_time_ms,
                    idempotency_key,
                )?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload_id),
                    version: part.part_number as u64,
                    length: part.length,
                    etag: part.etag,
                    content_type: "multipart/part-idempotent".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: part.update_time_ms,
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::CompleteMultipartUpload {
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                update_time_ms,
            } => self.apply_complete_multipart_upload(
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                update_time_ms,
            ),
            ObjectCommand::CompleteMultipartUploadIdempotent {
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                update_time_ms,
                idempotency_key,
            } => self.apply_complete_multipart_upload_idempotent(
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                update_time_ms,
                idempotency_key,
                raft_entry_index.unwrap_or(0),
            ),
            ObjectCommand::CompleteMultipartUploadWithRootPlacement {
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                replica_count,
                update_time_ms,
            } => self.apply_complete_multipart_upload_with_root_placement(
                upload_id,
                expected_size,
                expected_etag,
                expected_checksum,
                replica_count,
                update_time_ms,
            ),
            ObjectCommand::AbortMultipartUpload {
                upload_id,
                update_time_ms,
            } => {
                let upload = self.apply_abort_multipart_upload(upload_id)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload.upload_id),
                    version: 0,
                    length: upload.total_bytes,
                    etag: String::new(),
                    content_type: "multipart/abort".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms,
                    extents: Vec::new(),
                    deleted: true,
                })
            }
            _ => unreachable!("command classified outside Multipart"),
        }
    }
}
