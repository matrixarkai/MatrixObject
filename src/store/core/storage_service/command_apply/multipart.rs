// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    /// The two fences a mutation of an existing upload owes, in the order
    /// `create_multipart_upload` and `put_object` check them.
    ///
    /// # WHY THE UPLOAD CARRIES THE CREDENTIALS
    ///
    /// The lease owner and epoch are the ones recorded on the upload when it was created,
    /// not any the caller passes now -- an upload created under a lease keeps working for
    /// the writer that holds it, and a second writer arriving with no credentials is
    /// refused. `complete_multipart_upload` already behaves this way, because the put it
    /// performs carries the upload's own options; this makes the other two agree.
    ///
    /// The consequence worth naming: an upload created with NO credentials cannot be
    /// continued or aborted while a lease covers its key, until that lease expires. That is
    /// exactly what `complete` already did, and the alternative -- letting a stranger
    /// abort it -- is what this closes.
    fn check_multipart_upload_fences(
        &self,
        upload_id: &str,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        let upload = self.multipart_upload(upload_id)?;
        let meta_key = meta_object_key(&ObjectId::new(
            upload.bucket.clone(),
            upload.key.clone(),
        ));
        self.meta_service.check_shard_fence(&meta_key)?;
        self.meta_service.check_lease_fence(
            &meta_key,
            upload.options.lease_owner.as_deref(),
            upload.options.lease_epoch,
            now_ms,
        )
    }

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
                let (lease, _) = self.renew_placement_leader_idempotent(
                    &node_id,
                    epoch,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(placement_leader_lease_metadata(
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "multipart/create".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: upload.update_time_ms,
                    extents: Vec::new().into(),
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "multipart/create-idempotent".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: upload.update_time_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::UploadPart {
                upload_id,
                part_number,
                data,
                update_time_ms,
            } => {
                self.check_multipart_upload_fences(&upload_id, update_time_ms)?;
                let part =
                    self.apply_upload_part(upload_id.clone(), part_number, data, update_time_ms)?;
                let (etag_digest_bits, has_etag) =
                    etag_digest_fields(change_etag_digest(&part.etag).unwrap_or(None));
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload_id),
                    version: part.part_number as u64,
                    length: part.length,
                    etag_digest_bits,
                    has_etag,
                    content_type: "multipart/part".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: part.update_time_ms,
                    extents: Vec::new().into(),
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
                self.check_multipart_upload_fences(&upload_id, update_time_ms)?;
                let part = self.apply_upload_part_idempotent(
                    upload_id.clone(),
                    part_number,
                    data,
                    update_time_ms,
                    idempotency_key,
                )?;
                let (etag_digest_bits, has_etag) =
                    etag_digest_fields(change_etag_digest(&part.etag).unwrap_or(None));
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload_id),
                    version: part.part_number as u64,
                    length: part.length,
                    etag_digest_bits,
                    has_etag,
                    content_type: "multipart/part-idempotent".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: part.update_time_ms,
                    extents: Vec::new().into(),
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
            } => self.apply_complete_multipart_upload_with_placement(
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
                self.check_multipart_upload_fences(&upload_id, update_time_ms)?;
                let upload = self.apply_abort_multipart_upload(upload_id)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", upload.upload_id),
                    version: 0,
                    length: upload.total_bytes,
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "multipart/abort".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms,
                    extents: Vec::new().into(),
                    deleted: true,
                })
            }
            _ => unreachable!("command classified outside Multipart"),
        }
    }
}
