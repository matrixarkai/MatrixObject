impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_abort_multipart_upload(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::AbortMultipartUpload {
            upload_id: upload_id.to_string(),
            update_time_ms,
        })
    }

    pub fn propose_abort_multipart_upload_idempotent(
        &mut self,
        upload_id: &str,
        update_time_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = multipart_abort_request_checksum(upload_id)?;
        if let Some(existing) = self.store.multipart_abort_receipts.get(idempotency_key) {
            if existing.upload_id == upload_id && existing.request_checksum == request_checksum {
                return Ok(ObjectMetadata {
                    object_id: ObjectId::new("_multipart", existing.upload.upload_id.clone()),
                    version: 0,
                    length: existing.upload.total_bytes,
                    etag: String::new(),
                    content_type: "multipart/abort-idempotent-deduped".to_string(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: existing.update_time_ms,
                    extents: Vec::new().into(),
                    deleted: true,
                });
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart abort idempotency key reused with different request".to_string(),
            ));
        }
        self.propose(ObjectCommand::AbortMultipartUploadIdempotent {
            upload_id: upload_id.to_string(),
            update_time_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }
}
