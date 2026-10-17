impl MatrixObjectStore {
    fn apply_abort_multipart_upload(
        &mut self,
        upload_id: String,
    ) -> Result<MultipartUpload, ObjectError> {
        validate_upload_id(&upload_id)?;
        self.multipart_uploads
            .remove(&upload_id)
            .ok_or_else(|| ObjectError::NotFound("multipart upload missing".to_string()))
    }

    fn apply_abort_multipart_upload_idempotent(
        &mut self,
        upload_id: String,
        update_time_ms: u64,
        idempotency_key: String,
        commit_index: u64,
    ) -> Result<MultipartUpload, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        let request_checksum = multipart_abort_request_checksum(&upload_id)?;
        if let Some(existing) = self.multipart_abort_receipts.get(&idempotency_key) {
            if existing.upload_id == upload_id && existing.request_checksum == request_checksum {
                return Ok(existing.upload.clone());
            }
            return Err(ObjectError::PreconditionFailed(
                "multipart abort idempotency key reused with different request".to_string(),
            ));
        }
        let upload = self.apply_abort_multipart_upload(upload_id.clone())?;
        self.multipart_abort_receipts.insert(
            idempotency_key.clone(),
            MultipartAbortReceipt {
                idempotency_key,
                upload_id,
                request_checksum,
                upload: upload.clone(),
                commit_index,
                update_time_ms,
            },
        );
        Ok(upload)
    }

    pub fn trim_multipart_receipts(
        &mut self,
        retain_complete_receipts: usize,
        retain_abort_receipts: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let trimmed_complete = trim_multipart_complete_receipts(
            &mut self.multipart_complete_receipts,
            retain_complete_receipts,
        );
        let trimmed_abort = trim_multipart_abort_receipts(
            &mut self.multipart_abort_receipts,
            retain_abort_receipts,
        );
        Ok(metadata_control_metadata(
            "_multipartreceipts",
            "trim",
            trimmed_complete as u64,
            trimmed_abort as u64,
            "multipart/receipt-retention-trim",
            update_time_ms,
        ))
    }

}
