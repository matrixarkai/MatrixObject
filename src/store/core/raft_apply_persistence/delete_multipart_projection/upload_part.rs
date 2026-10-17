impl MatrixObjectStore {
    fn apply_upload_part(
        &mut self,
        upload_id: String,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
    ) -> Result<MultipartPart, ObjectError> {
        self.apply_upload_part_with_idempotency(upload_id, part_number, data, update_time_ms, None)
    }

    fn apply_upload_part_idempotent(
        &mut self,
        upload_id: String,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        idempotency_key: String,
    ) -> Result<MultipartPart, ObjectError> {
        validate_idempotency_key(&idempotency_key)?;
        self.apply_upload_part_with_idempotency(
            upload_id,
            part_number,
            data,
            update_time_ms,
            Some(idempotency_key),
        )
    }

    fn apply_upload_part_with_idempotency(
        &mut self,
        upload_id: String,
        part_number: u32,
        data: Vec<u8>,
        update_time_ms: u64,
        idempotency_key: Option<String>,
    ) -> Result<MultipartPart, ObjectError> {
        validate_upload_id(&upload_id)?;
        if part_number == 0 || part_number > k_max_multipart_part_number() {
            return Err(ObjectError::InvalidArgument(
                "multipart part number must be 1..10000".to_string(),
            ));
        }
        let upload = self
            .multipart_uploads
            .get_mut(&upload_id)
            .ok_or_else(|| ObjectError::NotFound("multipart upload missing".to_string()))?;
        if let Some(key) = idempotency_key.as_deref() {
            if upload.parts.iter().any(|(number, part)| {
                *number != part_number && part.idempotency_key.as_deref() == Some(key)
            }) {
                return Err(ObjectError::PreconditionFailed(
                    "multipart part idempotency key reused for a different part number".to_string(),
                ));
            }
        }
        let checksum = etag(&data);
        if let Some(existing) = upload.parts.get(&part_number) {
            let same_payload = upload
                .part_payloads
                .get(&part_number)
                .map_or(false, |payload| payload == &data);
            if idempotency_key.is_some() && existing.idempotency_key != idempotency_key {
                return Err(ObjectError::PreconditionFailed(
                    "multipart part idempotency key reused with different part intent".to_string(),
                ));
            }
            if same_payload {
                return Ok(existing.clone());
            }
            if idempotency_key.is_some() {
                return Err(ObjectError::PreconditionFailed(
                    "multipart part idempotent retry payload mismatch".to_string(),
                ));
            }
            upload.total_bytes = upload.total_bytes.saturating_sub(existing.length);
        }
        let part = MultipartPart {
            part_number,
            length: data.len() as u64,
            etag: checksum,
            update_time_ms,
            idempotency_key,
        };
        upload.total_bytes = upload.total_bytes.checked_add(part.length).ok_or_else(|| {
            ObjectError::InvalidArgument("multipart upload too large".to_string())
        })?;
        upload.next_part_number = upload
            .next_part_number
            .max(part_number.saturating_add(1).max(1));
        upload.update_time_ms = update_time_ms;
        upload.part_payloads.insert(part_number, data);
        upload.parts.insert(part_number, part.clone());
        Ok(part)
    }

}
