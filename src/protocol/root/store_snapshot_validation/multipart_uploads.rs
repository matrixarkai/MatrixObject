impl StoreSnapshot {
    fn validate_multipart_uploads(&self) -> Result<(), ObjectError> {
        for (upload_id, upload) in &self.multipart_uploads {
            validate_upload_id(upload_id)?;
            if &upload.upload_id != upload_id {
                return Err(ObjectError::Corruption(
                    "snapshot multipart upload key mismatch".to_string(),
                ));
            }
            validate_bucket(&upload.bucket)?;
            validate_key(&upload.key)?;
            if upload.next_part_number == 0 || upload.parts.len() != upload.part_payloads.len() {
                return Err(ObjectError::Corruption(
                    "snapshot multipart upload part index is invalid".to_string(),
                ));
            }
            let mut total_bytes = 0u64;
            let mut expected_next = 1u32;
            for (part_number, part) in &upload.parts {
                if *part_number == 0
                    || *part_number > k_max_multipart_part_number()
                    || part.part_number != *part_number
                {
                    return Err(ObjectError::Corruption(
                        "snapshot multipart part number is invalid".to_string(),
                    ));
                }
                expected_next = expected_next.max(part_number.saturating_add(1));
                let payload = upload.part_payloads.get(part_number).ok_or_else(|| {
                    ObjectError::Corruption("snapshot multipart part payload missing".to_string())
                })?;
                if payload.len() as u64 != part.length || !etag_matches(payload, &part.etag) {
                    return Err(ObjectError::Corruption(
                        "snapshot multipart part payload checksum mismatch".to_string(),
                    ));
                }
                total_bytes = total_bytes.checked_add(part.length).ok_or_else(|| {
                    ObjectError::Corruption("snapshot multipart length overflows".to_string())
                })?;
            }
            if total_bytes != upload.total_bytes || upload.next_part_number < expected_next {
                return Err(ObjectError::Corruption(
                    "snapshot multipart upload accounting mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }
}
