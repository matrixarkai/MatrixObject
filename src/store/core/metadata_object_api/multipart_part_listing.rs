impl MatrixObjectStore {
    pub fn list_multipart_parts(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
    ) -> Result<Vec<MultipartPart>, ObjectError> {
        let upload = self.multipart_upload(upload_id)?;
        let take = if limit == 0 { usize::MAX } else { limit };
        let Some(start_part) = marker.checked_add(1) else {
            return Ok(Vec::new());
        };
        let mut parts = Vec::with_capacity(take.min(upload.parts.len()));
        parts.extend(
            upload
                .parts
                .range(start_part..)
                .map(|(_, part)| part.clone())
                .take(take),
        );
        Ok(parts)
    }

    pub fn list_multipart_parts_page(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MultipartPartPage, ObjectError> {
        let upload = self.multipart_upload(upload_id)?;
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "multipart part page limit must be positive".to_string(),
            ));
        }
        let Some(start_part) = marker.checked_add(1) else {
            return Ok(MultipartPartPage {
                upload_id: upload_id.to_string(),
                marker,
                max_payload_bytes,
                ..MultipartPartPage::default()
            });
        };
        let part_count = upload.parts.range(start_part..).count();
        let mut page = MultipartPartPage {
            upload_id: upload_id.to_string(),
            marker,
            max_payload_bytes,
            scanned_parts: part_count as u64,
            ..MultipartPartPage::default()
        };
        page.parts.reserve(limit.min(part_count));
        for (index, (_, part)) in upload.parts.range(start_part..).enumerate() {
            if page.parts.len() >= limit {
                page.has_more = true;
                page.next_part_marker = page.parts.last().map(|part| part.part_number);
                break;
            }
            let payload_bytes = multipart_part_payload_bytes(&upload, &part);
            let would_exceed_payload = max_payload_bytes > 0
                && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
            if would_exceed_payload && !page.parts.is_empty() {
                page.has_more = true;
                page.next_part_marker = page.parts.last().map(|part| part.part_number);
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
            page.emitted_parts = page.emitted_parts.saturating_add(1);
            page.parts.push(part.clone());
            if would_exceed_payload {
                page.has_more = index + 1 < part_count;
                page.next_part_marker = page.parts.last().map(|part| part.part_number);
                break;
            }
        }
        Ok(page)
    }

    pub fn list_multipart_parts_with_metadata_read(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<(Vec<MultipartPart>, MetadataRaftReadiness), ObjectError> {
        validate_upload_id(upload_id)?;
        let object_id = ObjectId::new("_multipart", upload_id);
        let readiness = self.prepare_metadata_read(&meta_object_key(&object_id), &options)?;
        let parts = self.list_multipart_parts(upload_id, marker, limit)?;
        Ok((parts, readiness))
    }

    pub fn list_multipart_parts_page_with_metadata_read(
        &self,
        upload_id: &str,
        marker: u32,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(MultipartPartPage, MetadataRaftReadiness), ObjectError> {
        validate_upload_id(upload_id)?;
        let object_id = ObjectId::new("_multipart", upload_id);
        let readiness = self.prepare_metadata_read(&meta_object_key(&object_id), &options)?;
        let mut page =
            self.list_multipart_parts_page(upload_id, marker, limit, max_payload_bytes)?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        Ok((page, readiness))
    }
}
