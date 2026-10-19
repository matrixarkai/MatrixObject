// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload, ObjectError> {
        validate_upload_id(upload_id)?;
        self.multipart_uploads
            .get(upload_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("multipart upload missing".to_string()))
    }

    pub fn list_multipart_uploads(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MultipartUpload>, ObjectError> {
        self.list_multipart_uploads_by_key(bucket, None, None, marker, limit)
    }

    pub fn list_multipart_uploads_by_key(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MultipartUpload>, ObjectError> {
        if let Some(bucket) = bucket {
            validate_bucket(bucket)?;
        }
        if let Some(prefix) = prefix {
            if prefix.len() > 4096 {
                return Err(ObjectError::InvalidArgument(
                    "multipart upload prefix must be at most 4096 bytes".to_string(),
                ));
            }
        }
        if let Some(key_marker) = key_marker {
            validate_key(key_marker)?;
        }
        if let Some(upload_id_marker) = upload_id_marker {
            validate_upload_id(upload_id_marker)?;
        }
        let take = if limit == 0 { usize::MAX } else { limit };
        if take == usize::MAX {
            let mut uploads: Vec<_> = self
                .multipart_uploads
                .iter()
                .filter(|(_, upload)| {
                    bucket.map_or(true, |bucket| upload.bucket == bucket)
                        && prefix.map_or(true, |prefix| upload.key.starts_with(prefix))
                        && if let Some(key_marker) = key_marker {
                            upload.key.as_str() > key_marker
                                || (upload.key.as_str() == key_marker
                                    && upload_id_marker
                                        .map_or(true, |marker| upload.upload_id.as_str() > marker))
                        } else {
                            upload_id_marker
                                .map_or(true, |marker| upload.upload_id.as_str() > marker)
                        }
                })
                .map(|(_, upload)| upload.clone())
                .collect();
            uploads.sort_by(|left, right| {
                left.key
                    .cmp(&right.key)
                    .then_with(|| left.upload_id.cmp(&right.upload_id))
            });
            return Ok(uploads);
        }
        let mut selected_uploads: BinaryHeap<(String, String)> = BinaryHeap::new();
        for (upload_id, upload) in &self.multipart_uploads {
            let matches = bucket.map_or(true, |bucket| upload.bucket == bucket)
                && prefix.map_or(true, |prefix| upload.key.starts_with(prefix))
                && if let Some(key_marker) = key_marker {
                    upload.key.as_str() > key_marker
                        || (upload.key.as_str() == key_marker
                            && upload_id_marker
                                .map_or(true, |marker| upload.upload_id.as_str() > marker))
                } else {
                    upload_id_marker.map_or(true, |marker| upload.upload_id.as_str() > marker)
                };
            if !matches {
                continue;
            }
            let candidate = (upload.key.clone(), upload_id.clone());
            if selected_uploads.len() < take {
                selected_uploads.push(candidate);
            } else if selected_uploads
                .peek()
                .map_or(false, |largest_selected| candidate < *largest_selected)
            {
                selected_uploads.pop();
                selected_uploads.push(candidate);
            }
        }
        Ok(selected_uploads
            .into_sorted_vec()
            .into_iter()
            .filter_map(|(_, upload_id)| self.multipart_uploads.get(&upload_id).cloned())
            .collect())
    }

    pub fn list_multipart_uploads_page(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MultipartUploadPage, ObjectError> {
        self.list_multipart_uploads_page_by_key(
            bucket,
            None,
            None,
            marker,
            limit,
            max_payload_bytes,
        )
    }

    pub fn list_multipart_uploads_page_by_key(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MultipartUploadPage, ObjectError> {
        self.list_multipart_uploads_hierarchy_page_by_key(
            bucket,
            prefix,
            None,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
        )
    }

    pub fn list_multipart_uploads_hierarchy_page_by_key(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MultipartUploadPage, ObjectError> {
        if let Some(bucket) = bucket {
            validate_bucket(bucket)?;
        }
        if let Some(prefix) = prefix {
            if prefix.len() > 4096 {
                return Err(ObjectError::InvalidArgument(
                    "multipart upload prefix must be at most 4096 bytes".to_string(),
                ));
            }
        }
        if let Some(delimiter) = delimiter {
            if delimiter.is_empty()
                || delimiter.len() > 1024
                || delimiter.contains('\0')
                || delimiter.contains('\n')
            {
                return Err(ObjectError::InvalidArgument(
                    "multipart upload delimiter is invalid".to_string(),
                ));
            }
        }
        if let Some(key_marker) = key_marker {
            validate_key(key_marker)?;
        }
        if let Some(upload_id_marker) = upload_id_marker {
            validate_upload_id(upload_id_marker)?;
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "multipart upload page limit must be positive".to_string(),
            ));
        }
        let window_limit = limit
            .saturating_mul(16)
            .clamp(limit.saturating_add(1), 65_536);
        let mut selected_uploads: BinaryHeap<(String, String)> = BinaryHeap::new();
        let mut overflow_uploads = false;
        let mut scanned_uploads = 0_u64;
        for upload in self.multipart_uploads.values() {
            let matches = bucket.map_or(true, |bucket| upload.bucket == bucket)
                && prefix.map_or(true, |prefix| upload.key.starts_with(prefix))
                && if let Some(key_marker) = key_marker {
                    upload.key.as_str() > key_marker
                        || (upload.key.as_str() == key_marker
                            && upload_id_marker
                                .map_or(true, |marker| upload.upload_id.as_str() > marker))
                } else {
                    upload_id_marker.map_or(true, |marker| upload.upload_id.as_str() > marker)
                };
            if !matches {
                continue;
            }
            scanned_uploads = scanned_uploads.saturating_add(1);
            let candidate = (upload.key.clone(), upload.upload_id.clone());
            if selected_uploads.len() < window_limit {
                selected_uploads.push(candidate);
                continue;
            }
            overflow_uploads = true;
            if selected_uploads
                .peek()
                .map_or(false, |largest_selected| candidate < *largest_selected)
            {
                selected_uploads.pop();
                selected_uploads.push(candidate);
            }
        }
        let uploads: Vec<MultipartUpload> = selected_uploads
            .into_sorted_vec()
            .into_iter()
            .filter_map(|(_, upload_id)| self.multipart_uploads.get(&upload_id).cloned())
            .collect();
        let upload_count = uploads.len();
        let mut page = MultipartUploadPage {
            bucket: bucket.map(str::to_string),
            prefix: prefix.map(str::to_string),
            delimiter: delimiter.map(str::to_string),
            marker: upload_id_marker.map(str::to_string),
            key_marker: key_marker.map(str::to_string),
            upload_id_marker: upload_id_marker.map(str::to_string),
            max_payload_bytes,
            scanned_uploads,
            ..MultipartUploadPage::default()
        };
        let mut emitted_prefixes = BTreeSet::new();
        if let Some(key_marker) = key_marker {
            if let Some(common_prefix) =
                multipart_upload_common_prefix(prefix.unwrap_or(""), delimiter, key_marker)
            {
                emitted_prefixes.insert(common_prefix);
            }
        }
        let mut last_key_marker = key_marker.map(str::to_string);
        let mut last_upload_marker = upload_id_marker.map(str::to_string);
        for (index, upload) in uploads.into_iter().enumerate() {
            let common_prefix =
                multipart_upload_common_prefix(prefix.unwrap_or(""), delimiter, &upload.key);
            let emits_prefix = common_prefix
                .as_ref()
                .map_or(false, |prefix| !emitted_prefixes.contains(prefix));
            let emits_upload = common_prefix.is_none();
            if emits_prefix || emits_upload {
                let emitted_items = page
                    .uploads
                    .len()
                    .saturating_add(page.common_prefixes.len());
                if emitted_items >= limit {
                    page.has_more = true;
                    page.next_marker = last_upload_marker.clone();
                    page.next_key_marker = last_key_marker.clone();
                    break;
                }
            }
            if let Some(common_prefix) = common_prefix {
                emitted_prefixes.insert(common_prefix.clone());
                if emits_prefix {
                    const COMMON_PREFIX_ENTRY_OVERHEAD: u64 = 32;
                    let payload_bytes =
                        COMMON_PREFIX_ENTRY_OVERHEAD.saturating_add(common_prefix.len() as u64);
                    let would_exceed_payload = max_payload_bytes > 0
                        && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
                    if would_exceed_payload && !page.common_prefixes.is_empty() {
                        page.has_more = true;
                        page.next_marker = last_upload_marker.clone();
                        page.next_key_marker = last_key_marker.clone();
                        break;
                    }
                    page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
                    page.emitted_common_prefixes = page.emitted_common_prefixes.saturating_add(1);
                    page.common_prefixes.push(common_prefix);
                    last_key_marker = Some(upload.key.clone());
                    last_upload_marker = Some(upload.upload_id.clone());
                    if would_exceed_payload {
                        page.has_more = overflow_uploads || index + 1 < upload_count;
                        page.next_marker = last_upload_marker.clone();
                        page.next_key_marker = last_key_marker.clone();
                        break;
                    }
                }
                last_key_marker = Some(upload.key.clone());
                last_upload_marker = Some(upload.upload_id.clone());
                continue;
            }
            if page
                .uploads
                .len()
                .saturating_add(page.common_prefixes.len())
                >= limit
            {
                page.has_more = true;
                page.next_marker = last_upload_marker.clone();
                page.next_key_marker = last_key_marker.clone();
                break;
            }
            let payload_bytes = multipart_upload_payload_bytes(&upload);
            let would_exceed_payload = max_payload_bytes > 0
                && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
            if would_exceed_payload && !page.uploads.is_empty() {
                page.has_more = true;
                page.next_marker = last_upload_marker.clone();
                page.next_key_marker = last_key_marker.clone();
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
            page.emitted_uploads = page.emitted_uploads.saturating_add(1);
            last_key_marker = Some(upload.key.clone());
            last_upload_marker = Some(upload.upload_id.clone());
            page.uploads.push(upload);
            if would_exceed_payload {
                page.has_more = overflow_uploads || index + 1 < upload_count;
                page.next_marker = last_upload_marker.clone();
                page.next_key_marker = last_key_marker.clone();
                break;
            }
        }
        if !page.has_more && overflow_uploads {
            page.has_more = true;
            page.next_marker = last_upload_marker;
            page.next_key_marker = last_key_marker;
        }
        Ok(page)
    }

}
