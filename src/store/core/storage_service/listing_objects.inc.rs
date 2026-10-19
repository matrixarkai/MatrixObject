// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ObjectError> {
        validate_bucket(bucket)?;
        let limit = limit.max(1);
        let mut out = Vec::with_capacity(limit.min(self.objects.len()));
        let start_key = marker
            .filter(|marker| prefix.is_empty() || *marker >= prefix)
            .unwrap_or(prefix);
        for (object_id, metadata) in self.objects.range(ObjectId::new(bucket, start_key)..) {
            if object_id.bucket != bucket {
                break;
            }
            if !object_id.key.starts_with(prefix) {
                break;
            }
            if metadata.deleted {
                continue;
            }
            if marker.map_or(false, |m| object_id.key.as_str() <= m) {
                continue;
            }
            out.push(metadata.clone());
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectListPage, ObjectError> {
        self.list_objects_hierarchy_page(bucket, prefix, None, marker, limit, max_payload_bytes)
    }

    pub fn list_objects_hierarchy_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectListPage, ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(delimiter) = delimiter {
            if delimiter.is_empty()
                || delimiter.len() > 1024
                || delimiter.contains('\0')
                || delimiter.contains('\n')
            {
                return Err(ObjectError::InvalidArgument(
                    "object list delimiter is invalid".to_string(),
                ));
            }
        }
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "object list page limit must be positive".to_string(),
            ));
        }
        let mut page = ObjectListPage {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            delimiter: delimiter.map(str::to_string),
            marker: marker.map(str::to_string),
            max_payload_bytes,
            ..ObjectListPage::default()
        };
        page.objects.reserve(limit.min(self.objects.len()));
        page.common_prefixes.reserve(limit.min(self.objects.len()));
        let mut emitted_prefixes = BTreeSet::new();
        if let Some(marker) = marker {
            if let Some(common_prefix) = object_common_prefix(prefix, delimiter, marker) {
                emitted_prefixes.insert(common_prefix);
            }
        }
        let mut last_marker = marker.map(str::to_string);
        let start_key = marker
            .filter(|marker| prefix.is_empty() || *marker >= prefix)
            .unwrap_or(prefix);
        let max_scan = limit
            .saturating_mul(16)
            .clamp(limit.saturating_add(1), 65_536);
        for (object_id, metadata) in self.objects.range(ObjectId::new(bucket, start_key)..) {
            if object_id.bucket != bucket {
                break;
            }
            if !object_id.key.starts_with(prefix) {
                break;
            }
            if metadata.deleted {
                continue;
            }
            if marker.map_or(false, |m| object_id.key.as_str() <= m) {
                continue;
            }
            page.scanned_objects = page.scanned_objects.saturating_add(1);
            if page.scanned_objects as usize > max_scan {
                page.has_more = true;
                page.next_marker = last_marker;
                break;
            }
            let key = metadata.object_id.key.clone();
            let common_prefix = object_common_prefix(prefix, delimiter, &key);
            let emits_prefix = common_prefix
                .as_ref()
                .map_or(false, |prefix| !emitted_prefixes.contains(prefix));
            let emits_object = common_prefix.is_none();
            if emits_prefix || emits_object {
                let emitted_items = page
                    .objects
                    .len()
                    .saturating_add(page.common_prefixes.len());
                if emitted_items >= limit {
                    page.has_more = true;
                    page.next_marker = last_marker;
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
                        page.next_marker = last_marker;
                        break;
                    }
                    page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
                    page.emitted_common_prefixes = page.emitted_common_prefixes.saturating_add(1);
                    page.common_prefixes.push(common_prefix);
                    last_marker = Some(key.to_string());
                    if would_exceed_payload {
                        page.has_more = true;
                        page.next_marker = last_marker;
                        break;
                    }
                }
                last_marker = Some(key.to_string());
                continue;
            }
            if page
                .objects
                .len()
                .saturating_add(page.common_prefixes.len())
                >= limit
            {
                page.has_more = true;
                page.next_marker = last_marker;
                break;
            }
            let payload_bytes = object_metadata_payload_bytes(&metadata);
            let would_exceed_payload = max_payload_bytes > 0
                && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
            if would_exceed_payload && !page.objects.is_empty() {
                page.has_more = true;
                page.next_marker = last_marker;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
            page.emitted_objects = page.emitted_objects.saturating_add(1);
            last_marker = Some(key.to_string());
            page.objects.push(metadata.clone());
            if would_exceed_payload {
                page.has_more = true;
                page.next_marker = last_marker;
                break;
            }
        }
        stamp_object_list_page_proof("ListObjects", &mut page, None)?;
        Ok(page)
    }

    pub fn list_objects_with_metadata_read(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        let meta_prefix = if prefix.is_empty() {
            format!("object/{bucket}/")
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let readiness = self.prepare_metadata_read(&meta_prefix, &options)?;
        let listed = self.list_objects(bucket, prefix, marker, limit)?;
        Ok((listed, readiness))
    }

    pub fn list_objects_page_with_metadata_read(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_with_metadata_read(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            options,
        )
    }

    pub fn list_objects_hierarchy_page_with_metadata_read(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_with_metadata_read_list_page_proof_fenced(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            options,
            None,
        )
    }

    pub fn list_objects_hierarchy_page_with_metadata_read_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_object_list_page_proof_checksum: Option<&str>,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(delimiter) = delimiter {
            if delimiter.is_empty()
                || delimiter.len() > 1024
                || delimiter.contains('\0')
                || delimiter.contains('\n')
            {
                return Err(ObjectError::InvalidArgument(
                    "object list delimiter is invalid".to_string(),
                ));
            }
        }
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let meta_prefix = if prefix.is_empty() {
            format!("object/{bucket}/")
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let readiness = self.prepare_metadata_read(&meta_prefix, &options)?;
        let mut page = self.list_objects_hierarchy_page(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_list_page_proof(
            "ListObjects",
            &mut page,
            expected_object_list_page_proof_checksum,
        )?;
        Ok((page, readiness))
    }

}
