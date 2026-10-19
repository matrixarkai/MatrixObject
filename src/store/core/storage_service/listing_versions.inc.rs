// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn list_object_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<ObjectMetadata>, ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let object_id = ObjectId::new(bucket, key);
        let version_count = self
            .versions
            .get(&object_id)
            .map_or(0usize, Vec::len);
        let mut out = Vec::with_capacity(version_count.saturating_add(1));
        if let Some(current) = self.objects.get(&object_id) {
            out.push(current.clone());
        }
        if let Some(versions) = self.versions.get(&object_id) {
            out.extend(versions.iter().cloned());
        }
        out.sort_by(|a, b| b.version.cmp(&a.version));
        Ok(out)
    }

    pub fn list_object_versions_page(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectVersionPage, ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        if matches!(marker, Some(0)) {
            return Err(ObjectError::InvalidArgument(
                "object version page marker must be positive".to_string(),
            ));
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "object version page limit must be positive".to_string(),
            ));
        }
        let versions = self.list_object_versions(bucket, key)?;
        let mut page = ObjectVersionPage {
            bucket: bucket.to_string(),
            key: key.to_string(),
            marker,
            max_payload_bytes,
            requested_versions: versions.len() as u64,
            ..ObjectVersionPage::default()
        };
        page.versions.reserve(limit.min(versions.len()));
        let mut eligible_versions = versions
            .into_iter()
            .filter(|metadata| !marker.map_or(false, |marker| metadata.version >= marker))
            .peekable();
        while let Some(metadata) = eligible_versions.next() {
            if page.versions.len() >= limit {
                page.has_more = true;
                page.next_version_marker = page.versions.last().map(|metadata| metadata.version);
                break;
            }
            let payload_bytes = object_metadata_payload_bytes(&metadata);
            let would_exceed_payload = max_payload_bytes > 0
                && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
            if would_exceed_payload && !page.versions.is_empty() {
                page.has_more = true;
                page.next_version_marker = page.versions.last().map(|metadata| metadata.version);
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
            page.emitted_versions = page.emitted_versions.saturating_add(1);
            page.versions.push(metadata);
            if would_exceed_payload {
                page.has_more = eligible_versions.peek().is_some();
                page.next_version_marker = page.versions.last().map(|metadata| metadata.version);
                break;
            }
        }
        stamp_object_version_page_proof("ListObjectVersions", &mut page, None)?;
        Ok(page)
    }

    pub fn list_bucket_object_versions_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<BucketObjectVersionPage, ObjectError> {
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
                    "object version delimiter is invalid".to_string(),
                ));
            }
        }
        if let Some(key_marker) = key_marker {
            validate_key(key_marker)?;
        }
        if matches!(version_marker, Some(0)) {
            return Err(ObjectError::InvalidArgument(
                "object version marker must be positive".to_string(),
            ));
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "bucket object version page limit must be positive".to_string(),
            ));
        }
        let mut page = BucketObjectVersionPage {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            delimiter: delimiter.map(str::to_string),
            key_marker: key_marker.map(str::to_string),
            version_marker,
            max_payload_bytes,
            ..BucketObjectVersionPage::default()
        };
        let catalog_bound = self.objects.len().saturating_add(self.versions.len());
        page.versions.reserve(limit.min(catalog_bound));
        page.common_prefixes.reserve(limit.min(catalog_bound));
        let mut emitted_prefixes = BTreeSet::new();
        if let Some(key_marker) = key_marker {
            if let Some(common_prefix) = object_common_prefix(prefix, delimiter, key_marker) {
                emitted_prefixes.insert(common_prefix);
            }
        }
        let mut last_key_marker = key_marker.map(str::to_string);
        let mut last_version_marker = version_marker;
        let start_key = key_marker
            .filter(|marker| prefix.is_empty() || *marker >= prefix)
            .unwrap_or(prefix);
        let mut object_iter = self
            .objects
            .range(ObjectId::new(bucket, start_key)..)
            .peekable();
        let mut version_iter = self
            .versions
            .range(ObjectId::new(bucket, start_key)..)
            .peekable();
        let max_scan = limit
            .saturating_mul(16)
            .clamp(limit.saturating_add(1), 65_536);
        loop {
            let object_key = object_iter.peek().and_then(|(object_id, _)| {
                if object_id.bucket == bucket && object_id.key.starts_with(prefix) {
                    Some(object_id.key.as_str())
                } else {
                    None
                }
            });
            let version_key = version_iter.peek().and_then(|(object_id, _)| {
                if object_id.bucket == bucket && object_id.key.starts_with(prefix) {
                    Some(object_id.key.as_str())
                } else {
                    None
                }
            });
            let key = match (object_key, version_key) {
                (Some(left), Some(right)) => {
                    if left <= right {
                        left.to_string()
                    } else {
                        right.to_string()
                    }
                }
                (Some(key), None) | (None, Some(key)) => key.to_string(),
                (None, None) => break,
            };
            while object_iter
                .peek()
                .map_or(false, |(object_id, _)| object_id.key == key)
            {
                object_iter.next();
            }
            while version_iter
                .peek()
                .map_or(false, |(object_id, _)| object_id.key == key)
            {
                version_iter.next();
            }
            page.scanned_keys = page.scanned_keys.saturating_add(1);
            if page.scanned_keys as usize > max_scan {
                page.has_more = true;
                page.next_key_marker = last_key_marker.clone();
                page.next_version_marker = last_version_marker;
                break;
            }
            let common_prefix = object_common_prefix(prefix, delimiter, &key);
            let emits_prefix = common_prefix
                .as_ref()
                .map_or(false, |prefix| !emitted_prefixes.contains(prefix));
            let emits_versions = common_prefix.is_none();
            if emits_prefix || emits_versions {
                let emitted_items = page
                    .versions
                    .len()
                    .saturating_add(page.common_prefixes.len());
                if emitted_items >= limit {
                    page.has_more = true;
                    page.next_key_marker = last_key_marker.clone();
                    page.next_version_marker = last_version_marker;
                    break;
                }
            }
            if let Some(common_prefix) = common_prefix {
                emitted_prefixes.insert(common_prefix.clone());
                last_key_marker = Some(key.clone());
                last_version_marker = None;
                if emits_prefix {
                    const COMMON_PREFIX_ENTRY_OVERHEAD: u64 = 32;
                    let payload_bytes =
                        COMMON_PREFIX_ENTRY_OVERHEAD.saturating_add(common_prefix.len() as u64);
                    let would_exceed_payload = max_payload_bytes > 0
                        && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
                    if would_exceed_payload && !page.common_prefixes.is_empty() {
                        page.has_more = true;
                        page.next_key_marker = last_key_marker.clone();
                        page.next_version_marker = None;
                        break;
                    }
                    page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
                    page.emitted_common_prefixes = page.emitted_common_prefixes.saturating_add(1);
                    page.common_prefixes.push(common_prefix);
                    if would_exceed_payload {
                        page.has_more = true;
                        page.next_key_marker = last_key_marker.clone();
                        page.next_version_marker = None;
                        break;
                    }
                }
                continue;
            }
            let marker_for_key = if key_marker == Some(key.as_str()) {
                version_marker
            } else {
                None
            };
            let versions = self.list_object_versions_page(
                bucket,
                &key,
                marker_for_key,
                limit,
                max_payload_bytes,
            )?;
            for metadata in versions.versions {
                let emitted_items = page
                    .versions
                    .len()
                    .saturating_add(page.common_prefixes.len());
                if emitted_items >= limit {
                    page.has_more = true;
                    page.next_key_marker = last_key_marker.clone();
                    page.next_version_marker = last_version_marker;
                    break;
                }
                let payload_bytes = object_metadata_payload_bytes(&metadata);
                let would_exceed_payload = max_payload_bytes > 0
                    && page.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes;
                if would_exceed_payload && !page.versions.is_empty() {
                    page.has_more = true;
                    page.next_key_marker = last_key_marker.clone();
                    page.next_version_marker = last_version_marker;
                    break;
                }
                page.payload_bytes = page.payload_bytes.saturating_add(payload_bytes);
                page.emitted_versions = page.emitted_versions.saturating_add(1);
                last_key_marker = Some(key.clone());
                last_version_marker = Some(metadata.version);
                page.versions.push(metadata);
                if would_exceed_payload {
                    page.has_more = true;
                    page.next_key_marker = last_key_marker.clone();
                    page.next_version_marker = last_version_marker;
                    break;
                }
            }
            if page.has_more {
                break;
            }
            if versions.has_more {
                page.has_more = true;
                page.next_key_marker = Some(key);
                page.next_version_marker = versions.next_version_marker;
                break;
            }
        }
        stamp_bucket_object_version_page_proof("ListBucketObjectVersions", &mut page, None)?;
        Ok(page)
    }
}
