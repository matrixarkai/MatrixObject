impl MatrixObjectStore {
    pub fn set_object_tags(
        &mut self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::SetObjectTags {
            bucket: bucket.to_string(),
            key: key.to_string(),
            tags: tags.clone(),
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) =
            self.append_set_object_tags_journal_record(bucket, key, &tags, update_time_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<BTreeMap<String, String>, ObjectError> {
        Ok(self.head_object(bucket, key)?.tags.into())
    }

    pub fn get_object_tags_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(BTreeMap<String, String>, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        Ok((metadata.tags.into(), readiness))
    }

    pub fn list_objects_by_tag(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ObjectError> {
        validate_bucket(bucket)?;
        validate_tag_key(tag_key)?;
        validate_tag_value(tag_value)?;
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let take = if limit == 0 { usize::MAX } else { limit };
        let start_key = marker.unwrap_or("");
        let mut out = Vec::with_capacity(take.min(self.objects.len()));
        for (object_id, metadata) in self.objects.range(ObjectId::new(bucket, start_key)..) {
            if object_id.bucket != bucket {
                break;
            }
            if marker.map_or(false, |marker| object_id.key.as_str() <= marker) {
                continue;
            }
            if metadata.deleted {
                continue;
            }
            if metadata
                .tags
                .get(tag_key)
                .map_or(true, |value| value != tag_value)
            {
                continue;
            }
            out.push(metadata.clone());
            if out.len() >= take {
                break;
            }
        }
        Ok(out)
    }

    pub fn list_objects_by_tag_page(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectTagPage, ObjectError> {
        validate_bucket(bucket)?;
        validate_tag_key(tag_key)?;
        validate_tag_value(tag_value)?;
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "object tag page limit must be positive".to_string(),
            ));
        }
        let mut page = ObjectTagPage {
            bucket: bucket.to_string(),
            tag_key: tag_key.to_string(),
            tag_value: tag_value.to_string(),
            marker: marker.map(str::to_string),
            max_payload_bytes,
            ..ObjectTagPage::default()
        };
        let start_key = marker.unwrap_or("");
        let max_scan = limit
            .saturating_mul(64)
            .clamp(limit.saturating_add(1), 65_536);
        let mut last_marker = marker.map(str::to_string);
        for (object_id, metadata) in self.objects.range(ObjectId::new(bucket, start_key)..) {
            if object_id.bucket != bucket {
                break;
            }
            if marker.map_or(false, |marker| object_id.key.as_str() <= marker) {
                continue;
            }
            page.scanned_objects = page.scanned_objects.saturating_add(1);
            if page.scanned_objects as usize > max_scan {
                page.has_more = true;
                page.next_marker = Some(object_id.key.to_string());
                break;
            }
            if metadata.deleted {
                last_marker = Some(object_id.key.to_string());
                continue;
            }
            if metadata
                .tags
                .get(tag_key)
                .map_or(true, |value| value != tag_value)
            {
                last_marker = Some(object_id.key.to_string());
                continue;
            }
            if page.objects.len() >= limit {
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
            last_marker = Some(object_id.key.to_string());
            page.objects.push(metadata.clone());
            if would_exceed_payload {
                page.has_more = true;
                page.next_marker = last_marker;
                break;
            }
        }
        stamp_object_tag_list_page_proof("ListObjectsByTag", &mut page, None)?;
        Ok(page)
    }

    pub fn list_objects_by_tag_with_metadata_read(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        validate_tag_key(tag_key)?;
        validate_tag_value(tag_value)?;
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let meta_prefix = format!("object/{bucket}/");
        let readiness = self.prepare_metadata_read(&meta_prefix, &options)?;
        let listed = self.list_objects_by_tag(bucket, tag_key, tag_value, marker, limit)?;
        Ok((listed, readiness))
    }

    pub fn list_objects_by_tag_page_with_metadata_read(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectTagPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_by_tag_page_with_metadata_read_list_page_proof_fenced(
            bucket,
            tag_key,
            tag_value,
            marker,
            limit,
            max_payload_bytes,
            options,
            None,
        )
    }

    pub fn list_objects_by_tag_page_with_metadata_read_list_page_proof_fenced(
        &self,
        bucket: &str,
        tag_key: &str,
        tag_value: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_object_list_page_proof_checksum: Option<&str>,
    ) -> Result<(ObjectTagPage, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        validate_tag_key(tag_key)?;
        validate_tag_value(tag_value)?;
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let meta_prefix = format!("object/{bucket}/");
        let readiness = self.prepare_metadata_read(&meta_prefix, &options)?;
        let mut page = self.list_objects_by_tag_page(
            bucket,
            tag_key,
            tag_value,
            marker,
            limit,
            max_payload_bytes,
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_tag_list_page_proof(
            "ListObjectsByTag",
            &mut page,
            expected_object_list_page_proof_checksum,
        )?;
        Ok((page, readiness))
    }

}
