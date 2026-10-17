impl MatrixObjectStore {
    pub fn list_object_inventory(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectInventoryPage, ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let limit = limit.max(1);
        let mut page = ObjectInventoryPage {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            marker: marker.map(ToString::to_string),
            ..ObjectInventoryPage::default()
        };
        let start_key = marker
            .filter(|marker| prefix.is_empty() || *marker >= prefix)
            .unwrap_or(prefix);
        let max_scan = limit
            .saturating_mul(64)
            .clamp(limit.saturating_add(1), 65_536);
        let mut scanned = 0_usize;
        for (object_id, metadata) in self.objects.range(ObjectId::new(bucket, start_key)..) {
            if object_id.bucket != bucket {
                break;
            }
            if !object_id.key.starts_with(prefix) {
                break;
            }
            if marker.map_or(false, |m| object_id.key.as_str() <= m) {
                continue;
            }
            scanned = scanned.saturating_add(1);
            if metadata.deleted {
                if scanned >= max_scan {
                    page.has_more = true;
                    page.next_marker = Some(object_id.key.clone());
                    break;
                }
                continue;
            }
            let placement = self.meta_service.object_placement(object_id).ok();
            let record = ObjectInventoryRecord {
                metadata: metadata.clone(),
                placement,
            };
            let record_payload_bytes = object_inventory_record_payload_bytes(&record);
            let next_payload = page.payload_bytes.saturating_add(record_payload_bytes);
            if !page.records.is_empty() && max_payload_bytes > 0 && next_payload > max_payload_bytes
            {
                page.has_more = true;
                break;
            }
            page.payload_bytes = next_payload;
            page.listed_objects = page.listed_objects.saturating_add(1);
            page.next_marker = Some(object_id.key.clone());
            page.records.push(record);
            if page.records.len() >= limit {
                page.has_more = true;
                break;
            }
            if scanned >= max_scan {
                page.has_more = true;
                break;
            }
        }
        if !page.has_more && page.records.is_empty() {
            page.next_marker = None;
        }
        stamp_object_inventory_page_proof("ListObjectInventory", &mut page, None)?;
        Ok(page)
    }

    pub fn list_object_inventory_with_metadata_read(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectInventoryPage, MetadataRaftReadiness), ObjectError> {
        self.list_object_inventory_with_metadata_read_list_page_proof_fenced(
            bucket,
            prefix,
            marker,
            limit,
            max_payload_bytes,
            options,
            None,
        )
    }

    pub fn list_object_inventory_with_metadata_read_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_object_list_page_proof_checksum: Option<&str>,
    ) -> Result<(ObjectInventoryPage, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
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
        let mut page =
            self.list_object_inventory(bucket, prefix, marker, limit, max_payload_bytes)?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_inventory_page_proof(
            "ListObjectInventory",
            &mut page,
            expected_object_list_page_proof_checksum,
        )?;
        Ok((page, readiness))
    }

}
