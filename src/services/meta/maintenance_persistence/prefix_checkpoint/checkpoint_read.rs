// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn get_change_checkpoint(
        &self,
        consumer_id: &str,
        prefix: &str,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        let checkpoint_key = change_checkpoint_key(consumer_id, prefix);
        self.change_checkpoints
            .get(&checkpoint_key)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata change checkpoint missing".to_string()))
    }

    pub fn list_change_checkpoints(&self) -> Vec<MetadataChangeCheckpoint> {
        self.change_checkpoints.values().cloned().collect()
    }

    pub fn page_change_checkpoints(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangeCheckpointPage, ObjectError> {
        if !prefix.is_empty() {
            validate_meta_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_meta_key(marker)?;
            if !prefix.is_empty() && !marker.starts_with(prefix) {
                return Err(ObjectError::InvalidArgument(
                    "metadata change checkpoint marker must be within prefix".to_string(),
                ));
            }
        }
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata change checkpoint page limit must be positive".to_string(),
            ));
        }
        let limit = limit.min(k_max_metadata_receipt_list_limit());
        let mut page = MetadataChangeCheckpointPage {
            checkpoints: Vec::new(),
            prefix: prefix.to_string(),
            marker: marker.map(str::to_string),
            next_marker: None,
            checkpoint_count: 0,
            matched_count: 0,
            scanned_count: 0,
            total_checkpoint_count: self.change_checkpoints.len() as u64,
            payload_bytes: 0,
            max_payload_bytes,
            budget_limited: false,
            truncated: false,
        };
        let mut iter: Box<dyn Iterator<Item = (&String, &MetadataChangeCheckpoint)>> =
            if let Some(marker) = marker {
                Box::new(self.change_checkpoints.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else if prefix.is_empty() {
                Box::new(self.change_checkpoints.iter())
            } else {
                Box::new(self.change_checkpoints.range(prefix.to_string()..))
            };
        for (checkpoint_key, checkpoint) in &mut iter {
            if !prefix.is_empty() && !checkpoint_key.starts_with(prefix) {
                break;
            }
            page.scanned_count = page.scanned_count.saturating_add(1);
            if page.checkpoints.len() >= limit {
                page.truncated = true;
                break;
            }
            let item_payload = metadata_change_checkpoint_lag_payload_bytes(checkpoint);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                if page.checkpoints.is_empty() {
                    return Err(ObjectError::InvalidArgument(format!(
                        "metadata change checkpoint list first checkpoint exceeds max_payload_bytes: {checkpoint_key}"
                    )));
                }
                page.truncated = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_marker = Some(checkpoint_key.clone());
            page.checkpoints.push(checkpoint.clone());
            page.matched_count = page.matched_count.saturating_add(1);
        }
        page.checkpoint_count = page.checkpoints.len() as u64;
        if !page.truncated {
            page.next_marker = None;
        }
        Ok(page)
    }

}
