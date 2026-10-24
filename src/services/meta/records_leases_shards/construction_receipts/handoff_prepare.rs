// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn prepare_shard_record_handoff_raft_entry(
        &self,
        shard_id: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        include_deleted: bool,
        source_node_id: &str,
        expected_shard_owner: Option<&str>,
        expected_shard_epoch: Option<u64>,
        handoff_token: Option<&str>,
        apply_mutation_id: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataShardRecordHandoff, ObjectError> {
        validate_meta_key(shard_id)?;
        if let Some(marker) = marker {
            validate_meta_key(marker)?;
        }
        validate_node_id(source_node_id)?;
        if let Some(expected_shard_owner) = expected_shard_owner {
            validate_node_id(expected_shard_owner)?;
        }
        if expected_shard_epoch == Some(0) {
            return Err(ObjectError::InvalidArgument(
                "metadata shard handoff expected epoch must be positive".to_string(),
            ));
        }
        validate_idempotency_key(apply_mutation_id)?;
        if let Some(handoff_token) = handoff_token {
            validate_idempotency_key(handoff_token)?;
        }
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard record handoff previous commit fence mismatch".to_string(),
            ));
        }
        let limit = limit.clamp(1, 1024);
        let shard =
            self.shards.get(shard_id).cloned().ok_or_else(|| {
                ObjectError::NotFound("metadata handoff shard missing".to_string())
            })?;
        if expected_shard_owner.is_some_and(|owner| shard.owner_node_id != owner)
            || expected_shard_epoch.is_some_and(|epoch| shard.epoch != epoch)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard handoff source owner fence mismatch".to_string(),
            ));
        }
        let mut records = Vec::new();
        let mut scanned_records = 0u64;
        let mut payload_bytes = 0u64;
        let mut truncated = false;
        let mut next_marker = None;
        for (key, record) in self.records_range((
                std::ops::Bound::Included(shard.start_key.as_str()),
                std::ops::Bound::Unbounded,
            )) {
            if !metadata_shard_contains_key(&shard, key) {
                break;
            }
            if marker.map_or(false, |marker| key.as_str() <= marker) {
                continue;
            }
            scanned_records = scanned_records.saturating_add(1);
            if record.deleted && !include_deleted {
                continue;
            }
            if records.len() >= limit {
                truncated = true;
                next_marker = Some(key.to_string());
                break;
            }
            let record_payload_bytes = metadata_record_payload_bytes(record);
            if max_payload_bytes > 0
                && payload_bytes.saturating_add(record_payload_bytes) > max_payload_bytes
            {
                if records.is_empty() {
                    return Err(ObjectError::InvalidArgument(
                        "metadata shard handoff first record exceeds max_payload_bytes".to_string(),
                    ));
                }
                truncated = true;
                next_marker = Some(key.to_string());
                break;
            }
            payload_bytes = payload_bytes.saturating_add(record_payload_bytes);
            next_marker = Some(key.to_string());
            records.push(record.clone());
        }
        if records.is_empty() {
            return Err(ObjectError::NotFound(
                "metadata shard handoff contains no records".to_string(),
            ));
        }
        let (source_shard_topology_epoch, source_shard_topology_checksum) =
            metadata_shard_topology_fingerprint(self);
        let source_commit_index = self.next_commit_index.saturating_sub(1);
        let source_read_index = source_commit_index;
        let handoff_batch_checksum = metadata_shard_record_handoff_batch_checksum(
            source_node_id,
            &shard,
            marker,
            next_marker.as_deref(),
            limit,
            max_payload_bytes,
            include_deleted,
            &records,
            scanned_records,
            payload_bytes,
            truncated,
            source_commit_index,
            source_read_index,
            source_shard_topology_epoch,
            &source_shard_topology_checksum,
        );
        let handoff_token = handoff_token
            .map(str::to_string)
            .unwrap_or_else(|| handoff_batch_checksum.clone());
        let mut prepared = PreparedMetadataShardRecordHandoff {
            source_node_id: source_node_id.to_string(),
            shard,
            marker: marker.map(str::to_string),
            next_marker,
            limit,
            max_payload_bytes,
            include_deleted,
            records,
            scanned_records,
            payload_bytes,
            truncated,
            source_commit_index,
            source_read_index,
            source_shard_topology_epoch,
            source_shard_topology_checksum,
            handoff_token,
            handoff_batch_checksum,
            apply_mutation_id: apply_mutation_id.to_string(),
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_shard_record_handoff_checksum(&prepared);
        Ok(prepared)
    }

}
