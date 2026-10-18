impl MatrixMetaService {
    pub fn apply_prepared_shard_record_handoff(
        &mut self,
        prepared: PreparedMetadataShardRecordHandoff,
    ) -> Result<MetadataBatchReceipt, ObjectError> {
        validate_node_id(&prepared.source_node_id)?;
        validate_idempotency_key(&prepared.handoff_token)?;
        validate_idempotency_key(&prepared.apply_mutation_id)?;
        if prepared.records.is_empty() || prepared.records.len() > 1024 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared shard handoff requires 1..1024 records".to_string(),
            ));
        }
        let expected_entry_checksum =
            prepared_metadata_shard_record_handoff_checksum(&PreparedMetadataShardRecordHandoff {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_entry_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared shard handoff raft entry checksum mismatch".to_string(),
            ));
        }
        let expected_batch_checksum = metadata_shard_record_handoff_batch_checksum(
            &prepared.source_node_id,
            &prepared.shard,
            prepared.marker.as_deref(),
            prepared.next_marker.as_deref(),
            prepared.limit,
            prepared.max_payload_bytes,
            prepared.include_deleted,
            &prepared.records,
            prepared.scanned_records,
            prepared.payload_bytes,
            prepared.truncated,
            prepared.source_commit_index,
            prepared.source_read_index,
            prepared.source_shard_topology_epoch,
            &prepared.source_shard_topology_checksum,
        );
        if prepared.handoff_batch_checksum != expected_batch_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared shard handoff batch checksum mismatch".to_string(),
            ));
        }
        if prepared.source_read_index < prepared.source_commit_index {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared shard handoff source read index is stale".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard handoff commit index must be adjacent".to_string(),
            ));
        }
        let request_checksum = metadata_shard_record_handoff_request_checksum(&prepared);
        if let Some(existing) = self.batch_request_receipts.get(&prepared.apply_mutation_id) {
            if existing.request_checksum != request_checksum
                || existing.first_commit_index != prepared.prepared_commit_index
                || existing.last_commit_index != prepared.prepared_commit_index
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared shard handoff mutation id reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard handoff previous commit fence mismatch".to_string(),
            ));
        }
        let current_shard = self.shards.get(&prepared.shard.shard_id).ok_or_else(|| {
            ObjectError::NotFound("metadata handoff target shard missing".to_string())
        })?;
        if current_shard.start_key != prepared.shard.start_key
            || current_shard.end_key != prepared.shard.end_key
            || current_shard.epoch < prepared.shard.epoch
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard handoff target shard image mismatch".to_string(),
            ));
        }
        let mut seen_keys = BTreeSet::new();
        let mut max_update_time_ms = 0u64;
        let mut installed = Vec::with_capacity(prepared.records.len());
        let mut payload_bytes = 0u64;
        for record in &prepared.records {
            validate_meta_key(&record.key)?;
            if !metadata_shard_contains_key(&prepared.shard, &record.key) {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared shard handoff record outside shard range".to_string(),
                ));
            }
            if !seen_keys.insert(record.key.clone()) {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared shard handoff contains duplicate record key".to_string(),
                ));
            }
            if record.version == 0 || record.update_time_ms == 0 {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared shard handoff record image is incomplete".to_string(),
                ));
            }
            if record.deleted && !record.value.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared shard handoff deleted record carries value".to_string(),
                ));
            }
            if let Some(existing) = self.records.get(&record.key) {
                if existing.version > record.version {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared shard handoff would overwrite newer record".to_string(),
                    ));
                }
                if existing.version == record.version && existing != record {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared shard handoff record version image mismatch".to_string(),
                    ));
                }
            }
            payload_bytes = payload_bytes.saturating_add(metadata_record_payload_bytes(record));
            max_update_time_ms = max_update_time_ms.max(record.update_time_ms);
            installed.push(record.clone());
        }
        if payload_bytes != prepared.payload_bytes {
            return Err(ObjectError::Corruption(
                "metadata prepared shard handoff payload byte count mismatch".to_string(),
            ));
        }
        for record in &installed {
            let should_emit_change = self.records.get(&record.key) != Some(record);
            self.file_record(record.key.clone(), record.clone());
            if should_emit_change {
                self.append_metadata_change(MetadataChange {
                    sequence: self.next_change_sequence,
                    commit_index: prepared.prepared_commit_index,
                    key: record.key.clone(),
                    version: record.version,
                    deleted: record.deleted,
                    update_time_ms: record.update_time_ms,
                });
                self.next_change_sequence = self.next_change_sequence.saturating_add(1);
            }
        }
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        let receipt = MetadataBatchReceipt {
            idempotency_key: prepared.apply_mutation_id.clone(),
            request_checksum,
            owner_fenced: false,
            expected_owner: prepared.shard.owner_node_id.clone(),
            expected_epoch: prepared.shard.epoch,
            lease_owner: None,
            lease_epoch: None,
            now_ms: max_update_time_ms,
            records: installed,
            first_commit_index: prepared.prepared_commit_index,
            last_commit_index: prepared.prepared_commit_index,
            deduped: false,
        };
        self.batch_request_receipts
            .insert(prepared.apply_mutation_id, receipt.clone());
        Ok(receipt)
    }
}
