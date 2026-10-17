fn metadata_fingerprint_reserve_hint(service: &MatrixMetaService) -> usize {
    let hint = 4096usize
        .saturating_add(service.records.len().saturating_mul(128))
        .saturating_add(service.leases.len().saturating_mul(96))
        .saturating_add(service.lease_receipts.len().saturating_mul(192))
        .saturating_add(service.coordinator_config_receipts.len().saturating_mul(160))
        .saturating_add(service.coordinator_sessions.len().saturating_mul(128))
        .saturating_add(service.coordinator_session_receipts.len().saturating_mul(192))
        .saturating_add(service.shards.len().saturating_mul(128))
        .saturating_add(service.shard_receipts.len().saturating_mul(224))
        .saturating_add(service.batch_request_receipts.len().saturating_mul(192))
        .saturating_add(service.transactions.len().saturating_mul(160))
        .saturating_add(service.transaction_receipts.len().saturating_mul(224))
        .saturating_add(service.change_checkpoints.len().saturating_mul(128))
        .saturating_add(service.changes.len().saturating_mul(160));
    hint.min(8 * 1024 * 1024)
}

impl MatrixMetaService {
    pub fn export_snapshot(&self) -> MetaSnapshot {
        let mut snapshot = MetaSnapshot {
            records: self.records.clone(),
            leases: self.leases.clone(),
            lease_receipts: self.lease_receipts.clone(),
            coordinator_config: self.coordinator_config.clone(),
            coordinator_config_receipts: self.coordinator_config_receipts.clone(),
            coordinator_sessions: self.coordinator_sessions.clone(),
            coordinator_session_receipts: self.coordinator_session_receipts.clone(),
            shards: self.shards.clone(),
            shard_receipts: self.shard_receipts.clone(),
            batch_request_receipts: self.batch_request_receipts.clone(),
            transactions: self.transactions.clone(),
            transaction_receipts: self.transaction_receipts.clone(),
            change_checkpoints: self.change_checkpoints.clone(),
            changes: self.changes.clone(),
            next_commit_index: self.next_commit_index,
            next_change_sequence: self.next_change_sequence,
            compacted_tombstone_count: self.compacted_tombstone_count,
            last_tombstone_compaction_commit_index: self.last_tombstone_compaction_commit_index,
            raft_snapshot_floor_commit_index: self.raft_snapshot_floor_commit_index,
            raft_snapshot_compacted_change_count: self.raft_snapshot_compacted_change_count,
            raft_snapshot_compacted_batch_receipt_count: self
                .raft_snapshot_compacted_batch_receipt_count,
            change_log_retention_limit: self.change_log_retention_limit,
            mutation_receipt_retention_limit: self.mutation_receipt_retention_limit,
            admission_policy: self.admission_policy.clone(),
            qos_config: self.qos_config.clone(),
            qos_state: self.qos_state.clone(),
            checksum: String::new(),
        };
        snapshot.checksum = snapshot.checksum_material_hash();
        snapshot
    }

    pub fn metadata_state_fingerprint(&self) -> String {
        let mut material = Vec::with_capacity(metadata_fingerprint_reserve_hint(self));
        append_string(&mut material, "MOSRSMETA-FINGERPRINT1");
        append_u64(&mut material, self.next_commit_index);
        append_u64(&mut material, self.next_change_sequence);
        append_u64(&mut material, self.compacted_tombstone_count);
        append_u64(&mut material, self.last_tombstone_compaction_commit_index);
        append_u64(&mut material, self.raft_snapshot_floor_commit_index);
        append_u64(&mut material, self.raft_snapshot_compacted_change_count);
        append_u64(
            &mut material,
            self.raft_snapshot_compacted_batch_receipt_count,
        );
        append_u64(&mut material, self.change_log_retention_limit as u64);
        append_u64(&mut material, self.mutation_receipt_retention_limit as u64);
        append_metadata_admission_policy(&mut material, &self.admission_policy);
        append_metadata_qos_config(&mut material, &self.qos_config);
        append_metadata_qos_state(&mut material, &self.qos_state);
        append_metadata_coordinator_config(&mut material, &self.coordinator_config);
        append_u64(&mut material, self.coordinator_config_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.coordinator_config_receipts {
            append_string(&mut material, idempotency_key);
            append_metadata_coordinator_config_receipt(&mut material, receipt);
        }

        append_u64(&mut material, self.records.len() as u64);
        for (key, record) in &self.records {
            append_string(&mut material, key);
            append_string(&mut material, &etag(record.value.as_bytes()));
            append_u64(&mut material, record.version);
            append_u64(&mut material, record.update_time_ms);
            append_u64(&mut material, record.deleted as u64);
        }

        append_u64(&mut material, self.leases.len() as u64);
        for (namespace, lease) in &self.leases {
            append_string(&mut material, namespace);
            append_string(&mut material, &lease.owner);
            append_u64(&mut material, lease.epoch);
            append_u64(&mut material, lease.expire_time_ms);
            append_u64(&mut material, lease.ttl_ms);
        }
        append_u64(&mut material, self.lease_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.lease_receipts {
            append_string(&mut material, idempotency_key);
            append_metadata_lease_receipt(&mut material, receipt);
        }

        append_u64(&mut material, self.coordinator_sessions.len() as u64);
        for (session_id, session) in &self.coordinator_sessions {
            append_string(&mut material, session_id);
            append_string(&mut material, &session.owner);
            append_string(&mut material, &session.endpoint);
            append_u64(&mut material, session.epoch);
            append_u64(&mut material, session.acquire_time_ms);
            append_u64(&mut material, session.last_renew_ms);
            append_u64(&mut material, session.expire_time_ms);
            append_u64(&mut material, session.ttl_ms);
        }
        append_u64(
            &mut material,
            self.coordinator_session_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in &self.coordinator_session_receipts {
            append_string(&mut material, idempotency_key);
            append_coordinator_session_receipt(&mut material, receipt);
        }

        append_u64(&mut material, self.shards.len() as u64);
        for (shard_id, shard) in &self.shards {
            append_string(&mut material, shard_id);
            append_string(&mut material, &shard.start_key);
            append_string(&mut material, &shard.end_key);
            append_string(&mut material, &shard.owner_node_id);
            append_u64(&mut material, shard.epoch);
            append_u64(&mut material, shard.draining as u64);
        }
        append_u64(&mut material, self.shard_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.shard_receipts {
            append_string(&mut material, idempotency_key);
            append_metadata_shard_receipt(&mut material, receipt);
        }

        append_u64(&mut material, self.batch_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.batch_request_receipts {
            append_string(&mut material, idempotency_key);
            append_metadata_batch_receipt(&mut material, receipt);
        }

        append_u64(&mut material, self.transactions.len() as u64);
        for (transaction_id, transaction) in &self.transactions {
            append_string(&mut material, transaction_id);
            append_string(&mut material, &transaction.owner);
            append_u64(&mut material, transaction.expire_time_ms);
            append_string(&mut material, &transaction.state);
            append_u64(&mut material, transaction.begin_time_ms);
            append_u64(&mut material, transaction.update_time_ms);
            append_u64(&mut material, transaction.mutations.len() as u64);
            for mutation in &transaction.mutations {
                append_string(&mut material, &mutation.key);
                append_string(&mut material, &etag(mutation.value.as_bytes()));
                append_u64(&mut material, mutation.expected_version);
                append_u64(&mut material, mutation.delete as u64);
            }
            append_u64(&mut material, transaction.read_versions.len() as u64);
            for (key, version) in &transaction.read_versions {
                append_string(&mut material, key);
                append_u64(&mut material, *version);
            }
        }
        append_u64(&mut material, self.transaction_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.transaction_receipts {
            append_string(&mut material, idempotency_key);
            append_metadata_transaction_receipt(&mut material, receipt);
        }

        append_u64(&mut material, self.change_checkpoints.len() as u64);
        for (checkpoint_key, checkpoint) in &self.change_checkpoints {
            append_string(&mut material, checkpoint_key);
            append_string(&mut material, &checkpoint.consumer_id);
            append_string(&mut material, &checkpoint.prefix);
            append_u64(&mut material, checkpoint.after_change_sequence);
            append_u64(&mut material, checkpoint.after_commit_index);
            append_u64(&mut material, checkpoint.update_time_ms);
            append_string(&mut material, &checkpoint.owner);
            append_string(&mut material, &checkpoint.note);
        }

        append_u64(&mut material, self.changes.len() as u64);
        for change in &self.changes {
            append_u64(&mut material, change.sequence);
            append_u64(&mut material, change.commit_index);
            append_string(&mut material, &change.key);
            append_u64(&mut material, change.version);
            append_u64(&mut material, change.deleted as u64);
            append_u64(&mut material, change.update_time_ms);
        }
        etag(&material)
    }

    fn metadata_control_proof_digest_at(&self, now_ms: u64) -> (String, u64) {
        let (topology_epoch, topology_checksum) = metadata_shard_topology_fingerprint(self);
        let session_checksum = metadata_route_session_fingerprint(self, now_ms);
        let active_owner_sessions = self
            .coordinator_sessions
            .values()
            .filter(|session| {
                session.expire_time_ms > now_ms
                    && self.coordinator_session_endpoint_is_fenced(&session.endpoint)
            })
            .count() as u64;
        let stale_owner_shards = self
            .shards
            .values()
            .filter(|shard| {
                !shard.draining
                    && self
                        .active_fenced_owner_session(&shard.owner_node_id, now_ms)
                        .is_none()
            })
            .count() as u64;
        let external_coordinator_required =
            !is_embedded_coordinator_backend(&self.coordinator_config.backend);
        let external_coordinator_endpoints_ready =
            !external_coordinator_required || !self.coordinator_config.endpoints.is_empty();
        let external_coordinator_owner_sessions_ready =
            !external_coordinator_required || self.shards.is_empty() || stale_owner_shards == 0;
        let external_coordinator_ready =
            external_coordinator_endpoints_ready && external_coordinator_owner_sessions_ready;
        let state_fingerprint = self.metadata_state_fingerprint();
        let mut material = Vec::with_capacity(
            encoded_string_len("MOSRS-META-SERVICE-CONTROL3")
                + encoded_string_len(&state_fingerprint)
                + encoded_string_len(&topology_checksum)
                + encoded_string_len(&session_checksum)
                + std::mem::size_of::<u64>() * 20
                + self.coordinator_config.endpoints.len().saturating_mul(96),
        );
        append_string(&mut material, "MOSRS-META-SERVICE-CONTROL3");
        append_string(&mut material, &state_fingerprint);
        append_u64(&mut material, self.next_commit_index);
        append_u64(&mut material, self.next_change_sequence);
        append_u64(&mut material, self.compacted_tombstone_count);
        append_u64(&mut material, self.last_tombstone_compaction_commit_index);
        append_u64(&mut material, self.raft_snapshot_floor_commit_index);
        append_u64(&mut material, topology_epoch);
        append_string(&mut material, &topology_checksum);
        append_string(&mut material, &session_checksum);
        append_metadata_coordinator_config(&mut material, &self.coordinator_config);
        append_u64(&mut material, external_coordinator_required as u64);
        append_u64(&mut material, external_coordinator_endpoints_ready as u64);
        append_u64(
            &mut material,
            external_coordinator_owner_sessions_ready as u64,
        );
        append_u64(&mut material, external_coordinator_ready as u64);
        append_u64(&mut material, self.coordinator_config_receipts.len() as u64);
        append_u64(&mut material, self.coordinator_sessions.len() as u64);
        append_u64(&mut material, active_owner_sessions);
        append_u64(&mut material, stale_owner_shards);
        append_u64(
            &mut material,
            self.coordinator_session_receipts.len() as u64,
        );
        append_u64(&mut material, self.change_checkpoints.len() as u64);
        append_metadata_admission_policy(&mut material, &self.admission_policy);
        append_metadata_qos_config(&mut material, &self.qos_config);
        append_metadata_qos_state(&mut material, &self.qos_state);
        let state_bytes = material.len() as u64;
        (etag(&material), state_bytes)
    }

    fn metadata_control_proof_checksum_at(&self, now_ms: u64) -> String {
        self.metadata_control_proof_digest_at(now_ms).0
    }

    fn check_expected_metadata_control_proof(
        &self,
        expected_checksum: &str,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata service control proof checksum expectation is empty".to_string(),
            ));
        }
        let actual_checksum = self.metadata_control_proof_checksum_at(now_ms);
        if actual_checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata service control proof checksum mismatch: expected {} got {}",
                expected_checksum, actual_checksum
            )));
        }
        Ok(())
    }

}
