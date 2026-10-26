// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MetaSnapshot {
    pub(crate) fn checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(self.encoded_size_hint());
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
        for (key, record) in &self.records {
            append_string(&mut material, key);
            append_string(&mut material, &record.value.as_text());
            append_u64(&mut material, record.version());
            append_u64(&mut material, record.update_time_ms);
            append_u64(&mut material, record.deleted() as u64);
        }
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
                // CHECKSUM MATERIAL, same requirement as the encoding above.
                append_string(&mut material, &mutation.record_value.as_text());
                append_u64(&mut material, mutation.expected_version);
                append_u64(&mut material, mutation.delete as u64);
            }
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
        // THE ROSTER IS CHECKSUM MATERIAL. Two durable fields that no checksum covered have
        // already been found in this codebase once; durable state that nothing covers is
        // state a flipped bit can change without anything noticing.
        append_u64(&mut material, self.placement_commit_index);
        append_placement_roster(
            &mut material,
            &self.placement_nodes,
            &self.placements,
            &self.placement_leader_lease,
            &self.placement_leader_lease_receipts,
            &self.placement_coordinator_config,
            true,
        );
        etag(&material)
    }
}
