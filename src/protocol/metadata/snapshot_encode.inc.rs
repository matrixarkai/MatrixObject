impl MetaSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_size_hint());
        self.append_to_bytes(&mut out);
        Ok(out)
    }

    pub(crate) fn append_to_bytes(&self, mut out: &mut Vec<u8>) {
        let snapshot_start = out.len();
        append_string(&mut out, "MOSRSMETA16");
        append_u64(&mut out, self.next_commit_index);
        append_u64(&mut out, self.next_change_sequence);
        append_u64(&mut out, self.compacted_tombstone_count);
        append_u64(&mut out, self.last_tombstone_compaction_commit_index);
        append_u64(&mut out, self.raft_snapshot_floor_commit_index);
        append_u64(&mut out, self.raft_snapshot_compacted_change_count);
        append_u64(&mut out, self.raft_snapshot_compacted_batch_receipt_count);
        append_u64(&mut out, self.change_log_retention_limit as u64);
        append_u64(&mut out, self.mutation_receipt_retention_limit as u64);
        append_metadata_admission_policy(&mut out, &self.admission_policy);
        append_metadata_qos_config(&mut out, &self.qos_config);
        append_metadata_qos_state(&mut out, &self.qos_state);
        append_metadata_coordinator_config(&mut out, &self.coordinator_config);
        append_u64(&mut out, self.coordinator_config_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.coordinator_config_receipts {
            append_string(&mut out, idempotency_key);
            append_metadata_coordinator_config_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.records.len() as u64);
        for (key, record) in &self.records {
            append_string(&mut out, key);
            append_string(&mut out, &record.value);
            append_u64(&mut out, record.version);
            append_u64(&mut out, record.update_time_ms);
            append_u64(&mut out, record.deleted as u64);
        }
        append_u64(&mut out, self.leases.len() as u64);
        for (namespace, lease) in &self.leases {
            append_string(&mut out, namespace);
            append_string(&mut out, &lease.owner);
            append_u64(&mut out, lease.epoch);
            append_u64(&mut out, lease.expire_time_ms);
            append_u64(&mut out, lease.ttl_ms);
        }
        append_u64(&mut out, self.lease_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.lease_receipts {
            append_string(&mut out, idempotency_key);
            append_metadata_lease_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.coordinator_sessions.len() as u64);
        for (session_id, session) in &self.coordinator_sessions {
            append_string(&mut out, session_id);
            append_string(&mut out, &session.owner);
            append_string(&mut out, &session.endpoint);
            append_u64(&mut out, session.epoch);
            append_u64(&mut out, session.acquire_time_ms);
            append_u64(&mut out, session.last_renew_ms);
            append_u64(&mut out, session.expire_time_ms);
            append_u64(&mut out, session.ttl_ms);
        }
        append_u64(&mut out, self.coordinator_session_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.coordinator_session_receipts {
            append_string(&mut out, idempotency_key);
            append_coordinator_session_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.shards.len() as u64);
        for (shard_id, shard) in &self.shards {
            append_string(&mut out, shard_id);
            append_string(&mut out, &shard.start_key);
            append_string(&mut out, &shard.end_key);
            append_string(&mut out, &shard.owner_node_id);
            append_u64(&mut out, shard.epoch);
            append_u64(&mut out, shard.draining as u64);
        }
        append_u64(&mut out, self.shard_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.shard_receipts {
            append_string(&mut out, idempotency_key);
            append_metadata_shard_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.batch_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.batch_request_receipts {
            append_string(&mut out, idempotency_key);
            append_metadata_batch_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.transactions.len() as u64);
        for (transaction_id, transaction) in &self.transactions {
            append_string(&mut out, transaction_id);
            append_string(&mut out, &transaction.owner);
            append_u64(&mut out, transaction.expire_time_ms);
            append_string(&mut out, &transaction.state);
            append_u64(&mut out, transaction.begin_time_ms);
            append_u64(&mut out, transaction.update_time_ms);
            append_u64(&mut out, transaction.mutations.len() as u64);
            for mutation in &transaction.mutations {
                append_string(&mut out, &mutation.key);
                append_string(&mut out, &mutation.value);
                append_u64(&mut out, mutation.expected_version);
                append_u64(&mut out, mutation.delete as u64);
            }
            append_u64(&mut out, transaction.read_versions.len() as u64);
            for (key, version) in &transaction.read_versions {
                append_string(&mut out, key);
                append_u64(&mut out, *version);
            }
        }
        append_u64(&mut out, self.transaction_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.transaction_receipts {
            append_string(&mut out, idempotency_key);
            append_metadata_transaction_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.change_checkpoints.len() as u64);
        for (checkpoint_key, checkpoint) in &self.change_checkpoints {
            append_string(&mut out, checkpoint_key);
            append_string(&mut out, &checkpoint.consumer_id);
            append_string(&mut out, &checkpoint.prefix);
            append_u64(&mut out, checkpoint.after_change_sequence);
            append_u64(&mut out, checkpoint.after_commit_index);
            append_u64(&mut out, checkpoint.update_time_ms);
            append_string(&mut out, &checkpoint.owner);
            append_string(&mut out, &checkpoint.note);
        }
        append_u64(&mut out, self.changes.len() as u64);
        for change in &self.changes {
            append_u64(&mut out, change.sequence);
            append_u64(&mut out, change.commit_index);
            append_string(&mut out, &change.key);
            append_u64(&mut out, change.version);
            append_u64(&mut out, change.deleted as u64);
            append_u64(&mut out, change.update_time_ms);
        }
        append_string(&mut out, &self.checksum);
        let envelope_checksum = etag(&out[snapshot_start..]);
        append_string(&mut out, &envelope_checksum);
    }

    pub(crate) fn encoded_size_hint(&self) -> usize {
        const FIXED_HEADER_BYTES: usize = 512;
        let record_bytes = self.records.iter().fold(0usize, |total, (key, record)| {
            total
                .saturating_add(encoded_string_len(key))
                .saturating_add(metadata_record_value_encoded_size(record))
        });
        let lease_bytes = self.leases.iter().fold(0usize, |total, (namespace, lease)| {
            total
                .saturating_add(encoded_string_len(namespace))
                .saturating_add(metadata_lease_value_encoded_size(lease))
        });
        let coordinator_session_bytes =
            self.coordinator_sessions
                .iter()
                .fold(0usize, |total, (session_id, session)| {
                    total
                        .saturating_add(encoded_string_len(session_id))
                        .saturating_add(coordinator_session_value_encoded_size(session))
                });
        let shard_bytes = self.shards.iter().fold(0usize, |total, (shard_id, shard)| {
            total
                .saturating_add(encoded_string_len(shard_id))
                .saturating_add(metadata_shard_value_encoded_size(shard))
        });
        let transaction_bytes =
            self.transactions
                .iter()
                .fold(0usize, |total, (transaction_id, transaction)| {
                    total
                        .saturating_add(encoded_string_len(transaction_id))
                        .saturating_add(metadata_transaction_value_encoded_size(transaction))
                });
        let checkpoint_bytes =
            self.change_checkpoints
                .iter()
                .fold(0usize, |total, (checkpoint_key, checkpoint)| {
                    total
                        .saturating_add(encoded_string_len(checkpoint_key))
                        .saturating_add(metadata_change_checkpoint_encoded_size(checkpoint))
                });
        let change_bytes = self.changes.iter().fold(0usize, |total, change| {
            total.saturating_add(metadata_change_encoded_size(change))
        });
        let coordinator_config_receipt_bytes =
            self.coordinator_config_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total
                        .saturating_add(encoded_string_len(idempotency_key))
                        .saturating_add(metadata_coordinator_config_receipt_encoded_size(receipt))
                });
        let lease_receipt_bytes =
            self.lease_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total
                        .saturating_add(encoded_string_len(idempotency_key))
                        .saturating_add(metadata_lease_receipt_encoded_size(receipt))
                });
        let coordinator_session_receipt_bytes =
            self.coordinator_session_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total
                        .saturating_add(encoded_string_len(idempotency_key))
                        .saturating_add(coordinator_session_receipt_encoded_size(receipt))
                });
        let shard_receipt_bytes =
            self.shard_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total
                        .saturating_add(encoded_string_len(idempotency_key))
                        .saturating_add(metadata_shard_receipt_encoded_size(receipt))
                });
        let batch_receipt_bytes =
            self.batch_request_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total
                        .saturating_add(encoded_string_len(idempotency_key))
                        .saturating_add(metadata_batch_receipt_encoded_size(receipt))
                });
        let transaction_receipt_bytes =
            self.transaction_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total
                        .saturating_add(encoded_string_len(idempotency_key))
                        .saturating_add(metadata_transaction_receipt_encoded_size(receipt))
                });
        FIXED_HEADER_BYTES
            .saturating_add(record_bytes)
            .saturating_add(lease_bytes)
            .saturating_add(coordinator_session_bytes)
            .saturating_add(shard_bytes)
            .saturating_add(transaction_bytes)
            .saturating_add(checkpoint_bytes)
            .saturating_add(change_bytes)
            .saturating_add(coordinator_config_receipt_bytes)
            .saturating_add(lease_receipt_bytes)
            .saturating_add(coordinator_session_receipt_bytes)
            .saturating_add(shard_receipt_bytes)
            .saturating_add(batch_receipt_bytes)
            .saturating_add(transaction_receipt_bytes)
            .saturating_add(metadata_coordinator_config_encoded_size(&self.coordinator_config))
            .saturating_add(metadata_admission_policy_encoded_size(&self.admission_policy))
            .saturating_add(metadata_qos_config_encoded_size(&self.qos_config))
            .saturating_add(metadata_qos_state_encoded_size(&self.qos_state))
            .saturating_add(encoded_string_len(&self.checksum))
            .saturating_add(64)
    }
}

fn metadata_admission_policy_encoded_size(_: &MetadataAdmissionPolicy) -> usize {
    8 * std::mem::size_of::<u64>()
}

fn metadata_qos_config_encoded_size(_: &MetadataQosConfig) -> usize {
    8 * std::mem::size_of::<u64>()
}

fn metadata_qos_state_encoded_size(_: &MetadataQosState) -> usize {
    8 * std::mem::size_of::<u64>()
}

fn metadata_coordinator_config_encoded_size(config: &MetadataCoordinatorConfig) -> usize {
    config
        .endpoints
        .iter()
        .fold(
            encoded_string_len(&config.backend)
                .saturating_add(std::mem::size_of::<u64>())
                .saturating_add(encoded_string_len(&config.namespace_path))
                .saturating_add(2 * std::mem::size_of::<u64>()),
            |total, endpoint| total.saturating_add(encoded_string_len(endpoint)),
        )
}

fn metadata_record_encoded_size(record: &MetadataRecord) -> usize {
    encoded_string_len(&record.key)
        .saturating_add(metadata_record_value_encoded_size(record))
}

fn metadata_record_value_encoded_size(record: &MetadataRecord) -> usize {
    encoded_string_len(&record.value)
        .saturating_add(3 * std::mem::size_of::<u64>())
}

fn metadata_lease_encoded_size(lease: &MetadataLease) -> usize {
    encoded_string_len(&lease.namespace)
        .saturating_add(metadata_lease_value_encoded_size(lease))
}

fn metadata_lease_value_encoded_size(lease: &MetadataLease) -> usize {
    encoded_string_len(&lease.owner).saturating_add(3 * std::mem::size_of::<u64>())
}

fn coordinator_session_encoded_size(session: &CoordinatorSession) -> usize {
    encoded_string_len(&session.session_id)
        .saturating_add(coordinator_session_value_encoded_size(session))
}

fn coordinator_session_value_encoded_size(session: &CoordinatorSession) -> usize {
    encoded_string_len(&session.owner)
        .saturating_add(encoded_string_len(&session.endpoint))
        .saturating_add(5 * std::mem::size_of::<u64>())
}

fn metadata_shard_encoded_size(shard: &MetadataShard) -> usize {
    encoded_string_len(&shard.shard_id)
        .saturating_add(metadata_shard_value_encoded_size(shard))
}

fn metadata_shard_value_encoded_size(shard: &MetadataShard) -> usize {
    encoded_string_len(&shard.start_key)
        .saturating_add(encoded_string_len(&shard.end_key))
        .saturating_add(encoded_string_len(&shard.owner_node_id))
        .saturating_add(2 * std::mem::size_of::<u64>())
}

fn metadata_mutation_encoded_size(mutation: &MetadataMutation) -> usize {
    encoded_string_len(&mutation.key)
        .saturating_add(encoded_string_len(&mutation.value))
        .saturating_add(2 * std::mem::size_of::<u64>())
}

fn metadata_transaction_encoded_size(transaction: &MetadataTransaction) -> usize {
    encoded_string_len(&transaction.transaction_id)
        .saturating_add(metadata_transaction_value_encoded_size(transaction))
}

fn metadata_transaction_value_encoded_size(transaction: &MetadataTransaction) -> usize {
    let mutation_bytes = transaction.mutations.iter().fold(0usize, |total, mutation| {
        total.saturating_add(metadata_mutation_encoded_size(mutation))
    });
    let read_version_bytes = transaction
        .read_versions
        .keys()
        .fold(0usize, |total, key| {
            total
                .saturating_add(encoded_string_len(key))
                .saturating_add(std::mem::size_of::<u64>())
        });
    encoded_string_len(&transaction.owner)
        .saturating_add(encoded_string_len(&transaction.state))
        .saturating_add(5 * std::mem::size_of::<u64>())
        .saturating_add(mutation_bytes)
        .saturating_add(read_version_bytes)
}

fn metadata_change_checkpoint_encoded_size(checkpoint: &MetadataChangeCheckpoint) -> usize {
    encoded_string_len(&checkpoint.consumer_id)
        .saturating_add(encoded_string_len(&checkpoint.prefix))
        .saturating_add(3 * std::mem::size_of::<u64>())
        .saturating_add(encoded_string_len(&checkpoint.owner))
        .saturating_add(encoded_string_len(&checkpoint.note))
}

fn metadata_change_encoded_size(change: &MetadataChange) -> usize {
    (5 * std::mem::size_of::<u64>()).saturating_add(encoded_string_len(&change.key))
}

fn metadata_coordinator_config_receipt_encoded_size(
    receipt: &MetadataCoordinatorConfigReceipt,
) -> usize {
    encoded_string_len(&receipt.idempotency_key)
        .saturating_add(encoded_string_len(&receipt.request_checksum))
        .saturating_add(metadata_coordinator_config_encoded_size(&receipt.config))
        .saturating_add(2 * std::mem::size_of::<u64>())
}

fn metadata_lease_receipt_encoded_size(receipt: &MetadataLeaseReceipt) -> usize {
    encoded_string_len(&receipt.idempotency_key)
        .saturating_add(encoded_string_len(&receipt.operation))
        .saturating_add(encoded_string_len(&receipt.request_checksum))
        .saturating_add(metadata_lease_encoded_size(&receipt.lease))
        .saturating_add(3 * std::mem::size_of::<u64>())
}

fn coordinator_session_receipt_encoded_size(receipt: &CoordinatorSessionReceipt) -> usize {
    encoded_string_len(&receipt.idempotency_key)
        .saturating_add(encoded_string_len(&receipt.operation))
        .saturating_add(encoded_string_len(&receipt.request_checksum))
        .saturating_add(coordinator_session_encoded_size(&receipt.session))
        .saturating_add(3 * std::mem::size_of::<u64>())
}

fn metadata_shard_receipt_encoded_size(receipt: &MetadataShardReceipt) -> usize {
    receipt.shards.iter().fold(
        encoded_string_len(&receipt.idempotency_key)
            .saturating_add(encoded_string_len(&receipt.operation))
            .saturating_add(encoded_string_len(&receipt.request_checksum))
            .saturating_add(3 * std::mem::size_of::<u64>()),
        |total, shard| total.saturating_add(metadata_shard_encoded_size(shard)),
    )
}

fn metadata_batch_receipt_encoded_size(receipt: &MetadataBatchReceipt) -> usize {
    receipt.records.iter().fold(
        encoded_string_len(&receipt.idempotency_key)
            .saturating_add(encoded_string_len(&receipt.request_checksum))
            .saturating_add(8 * std::mem::size_of::<u64>())
            .saturating_add(encoded_string_len(&receipt.expected_owner))
            .saturating_add(encoded_string_len(
                receipt.lease_owner.as_deref().unwrap_or_default(),
            )),
        |total, record| total.saturating_add(metadata_record_encoded_size(record)),
    )
}

fn metadata_transaction_receipt_encoded_size(receipt: &MetadataTransactionReceipt) -> usize {
    receipt.records.iter().fold(
        encoded_string_len(&receipt.idempotency_key)
            .saturating_add(encoded_string_len(&receipt.operation))
            .saturating_add(encoded_string_len(&receipt.request_checksum))
            .saturating_add(metadata_transaction_encoded_size(&receipt.transaction))
            .saturating_add(3 * std::mem::size_of::<u64>()),
        |total, record| total.saturating_add(metadata_record_encoded_size(record)),
    )
}
