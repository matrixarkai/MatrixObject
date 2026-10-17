impl MetaSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.next_commit_index == 0 {
            return Err(ObjectError::Corruption(
                "metadata snapshot next commit index is invalid".to_string(),
            ));
        }
        if self.next_change_sequence == 0 {
            return Err(ObjectError::Corruption(
                "metadata snapshot next change sequence is invalid".to_string(),
            ));
        }
        if self.last_tombstone_compaction_commit_index > self.next_commit_index {
            return Err(ObjectError::Corruption(
                "metadata snapshot tombstone compaction index is invalid".to_string(),
            ));
        }
        if self.raft_snapshot_floor_commit_index > self.next_commit_index {
            return Err(ObjectError::Corruption(
                "metadata snapshot raft compaction floor is invalid".to_string(),
            ));
        }
        validate_meta_retention_limit(
            "metadata snapshot change log retention limit",
            self.change_log_retention_limit,
        )?;
        validate_meta_retention_limit(
            "metadata snapshot mutation receipt retention limit",
            self.mutation_receipt_retention_limit,
        )?;
        validate_metadata_admission_policy(&self.admission_policy)?;
        validate_metadata_qos_config(&self.qos_config)?;
        validate_metadata_coordinator_config(&self.coordinator_config)?;
        if self.checksum != self.checksum_material_hash() {
            return Err(ObjectError::Corruption(
                "metadata snapshot checksum mismatch".to_string(),
            ));
        }
        for (key, record) in &self.records {
            validate_meta_key(key)?;
            if record.key != *key || record.version == 0 {
                return Err(ObjectError::Corruption(
                    "metadata snapshot record is invalid".to_string(),
                ));
            }
        }
        for (namespace, lease) in &self.leases {
            validate_meta_key(namespace)?;
            if lease.namespace != *namespace || lease.owner.is_empty() || lease.epoch == 0 {
                return Err(ObjectError::Corruption(
                    "metadata snapshot lease is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.lease_receipts {
            validate_idempotency_key(idempotency_key)?;
            validate_meta_key(&receipt.lease.namespace)?;
            validate_meta_key(&receipt.lease.owner)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.commit_index == 0
                || receipt.commit_index >= self.next_commit_index
                || receipt.lease.epoch == 0
                || !matches!(receipt.operation.as_str(), "acquire" | "renew" | "release")
                || (receipt.released && receipt.operation != "release")
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot lease receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.coordinator_config_receipts {
            validate_idempotency_key(idempotency_key)?;
            validate_metadata_coordinator_config(&receipt.config)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum
                    != metadata_coordinator_config_request_checksum(&receipt.config)
                || receipt.commit_index == 0
                || receipt.commit_index >= self.next_commit_index
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot coordinator config receipt is invalid".to_string(),
                ));
            }
        }
        for (session_id, session) in &self.coordinator_sessions {
            validate_session_id(session_id)?;
            if session.session_id != *session_id
                || session.owner.is_empty()
                || session.endpoint.is_empty()
                || session.epoch == 0
                || session.ttl_ms == 0
                || session.last_renew_ms < session.acquire_time_ms
                || session.expire_time_ms < session.last_renew_ms
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot coordinator session is invalid".to_string(),
                ));
            }
            validate_meta_key(&session.owner)?;
            validate_session_endpoint(&session.endpoint)?;
        }
        for (idempotency_key, receipt) in &self.coordinator_session_receipts {
            validate_idempotency_key(idempotency_key)?;
            validate_session_id(&receipt.session.session_id)?;
            validate_meta_key(&receipt.session.owner)?;
            validate_session_endpoint(&receipt.session.endpoint)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.commit_index == 0
                || receipt.commit_index >= self.next_commit_index
                || receipt.session.epoch == 0
                || receipt.session.ttl_ms == 0
                || receipt.session.last_renew_ms < receipt.session.acquire_time_ms
                || receipt.session.expire_time_ms < receipt.session.last_renew_ms
                || !matches!(receipt.operation.as_str(), "register" | "renew" | "release")
                || (receipt.released && receipt.operation != "release")
                || (!receipt.released && receipt.operation == "release")
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot coordinator session receipt is invalid".to_string(),
                ));
            }
        }
        for (shard_id, shard) in &self.shards {
            validate_meta_key(shard_id)?;
            if shard.shard_id != *shard_id || shard.epoch == 0 {
                return Err(ObjectError::Corruption(
                    "metadata snapshot shard is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.shard_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.commit_index == 0
                || receipt.commit_index >= self.next_commit_index
                || receipt.shards.is_empty()
                || !matches!(
                    receipt.operation.as_str(),
                    "configure" | "split" | "merge" | "migrate"
                )
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot shard receipt is invalid".to_string(),
                ));
            }
            for shard in &receipt.shards {
                validate_meta_key(&shard.shard_id)?;
                validate_meta_key(&shard.owner_node_id)?;
                if shard.epoch == 0
                    || (!shard.end_key.is_empty() && shard.start_key >= shard.end_key)
                {
                    return Err(ObjectError::Corruption(
                        "metadata snapshot shard receipt image is invalid".to_string(),
                    ));
                }
            }
        }
        for (idempotency_key, receipt) in &self.batch_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.records.is_empty()
                || receipt.first_commit_index == 0
                || receipt.last_commit_index < receipt.first_commit_index
                || receipt.last_commit_index >= self.next_commit_index
                || (receipt.owner_fenced && receipt.expected_epoch == 0)
                || (!receipt.owner_fenced
                    && (!receipt.expected_owner.is_empty() || receipt.expected_epoch != 0))
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot batch receipt is invalid".to_string(),
                ));
            }
            if receipt.owner_fenced {
                validate_meta_key(&receipt.expected_owner)?;
            }
            if let Some(owner) = &receipt.lease_owner {
                validate_meta_key(owner)?;
            }
            for record in &receipt.records {
                validate_meta_key(&record.key)?;
                if record.version == 0 {
                    return Err(ObjectError::Corruption(
                        "metadata snapshot batch receipt record is invalid".to_string(),
                    ));
                }
            }
        }
        for (transaction_id, transaction) in &self.transactions {
            validate_meta_key(transaction_id)?;
            if transaction.transaction_id != *transaction_id
                || transaction.owner.is_empty()
                || transaction.expire_time_ms == 0
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot transaction is invalid".to_string(),
                ));
            }
            for mutation in &transaction.mutations {
                validate_meta_key(&mutation.key)?;
            }
            for (key, version) in &transaction.read_versions {
                validate_meta_key(key)?;
                if *version == u64::MAX {
                    return Err(ObjectError::Corruption(
                        "metadata snapshot transaction read version is invalid".to_string(),
                    ));
                }
            }
        }
        for (idempotency_key, receipt) in &self.transaction_receipts {
            validate_idempotency_key(idempotency_key)?;
            validate_meta_key(&receipt.transaction.transaction_id)?;
            validate_meta_key(&receipt.transaction.owner)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.commit_index == 0
                || receipt.commit_index >= self.next_commit_index
                || !matches!(
                    receipt.operation.as_str(),
                    "begin" | "renew" | "commit" | "abort"
                )
                || receipt.transaction.expire_time_ms == 0
                || receipt.transaction.update_time_ms > receipt.update_time_ms
                || (receipt.operation == "commit" && receipt.transaction.state != "committed")
                || (receipt.operation == "abort" && receipt.transaction.state != "aborted")
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot transaction receipt is invalid".to_string(),
                ));
            }
            for mutation in &receipt.transaction.mutations {
                validate_meta_key(&mutation.key)?;
            }
            for (key, version) in &receipt.transaction.read_versions {
                validate_meta_key(key)?;
                if *version == u64::MAX {
                    return Err(ObjectError::Corruption(
                        "metadata snapshot transaction receipt read version is invalid".to_string(),
                    ));
                }
            }
            for record in &receipt.records {
                validate_meta_key(&record.key)?;
                if record.version == 0 {
                    return Err(ObjectError::Corruption(
                        "metadata snapshot transaction receipt record is invalid".to_string(),
                    ));
                }
            }
        }
        for (checkpoint_key, checkpoint) in &self.change_checkpoints {
            validate_meta_key(checkpoint_key)?;
            if *checkpoint_key != change_checkpoint_key(&checkpoint.consumer_id, &checkpoint.prefix)
                || checkpoint.consumer_id.is_empty()
                || checkpoint.prefix.is_empty()
                || checkpoint.owner.is_empty()
                || checkpoint.after_change_sequence >= self.next_change_sequence
                || checkpoint.after_commit_index > self.next_commit_index
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot change checkpoint is invalid".to_string(),
                ));
            }
        }
        let mut previous_sequence = 0;
        for change in &self.changes {
            if change.sequence <= previous_sequence || change.sequence >= self.next_change_sequence
            {
                return Err(ObjectError::Corruption(
                    "metadata snapshot change sequence is invalid".to_string(),
                ));
            }
            validate_meta_key(&change.key)?;
            previous_sequence = change.sequence;
        }
        Ok(())
    }

}
