/// What one `apply_mutation` changed, enough to reverse it exactly.
#[derive(Clone, Debug)]
pub(crate) struct MetaRecordUndo {
    key: String,
    previous_record: Option<MetadataRecord>,
    changes_len: usize,
    next_commit_index: u64,
    next_change_sequence: u64,
}

impl MatrixMetaService {
    fn preflight_mutations(
        &self,
        mutations: &[MetadataMutation],
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<BTreeMap<String, u64>, ObjectError> {
        if mutations.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata batch must contain at least one mutation".to_string(),
            ));
        }
        self.check_mutation_admission(mutations)?;
        let mut read_versions = BTreeMap::new();
        let mut projected_versions = BTreeMap::new();
        for mutation in mutations {
            validate_meta_key(&mutation.key)?;
            self.check_shard_fence(&mutation.key)?;
            self.check_lease_fence(&mutation.key, lease_owner, lease_epoch, now_ms)?;
            let base_version = self
                .records
                .get(&mutation.key)
                .map_or(0, |record| record.version);
            read_versions
                .entry(mutation.key.to_string())
                .or_insert(base_version);
            let current_version = projected_versions
                .get(&mutation.key)
                .copied()
                .unwrap_or(base_version);
            if current_version != mutation.expected_version {
                return Err(ObjectError::PreconditionFailed(
                    "metadata expected version mismatch".to_string(),
                ));
            }
            projected_versions.insert(mutation.key.clone(), current_version.saturating_add(1));
        }
        Ok(read_versions)
    }

    fn metadata_qos_enabled(&self) -> bool {
        self.qos_config.max_write_ops_per_window > 0
            || self.qos_config.max_write_bytes_per_window > 0
    }

    pub(crate) fn metadata_write_payload_bytes(
        mutations: &[MetadataMutation],
    ) -> Result<u64, ObjectError> {
        mutations.iter().try_fold(0_u64, |total, mutation| {
            total
                .checked_add(mutation.key.len() as u64)
                .and_then(|total| total.checked_add(mutation.value.len() as u64))
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("metadata qos payload bytes overflow".to_string())
                })
        })
    }

    fn admit_metadata_qos(
        &mut self,
        write_ops: u64,
        write_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if !self.metadata_qos_enabled() {
            return Ok(());
        }
        let window_ms = self.qos_config.window_ms;
        if self.qos_state.window_start_ms == 0
            || now_ms < self.qos_state.window_start_ms
            || now_ms.saturating_sub(self.qos_state.window_start_ms) >= window_ms
        {
            self.qos_state.window_start_ms = now_ms;
            self.qos_state.write_ops_in_window = 0;
            self.qos_state.write_bytes_in_window = 0;
        }
        let projected_ops = self
            .qos_state
            .write_ops_in_window
            .checked_add(write_ops)
            .ok_or_else(|| ObjectError::InvalidArgument("metadata qos op overflow".to_string()))?;
        if self.qos_config.max_write_ops_per_window > 0
            && projected_ops > self.qos_config.max_write_ops_per_window
        {
            self.qos_state.throttled_write_ops =
                self.qos_state.throttled_write_ops.saturating_add(write_ops);
            return Err(ObjectError::PreconditionFailed(
                "metadata qos throttled: write operation window exceeded".to_string(),
            ));
        }
        let projected_bytes = self
            .qos_state
            .write_bytes_in_window
            .checked_add(write_bytes)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("metadata qos byte overflow".to_string())
            })?;
        if self.qos_config.max_write_bytes_per_window > 0
            && projected_bytes > self.qos_config.max_write_bytes_per_window
        {
            self.qos_state.throttled_write_bytes = self
                .qos_state
                .throttled_write_bytes
                .saturating_add(write_bytes);
            return Err(ObjectError::PreconditionFailed(
                "metadata qos throttled: write byte window exceeded".to_string(),
            ));
        }
        self.qos_state.write_ops_in_window = projected_ops;
        self.qos_state.write_bytes_in_window = projected_bytes;
        Ok(())
    }

    fn preflight_owner_fenced_mutations(
        &self,
        mutations: &[MetadataMutation],
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<BTreeMap<String, u64>, ObjectError> {
        if mutations.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata owner-fenced batch must contain at least one mutation".to_string(),
            ));
        }
        self.check_mutation_admission(mutations)?;
        let mut read_versions = BTreeMap::new();
        let mut projected_versions = BTreeMap::new();
        for mutation in mutations {
            validate_meta_key(&mutation.key)?;
            self.check_shard_owner_serving(&mutation.key, expected_owner, expected_epoch, now_ms)?;
            self.check_lease_fence(&mutation.key, lease_owner, lease_epoch, now_ms)?;
            let base_version = self
                .records
                .get(&mutation.key)
                .map_or(0, |record| record.version);
            read_versions
                .entry(mutation.key.to_string())
                .or_insert(base_version);
            let current_version = projected_versions
                .get(&mutation.key)
                .copied()
                .unwrap_or(base_version);
            if current_version != mutation.expected_version {
                return Err(ObjectError::PreconditionFailed(
                    "metadata expected version mismatch".to_string(),
                ));
            }
            projected_versions.insert(mutation.key.clone(), current_version.saturating_add(1));
        }
        Ok(read_versions)
    }

    fn check_transaction_read_versions(
        &self,
        transaction: &MetadataTransaction,
    ) -> Result<(), ObjectError> {
        for mutation in &transaction.mutations {
            if !transaction.read_versions.contains_key(mutation.key.as_str()) {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction read set is incomplete".to_string(),
                ));
            }
        }
        for (key, expected_version) in &transaction.read_versions {
            validate_meta_key(key)?;
            self.check_shard_fence(key)?;
            let current_version = self
                    .records
                    .get(key.as_str())
                    .map_or(0, |record| record.version);
            if current_version != *expected_version {
                return Err(ObjectError::PreconditionFailed(
                    "metadata transaction read set changed".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Everything `apply_mutation` changes, so one record commit can be undone
    /// without copying the service.
    pub(crate) fn capture_record_undo(&self, key: &str) -> MetaRecordUndo {
        MetaRecordUndo {
            previous_record: self.records.get(key).cloned(),
            key: key.to_string(),
            changes_len: self.changes.len(),
            next_commit_index: self.next_commit_index,
            next_change_sequence: self.next_change_sequence,
        }
    }

    pub(crate) fn restore_record_undo(&mut self, undo: MetaRecordUndo) {
        match undo.previous_record {
            Some(record) => {
                self.records.insert(undo.key.into(), record);
            }
            None => {
                self.records.remove(undo.key.as_str());
            }
        }
        // `apply_mutation` only appends here.
        self.changes.truncate(undo.changes_len);
        self.next_commit_index = undo.next_commit_index;
        self.next_change_sequence = undo.next_change_sequence;
    }

    fn apply_mutation(
        &mut self,
        mutation: MetadataMutation,
        now_ms: u64,
    ) -> Result<MetadataRecord, ObjectError> {
        let current_version = self
            .records
            .get(&mutation.key)
            .map_or(0, |record| record.version);
        if current_version != mutation.expected_version {
            return Err(ObjectError::PreconditionFailed(
                "metadata expected version mismatch".to_string(),
            ));
        }
        let record = MetadataRecord {
            key: mutation.key.clone(),
            value: mutation.value,
            version: current_version.saturating_add(1),
            update_time_ms: now_ms,
            deleted: mutation.delete,
        };
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        self.records.insert(mutation.key.clone(), record.clone());
        self.changes.push(MetadataChange {
            sequence: self.next_change_sequence,
            commit_index: self.next_commit_index.saturating_sub(1),
            key: mutation.key,
            version: record.version,
            deleted: record.deleted,
            update_time_ms: now_ms,
        });
        self.next_change_sequence = self.next_change_sequence.saturating_add(1);
        Ok(record)
    }

    pub(crate) fn check_shard_fence(&self, key: &str) -> Result<(), ObjectError> {
        if self.shards.is_empty() {
            return Ok(());
        }
        let shard = self.find_shard_for_key(key)?;
        if shard.draining {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard is draining".to_string(),
            ));
        }
        Ok(())
    }

    fn find_shard_for_key(&self, key: &str) -> Result<MetadataShard, ObjectError> {
        self.shards
            .values()
            .find(|shard| {
                key >= shard.start_key.as_str()
                    && (shard.end_key.is_empty() || key < shard.end_key.as_str())
            })
            .cloned()
            .ok_or_else(|| ObjectError::PreconditionFailed("metadata shard missing".to_string()))
    }

    pub(crate) fn check_lease_fence(
        &self,
        key: &str,
        owner: Option<&str>,
        epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        let active = self
            .leases
            .values()
            .filter(|lease| lease.expire_time_ms > now_ms && key.starts_with(&lease.namespace))
            .max_by(|left, right| left.namespace.len().cmp(&right.namespace.len()));
        let Some(lease) = active else {
            return Ok(());
        };
        if owner == Some(lease.owner.as_str()) && epoch == Some(lease.epoch) {
            Ok(())
        } else {
            Err(ObjectError::PreconditionFailed(
                "metadata lease fence mismatch".to_string(),
            ))
        }
    }
}
