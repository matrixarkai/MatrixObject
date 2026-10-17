impl MatrixObjectStore {
    pub fn check_meta_shard_owner_serving(
        &self,
        key: &str,
        expected_owner: &str,
        expected_epoch: u64,
        now_ms: u64,
    ) -> Result<MetadataShard, ObjectError> {
        self.meta_service
            .check_shard_owner_serving(key, expected_owner, expected_epoch, now_ms)
    }

    pub fn metadata_record(&self, key: &str) -> Result<MetadataRecord, ObjectError> {
        self.meta_service.get_record(key)
    }

    pub fn metadata_record_with_metadata_read(
        &self,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(MetadataRecord, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(key, &options)?;
        let record = self.metadata_record(key)?;
        Ok((record, readiness))
    }

    pub fn metadata_records_batch_with_metadata_read(
        &self,
        keys: Vec<String>,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(MetadataRecordBatchRead, MetadataRaftReadiness), ObjectError> {
        if keys.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata batch get must contain at least one key".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        let mut readiness = None;
        for key in &keys {
            validate_meta_key(key)?;
            if !seen.insert(key.clone()) {
                return Err(ObjectError::InvalidArgument(
                    "metadata batch get contains duplicate keys".to_string(),
                ));
            }
            readiness = Some(self.prepare_metadata_read(key, &options)?);
        }

        let requested_records = keys.len() as u64;
        let mut report = MetadataRecordBatchRead {
            requested_records,
            ..MetadataRecordBatchRead::default()
        };
        for key in keys {
            match self.metadata_record(&key) {
                Ok(record) => {
                    let payload_bytes = metadata_record_payload_bytes(&record);
                    if max_payload_bytes > 0
                        && report.payload_bytes.saturating_add(payload_bytes) > max_payload_bytes
                    {
                        return Err(ObjectError::InvalidArgument(
                            "metadata batch get exceeds max_payload_bytes".to_string(),
                        ));
                    }
                    report.payload_bytes = report.payload_bytes.saturating_add(payload_bytes);
                    report.found_records = report.found_records.saturating_add(1);
                    report.records.push(record);
                }
                Err(ObjectError::NotFound(_)) => {
                    report.missing_records = report.missing_records.saturating_add(1);
                }
                Err(err) => return Err(err),
            }
        }
        Ok((report, readiness.unwrap_or_default()))
    }

    pub fn check_metadata_read_set(
        &self,
        conditions: Vec<MetadataReadCondition>,
    ) -> Result<MetadataReadSetReport, ObjectError> {
        self.meta_service.check_read_set(&conditions)
    }

    pub fn check_metadata_read_set_with_metadata_read(
        &self,
        conditions: Vec<MetadataReadCondition>,
        options: MetadataReadOptions,
    ) -> Result<(MetadataReadSetReport, MetadataRaftReadiness), ObjectError> {
        if conditions.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata read set must contain at least one condition".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        let mut readiness = None;
        for condition in &conditions {
            validate_meta_key(&condition.key)?;
            if !seen.insert(condition.key.clone()) {
                return Err(ObjectError::InvalidArgument(
                    "metadata read set contains duplicate keys".to_string(),
                ));
            }
            if condition.require_present && condition.require_deleted {
                return Err(ObjectError::InvalidArgument(
                    "metadata read set condition cannot require present and deleted".to_string(),
                ));
            }
            readiness = Some(self.prepare_metadata_read(&condition.key, &options)?);
        }
        let report = self.check_metadata_read_set(conditions)?;
        Ok((report, readiness.unwrap_or_default()))
    }
}
