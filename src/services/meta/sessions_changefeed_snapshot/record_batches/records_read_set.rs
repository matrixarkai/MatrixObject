impl MatrixMetaService {
    pub fn commit_record(
        &mut self,
        key: &str,
        value: String,
        expected_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<MetadataRecord, ObjectError> {
        validate_meta_key(key)?;
        self.check_shard_fence(key)?;
        self.check_lease_fence(key, lease_owner, lease_epoch, now_ms)?;
        self.apply_mutation(
            MetadataMutation {
                key: key.to_string().into(),
                value,
                expected_version,
                delete: false,
            },
            now_ms,
        )
    }

    pub fn delete_record(
        &mut self,
        key: &str,
        expected_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<MetadataRecord, ObjectError> {
        validate_meta_key(key)?;
        self.check_shard_fence(key)?;
        self.check_lease_fence(key, lease_owner, lease_epoch, now_ms)?;
        self.apply_mutation(
            MetadataMutation {
                key: key.to_string().into(),
                value: String::new(),
                expected_version,
                delete: true,
            },
            now_ms,
        )
    }

    pub fn get_record(&self, key: &str) -> Result<MetadataRecord, ObjectError> {
        self.records
            .get(key)
            .filter(|record| !record.deleted)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(format!("metadata record {}", key)))
    }

    pub fn scan_records(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataRecordPage, ObjectError> {
        validate_meta_key(prefix)?;
        if let Some(after_key) = after_key {
            validate_meta_key(after_key)?;
        }
        if max_records == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata scan max_records must be positive".to_string(),
            ));
        }
        let mut page = MetadataRecordPage::default();
        let mut next_key = None;
        for (key, record) in self.records.range::<str, _>((
                std::ops::Bound::Included(prefix),
                std::ops::Bound::Unbounded,
            )) {
            if !key.starts_with(prefix) {
                break;
            }
            if after_key.map_or(false, |marker| key.as_str() <= marker) {
                continue;
            }
            if record.deleted {
                continue;
            }
            page.scanned_records = page.scanned_records.saturating_add(1);
            if page.records.len() >= max_records {
                next_key = Some(key.clone());
                break;
            }
            let record_payload = metadata_record_payload_bytes(record);
            if max_payload_bytes > 0
                && !page.records.is_empty()
                && page.payload_bytes.saturating_add(record_payload) > max_payload_bytes
            {
                next_key = Some(key.clone());
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(record_payload);
            page.next_key_marker = Some(key.to_string());
            page.records.push(record.clone());
        }
        page.has_more = next_key.is_some();
        Ok(page)
    }

    pub fn check_read_set(
        &self,
        conditions: &[MetadataReadCondition],
    ) -> Result<MetadataReadSetReport, ObjectError> {
        if conditions.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata read set must contain at least one condition".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        let mut report = MetadataReadSetReport::default();
        for condition in conditions {
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
            self.check_shard_fence(&condition.key)?;
            report.checked_records = report.checked_records.saturating_add(1);
            let current = self.records.get(condition.key.as_str());
            let current_version = current.map_or(0, |record| record.version);
            if current_version != condition.expected_version {
                return Err(ObjectError::PreconditionFailed(
                    "metadata read set version changed".to_string(),
                ));
            }
            match current {
                Some(record) if record.deleted => {
                    report.deleted_records = report.deleted_records.saturating_add(1);
                    if condition.require_present {
                        return Err(ObjectError::PreconditionFailed(
                            "metadata read set expected live record".to_string(),
                        ));
                    }
                    report.records.push(record.clone());
                }
                Some(record) => {
                    report.matched_records = report.matched_records.saturating_add(1);
                    if condition.require_deleted {
                        return Err(ObjectError::PreconditionFailed(
                            "metadata read set expected deleted record".to_string(),
                        ));
                    }
                    report.records.push(record.clone());
                }
                None => {
                    report.missing_records = report.missing_records.saturating_add(1);
                    if condition.require_present || condition.require_deleted {
                        return Err(ObjectError::PreconditionFailed(
                            "metadata read set expected existing record".to_string(),
                        ));
                    }
                }
            }
            report.max_version = report.max_version.max(current_version);
        }
        Ok(report)
    }
}
