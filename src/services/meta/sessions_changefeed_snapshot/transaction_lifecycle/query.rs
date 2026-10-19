// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn begin_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
    ) -> Result<MetadataTransaction, ObjectError> {
        validate_meta_key(transaction_id)?;
        validate_meta_key(owner)?;
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata transaction ttl must be positive".to_string(),
            ));
        }
        if self.transactions.contains_key(transaction_id) {
            return Err(ObjectError::PreconditionFailed(
                "metadata transaction already exists".to_string(),
            ));
        }
        let write_bytes = Self::metadata_write_payload_bytes(&mutations)?
            .saturating_add(transaction_id.len() as u64)
            .saturating_add(owner.len() as u64);
        self.admit_metadata_qos(1, write_bytes, now_ms)?;
        self.check_open_transaction_admission(transaction_id)?;
        let read_versions = self.preflight_mutations(&mutations, None, None, now_ms)?;
        let transaction = MetadataTransaction {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            expire_time_ms: now_ms.saturating_add(ttl_ms),
            state: "pending".to_string(),
            mutations,
            read_versions,
            begin_time_ms: now_ms,
            update_time_ms: now_ms,
        };
        self.transactions
            .insert(transaction_id.to_string(), transaction.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(transaction)
    }

    pub fn get_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<MetadataTransaction, ObjectError> {
        validate_meta_key(transaction_id)?;
        self.transactions
            .get(transaction_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata transaction missing".to_string()))
    }

    pub fn list_transactions(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
    ) -> Result<MetadataTransactionPage, ObjectError> {
        if !prefix.is_empty() {
            validate_meta_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_meta_key(marker)?;
            if !prefix.is_empty() && !marker.starts_with(prefix) {
                return Err(ObjectError::InvalidArgument(
                    "metadata transaction list marker must be within prefix".to_string(),
                ));
            }
        }
        if let Some(owner) = owner {
            validate_meta_key(owner)?;
        }
        if let Some(state) = state {
            if !matches!(state, "pending" | "committed" | "aborted" | "expired") {
                return Err(ObjectError::InvalidArgument(
                    "metadata transaction state filter must be pending, committed, aborted, or expired"
                        .to_string(),
                ));
            }
        }
        let limit = limit.clamp(1, k_max_metadata_transaction_list_limit());
        let mut page = MetadataTransactionPage {
            prefix: prefix.to_string(),
            owner: owner.map(str::to_string),
            state: state.map(str::to_string),
            only_expired,
            include_terminal,
            limit: limit as u64,
            ..MetadataTransactionPage::default()
        };
        let start = marker.unwrap_or(prefix);
        let mut iter: Box<dyn Iterator<Item = (&String, &MetadataTransaction)> + '_> =
            if marker.is_some() {
                Box::new(self.transactions.range((
                    std::ops::Bound::Excluded(start.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else if prefix.is_empty() {
                Box::new(self.transactions.iter())
            } else {
                Box::new(self.transactions.range(prefix.to_string()..))
            };
        for (transaction_id, transaction) in iter.by_ref() {
            if !prefix.is_empty() && !transaction_id.starts_with(prefix) {
                break;
            }
            page.scanned_count = page.scanned_count.saturating_add(1);
            if !include_terminal && transaction.state != "pending" {
                page.skipped_terminal_count = page.skipped_terminal_count.saturating_add(1);
                continue;
            }
            if let Some(owner) = owner {
                if transaction.owner != owner {
                    page.skipped_owner_count = page.skipped_owner_count.saturating_add(1);
                    continue;
                }
            }
            if let Some(state) = state {
                if transaction.state != state {
                    page.skipped_state_count = page.skipped_state_count.saturating_add(1);
                    continue;
                }
            }
            if only_expired
                && (transaction.state != "pending" || transaction.expire_time_ms > now_ms)
            {
                page.skipped_not_expired_count = page.skipped_not_expired_count.saturating_add(1);
                continue;
            }
            page.matched_count = page.matched_count.saturating_add(1);
            if page.transactions.len() >= limit {
                page.truncated = true;
                break;
            }
            page.next_marker = Some(transaction_id.clone());
            page.transactions.push(transaction.clone());
        }
        if !page.truncated {
            page.next_marker = None;
        }
        Ok(page)
    }

    pub fn list_transaction_summaries(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        include_terminal: bool,
        owner: Option<&str>,
        state: Option<&str>,
        only_expired: bool,
        now_ms: u64,
        max_payload_bytes: u64,
    ) -> Result<MetadataTransactionSummaryPage, ObjectError> {
        if !prefix.is_empty() {
            validate_meta_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_meta_key(marker)?;
            if !prefix.is_empty() && !marker.starts_with(prefix) {
                return Err(ObjectError::InvalidArgument(
                    "metadata transaction summary marker must be within prefix".to_string(),
                ));
            }
        }
        if let Some(owner) = owner {
            validate_meta_key(owner)?;
        }
        if let Some(state) = state {
            if !matches!(state, "pending" | "committed" | "aborted" | "expired") {
                return Err(ObjectError::InvalidArgument(
                    "metadata transaction summary state filter must be pending, committed, aborted, or expired"
                        .to_string(),
                ));
            }
        }
        let limit = limit.clamp(1, k_max_metadata_transaction_list_limit());
        let mut page = MetadataTransactionSummaryPage {
            prefix: prefix.to_string(),
            marker: marker.map(str::to_string),
            next_marker: None,
            owner: owner.map(str::to_string),
            state: state.map(str::to_string),
            only_expired,
            include_terminal,
            total_transaction_count: self.transactions.len() as u64,
            max_payload_bytes,
            ..MetadataTransactionSummaryPage::default()
        };
        let start = marker.unwrap_or(prefix);
        let mut iter: Box<dyn Iterator<Item = (&String, &MetadataTransaction)> + '_> =
            if marker.is_some() {
                Box::new(self.transactions.range((
                    std::ops::Bound::Excluded(start.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else if prefix.is_empty() {
                Box::new(self.transactions.iter())
            } else {
                Box::new(self.transactions.range(prefix.to_string()..))
            };
        for (transaction_id, transaction) in iter.by_ref() {
            if !prefix.is_empty() && !transaction_id.starts_with(prefix) {
                break;
            }
            page.scanned_count = page.scanned_count.saturating_add(1);
            if !include_terminal && transaction.state != "pending" {
                page.skipped_terminal_count = page.skipped_terminal_count.saturating_add(1);
                continue;
            }
            if let Some(owner) = owner {
                if transaction.owner != owner {
                    page.skipped_owner_count = page.skipped_owner_count.saturating_add(1);
                    continue;
                }
            }
            if let Some(state) = state {
                if transaction.state != state {
                    page.skipped_state_count = page.skipped_state_count.saturating_add(1);
                    continue;
                }
            }
            if only_expired
                && (transaction.state != "pending" || transaction.expire_time_ms > now_ms)
            {
                page.skipped_not_expired_count = page.skipped_not_expired_count.saturating_add(1);
                continue;
            }
            if page.transactions.len() >= limit {
                page.truncated = true;
                break;
            }
            let summary = metadata_transaction_summary(transaction);
            let item_payload = metadata_transaction_summary_payload_bytes(&summary);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                if page.transactions.is_empty() {
                    return Err(ObjectError::InvalidArgument(format!(
                        "metadata transaction summary first item exceeds max_payload_bytes: {transaction_id}"
                    )));
                }
                page.truncated = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_marker = Some(transaction_id.clone());
            page.transactions.push(summary);
            page.matched_count = page.matched_count.saturating_add(1);
        }
        page.transaction_count = page.transactions.len() as u64;
        if !page.truncated {
            page.next_marker = None;
        }
        Ok(page)
    }


}
