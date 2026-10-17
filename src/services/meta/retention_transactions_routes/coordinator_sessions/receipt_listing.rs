impl MatrixMetaService {
    pub fn list_coordinator_session_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        session_id: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        max_payload_bytes: u64,
    ) -> Result<CoordinatorSessionReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(operation) = operation {
            match operation {
                "register" | "renew" | "release" => {}
                _ => {
                    return Err(ObjectError::InvalidArgument(
                        "coordinator session receipt operation filter must be register, renew, or release"
                            .to_string(),
                    ));
                }
            }
        }
        if let Some(session_id) = session_id {
            validate_session_id(session_id)?;
        }
        if let Some(owner) = owner {
            validate_meta_key(owner)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = CoordinatorSessionReceiptPage {
            receipts: Vec::new(),
            marker: marker.map(str::to_string),
            next_marker: None,
            operation: operation.map(str::to_string),
            session_id: session_id.map(str::to_string),
            owner: owner.map(str::to_string),
            released,
            total_receipt_count: self.coordinator_session_receipts.len() as u64,
            max_payload_bytes,
            ..CoordinatorSessionReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &CoordinatorSessionReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.coordinator_session_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.coordinator_session_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if operation.is_some_and(|operation| receipt.operation != operation) {
                page.skipped_operation_count = page.skipped_operation_count.saturating_add(1);
                continue;
            }
            if session_id.is_some_and(|session_id| receipt.session.session_id != session_id) {
                page.skipped_session_count = page.skipped_session_count.saturating_add(1);
                continue;
            }
            if owner.is_some_and(|owner| receipt.session.owner != owner) {
                page.skipped_owner_count = page.skipped_owner_count.saturating_add(1);
                continue;
            }
            if released.is_some_and(|expected| receipt.released != expected) {
                page.skipped_released_count = page.skipped_released_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                break;
            }
            let item_payload = coordinator_session_receipt_payload_bytes(receipt);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                if page.receipts.is_empty() {
                    return Err(ObjectError::InvalidArgument(format!(
                        "coordinator session receipt list first receipt exceeds max_payload_bytes: {idempotency_key}"
                    )));
                }
                page.truncated = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_marker = Some(idempotency_key.clone());
            page.receipts.push(receipt.clone());
            page.matched_count = page.matched_count.saturating_add(1);
        }
        page.receipt_count = page.receipts.len() as u64;
        if !page.truncated {
            page.next_marker = None;
        }
        Ok(page)
    }
}
