#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatorSession {
    pub session_id: String,
    pub owner: String,
    pub endpoint: String,
    pub epoch: u64,
    pub acquire_time_ms: u64,
    pub last_renew_ms: u64,
    pub expire_time_ms: u64,
    pub ttl_ms: u64,
}

impl CoordinatorSession {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}session_id"), self.session_id.clone());
        fields.insert(format!("{field_prefix}owner"), self.owner.clone());
        fields.insert(format!("{field_prefix}endpoint"), self.endpoint.clone());
        fields.insert(format!("{field_prefix}epoch"), self.epoch.to_string());
        fields.insert(
            format!("{field_prefix}acquire_time_ms"),
            self.acquire_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}last_renew_ms"),
            self.last_renew_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}ttl_ms"), self.ttl_ms.to_string());
        fields
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatorSessionReceipt {
    pub idempotency_key: String,
    pub operation: String,
    pub request_checksum: String,
    pub session: CoordinatorSession,
    pub released: bool,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub deduped: bool,
}

impl CoordinatorSessionReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-COORDINATOR-SESSION-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        coordinator_session_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_coordinator_session_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_coordinator_session_receipt_proof_checksum =
            expected_coordinator_session_receipt_proof_checksum.as_ref();
        if expected_coordinator_session_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_coordinator_session_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata coordinator session receipt proof checksum mismatch: expected {} got {}",
                expected_coordinator_session_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .session
            .to_control_fields(&format!("{field_prefix}session."));
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}operation"), self.operation.clone());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(format!("{field_prefix}released"), self.released.to_string());
        fields.insert(
            format!("{field_prefix}commit_index"),
            self.commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}coordinator_session_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}coordinator_session_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CoordinatorSessionReceiptPage {
    pub receipts: Vec<CoordinatorSessionReceipt>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub operation: Option<String>,
    pub session_id: Option<String>,
    pub owner: Option<String>,
    pub released: Option<bool>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub matched_count: u64,
    pub skipped_operation_count: u64,
    pub skipped_session_count: u64,
    pub skipped_owner_count: u64,
    pub skipped_released_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
}

impl CoordinatorSessionReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-COORDINATOR-SESSION-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        coordinator_session_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_coordinator_session_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_coordinator_session_receipt_page_proof_checksum =
            expected_coordinator_session_receipt_page_proof_checksum.as_ref();
        if expected_coordinator_session_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_coordinator_session_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata coordinator session receipt page proof checksum mismatch: expected {} got {}",
                expected_coordinator_session_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = metadata_receipt_page_common_control_fields(
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.matched_count,
            self.payload_bytes,
            self.max_payload_bytes,
            self.budget_limited,
            self.truncated,
            &self.marker,
            &self.next_marker,
            field_prefix,
        );
        append_optional_string_field(&mut fields, field_prefix, "operation", &self.operation);
        append_optional_string_field(&mut fields, field_prefix, "session_id", &self.session_id);
        append_optional_string_field(&mut fields, field_prefix, "owner", &self.owner);
        if let Some(released) = self.released {
            fields.insert(format!("{field_prefix}released"), released.to_string());
        }
        for (name, value) in [
            ("skipped_operation_count", self.skipped_operation_count),
            ("skipped_session_count", self.skipped_session_count),
            ("skipped_owner_count", self.skipped_owner_count),
            ("skipped_released_count", self.skipped_released_count),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}coordinator_session_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}coordinator_session_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<CoordinatorSessionReceiptPage> {
    pub fn verify_expected_coordinator_session_receipt_page_proof_checksum(
        &self,
        expected_coordinator_session_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value.verify_expected_proof_checksum(
            expected_coordinator_session_receipt_page_proof_checksum,
        )
    }

    pub fn require_coordinator_session_receipt_page_proof_checksum(
        self,
        expected_coordinator_session_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_coordinator_session_receipt_page_proof_checksum(
            expected_coordinator_session_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

pub(crate) fn metadata_receipt_page_common_control_fields(
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    matched_count: u64,
    payload_bytes: u64,
    max_payload_bytes: u64,
    budget_limited: bool,
    truncated: bool,
    marker: &Option<String>,
    next_marker: &Option<String>,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    append_optional_string_field(&mut fields, field_prefix, "marker", marker);
    append_optional_string_field(&mut fields, field_prefix, "next_marker", next_marker);
    for (name, value) in [
        ("receipt_count", receipt_count),
        ("total_receipt_count", total_receipt_count),
        ("scanned_count", scanned_count),
        ("matched_count", matched_count),
        ("payload_bytes", payload_bytes),
        ("max_payload_bytes", max_payload_bytes),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields.insert(
        format!("{field_prefix}budget_limited"),
        budget_limited.to_string(),
    );
    fields.insert(format!("{field_prefix}truncated"), truncated.to_string());
    fields
}
