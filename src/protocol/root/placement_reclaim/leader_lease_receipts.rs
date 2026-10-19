// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootLeaderLeaseReceipt {
    pub idempotency_key: String,
    pub operation: String,
    pub request_checksum: String,
    pub lease: RootLeaderLease,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub receipt_proof_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootLeaderLeaseReceiptPage {
    pub receipts: Vec<RootLeaderLeaseReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub skipped_operation_count: u64,
    pub skipped_leader_count: u64,
    pub operation: Option<String>,
    pub leader_node_id: Option<String>,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub limit: u64,
}

impl RootLeaderLeaseReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-ROOT-LEADER-LEASE-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        root_leader_lease_receipt_proof_checksum(self)
    }

    pub fn with_proof_checksum(mut self) -> Self {
        self.receipt_proof_checksum = self.proof_checksum();
        self
    }

    pub fn verify_proof_checksum(&self) -> Result<(), ObjectError> {
        let actual = self.proof_checksum();
        if self.receipt_proof_checksum.is_empty() || self.receipt_proof_checksum == actual {
            return Ok(());
        }
        Err(ObjectError::Corruption(format!(
            "root leader lease receipt proof checksum mismatch: expected {actual}, got {}",
            self.receipt_proof_checksum
        )))
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}operation"), self.operation.clone());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.extend(self.lease.to_control_fields(field_prefix));
        fields.insert(
            format!("{field_prefix}commit_index"),
            self.commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}root_leader_lease_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}root_leader_lease_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

impl RootLeaderLeaseReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-ROOT-LEADER-LEASE-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        root_leader_lease_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_root_leader_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_root_leader_lease_receipt_page_proof_checksum =
            expected_root_leader_lease_receipt_page_proof_checksum.as_ref();
        if expected_root_leader_lease_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_root_leader_lease_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "root leader lease receipt page proof checksum mismatch: expected {} got {}",
                expected_root_leader_lease_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(&mut fields, field_prefix, "operation", &self.operation);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "leader_node_id",
            &self.leader_node_id,
        );
        for (name, value) in [
            ("receipt_count", self.receipt_count),
            ("total_receipt_count", self.total_receipt_count),
            ("scanned_count", self.scanned_count),
            ("skipped_operation_count", self.skipped_operation_count),
            ("skipped_leader_count", self.skipped_leader_count),
            ("limit", self.limit),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}root_leader_lease_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}root_leader_lease_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenRootRead<RootLeaderLeaseReceiptPage> {
    pub fn verify_expected_root_leader_lease_receipt_page_proof_checksum(
        &self,
        expected_root_leader_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_root_leader_lease_receipt_page_proof_checksum)
    }

    pub fn require_root_leader_lease_receipt_page_proof_checksum(
        self,
        expected_root_leader_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_root_leader_lease_receipt_page_proof_checksum(
            expected_root_leader_lease_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

