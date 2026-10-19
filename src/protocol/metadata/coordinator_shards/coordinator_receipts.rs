// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataCoordinatorConfigReceipt {
    pub idempotency_key: String,
    pub request_checksum: String,
    pub config: MetadataCoordinatorConfig,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub deduped: bool,
}

impl MetadataCoordinatorConfigReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-COORDINATOR-CONFIG-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_coordinator_config_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_coordinator_config_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_coordinator_config_receipt_proof_checksum =
            expected_metadata_coordinator_config_receipt_proof_checksum.as_ref();
        if expected_metadata_coordinator_config_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_coordinator_config_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata coordinator config receipt proof checksum mismatch: expected {} got {}",
                expected_metadata_coordinator_config_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.config.to_control_fields(field_prefix);
        fields.extend(
            self.config
                .to_control_fields(&format!("{field_prefix}config.")),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
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
            format!("{field_prefix}coordinator_config_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}coordinator_config_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataCoordinatorConfigReceiptPage {
    pub receipts: Vec<MetadataCoordinatorConfigReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub backend: Option<String>,
}

impl MetadataCoordinatorConfigReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-COORDINATOR-CONFIG-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_coordinator_config_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_coordinator_config_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_coordinator_config_receipt_page_proof_checksum =
            expected_metadata_coordinator_config_receipt_page_proof_checksum.as_ref();
        if expected_metadata_coordinator_config_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_coordinator_config_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata coordinator config receipt page proof checksum mismatch: expected {} got {}",
                expected_metadata_coordinator_config_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = metadata_receipt_page_common_control_fields(
            self.receipt_count,
            self.total_receipt_count,
            self.scanned_count,
            self.receipt_count,
            0,
            0,
            false,
            self.truncated,
            &None,
            &self.next_marker,
            field_prefix,
        );
        fields.insert(format!("{field_prefix}limit"), self.limit.to_string());
        append_optional_string_field(&mut fields, field_prefix, "backend", &self.backend);
        fields.insert(
            format!("{field_prefix}coordinator_config_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}coordinator_config_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<MetadataCoordinatorConfigReceiptPage> {
    pub fn verify_expected_coordinator_config_receipt_page_proof_checksum(
        &self,
        expected_metadata_coordinator_config_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value.verify_expected_proof_checksum(
            expected_metadata_coordinator_config_receipt_page_proof_checksum,
        )
    }

    pub fn require_coordinator_config_receipt_page_proof_checksum(
        self,
        expected_metadata_coordinator_config_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_coordinator_config_receipt_page_proof_checksum(
            expected_metadata_coordinator_config_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}
