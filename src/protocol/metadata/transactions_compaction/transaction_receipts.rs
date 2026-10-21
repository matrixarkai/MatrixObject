// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTransactionReceipt {
    pub idempotency_key: String,
    pub operation: String,
    pub request_checksum: String,
    pub transaction: MetadataTransaction,
    pub records: Vec<MetadataRecord>,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub deduped: bool,
}

impl MetadataTransactionReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-TRANSACTION-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_transaction_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_transaction_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_transaction_receipt_proof_checksum =
            expected_metadata_transaction_receipt_proof_checksum.as_ref();
        if expected_metadata_transaction_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_transaction_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction receipt proof checksum mismatch: expected {} got {}",
                expected_metadata_transaction_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .transaction
            .to_control_fields(&format!("{field_prefix}transaction."));
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}operation"), self.operation.clone());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}record_count"),
            self.records.len().to_string(),
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
            format!("{field_prefix}metadata_transaction_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_transaction_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, record) in self.records.iter().enumerate() {
            fields.extend(record.to_control_fields(&format!("{field_prefix}record.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataTransactionReceiptPage {
    pub receipts: Vec<MetadataTransactionReceipt>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub operation: Option<String>,
    pub transaction_id: Option<String>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub matched_count: u64,
    pub skipped_operation_count: u64,
    pub skipped_transaction_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
}

impl MetadataTransactionReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-TRANSACTION-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_transaction_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_transaction_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_transaction_receipt_page_proof_checksum =
            expected_metadata_transaction_receipt_page_proof_checksum.as_ref();
        if expected_metadata_transaction_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_transaction_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction receipt page proof checksum mismatch: expected {} got {}",
                expected_metadata_transaction_receipt_page_proof_checksum, actual
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
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "transaction_id",
            &self.transaction_id,
        );
        fields.insert(
            format!("{field_prefix}skipped_operation_count"),
            self.skipped_operation_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}skipped_transaction_count"),
            self.skipped_transaction_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_transaction_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_transaction_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

