// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataTransactionPage {
    pub transactions: Vec<MetadataTransaction>,
    pub prefix: String,
    pub owner: Option<String>,
    pub state: Option<String>,
    pub only_expired: bool,
    pub include_terminal: bool,
    pub limit: u64,
    pub scanned_count: u64,
    pub matched_count: u64,
    pub skipped_terminal_count: u64,
    pub skipped_owner_count: u64,
    pub skipped_state_count: u64,
    pub skipped_not_expired_count: u64,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

impl MetadataTransactionPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-TRANSACTION-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_transaction_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_transaction_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_transaction_page_proof_checksum =
            expected_metadata_transaction_page_proof_checksum.as_ref();
        if expected_metadata_transaction_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_transaction_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction page proof checksum mismatch: expected {} got {}",
                expected_metadata_transaction_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = metadata_transaction_page_common_control_fields(
            &self.prefix,
            &None,
            &self.next_marker,
            &self.owner,
            &self.state,
            self.only_expired,
            self.include_terminal,
            self.transactions.len() as u64,
            self.transactions.len() as u64,
            self.scanned_count,
            self.matched_count,
            self.skipped_terminal_count,
            self.skipped_owner_count,
            self.skipped_state_count,
            self.skipped_not_expired_count,
            0,
            0,
            false,
            self.truncated,
            field_prefix,
        );
        fields.insert(format!("{field_prefix}limit"), self.limit.to_string());
        fields.insert(
            format!("{field_prefix}metadata_transaction_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_transaction_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, transaction) in self.transactions.iter().enumerate() {
            fields.extend(
                transaction.to_control_fields(&format!("{field_prefix}transaction.{index}.")),
            );
        }
        fields
    }
}

impl ProvenMetadataRead<MetadataTransactionSummaryPage> {
    pub fn verify_expected_transaction_summary_page_proof_checksum(
        &self,
        expected_metadata_transaction_summary_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value.verify_expected_proof_checksum(
            expected_metadata_transaction_summary_page_proof_checksum,
        )
    }

    pub fn require_transaction_summary_page_proof_checksum(
        self,
        expected_metadata_transaction_summary_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_transaction_summary_page_proof_checksum(
            expected_metadata_transaction_summary_page_proof_checksum,
        )?;
        Ok(self)
    }
}

impl ProvenMetadataRead<MetadataTransactionPage> {
    pub fn verify_expected_transaction_page_proof_checksum(
        &self,
        expected_metadata_transaction_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_metadata_transaction_page_proof_checksum)
    }

    pub fn require_transaction_page_proof_checksum(
        self,
        expected_metadata_transaction_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_transaction_page_proof_checksum(
            expected_metadata_transaction_page_proof_checksum,
        )?;
        Ok(self)
    }
}

pub(crate) fn metadata_transaction_page_common_control_fields(
    prefix: &str,
    marker: &Option<String>,
    next_marker: &Option<String>,
    owner: &Option<String>,
    state: &Option<String>,
    only_expired: bool,
    include_terminal: bool,
    transaction_count: u64,
    total_transaction_count: u64,
    scanned_count: u64,
    matched_count: u64,
    skipped_terminal_count: u64,
    skipped_owner_count: u64,
    skipped_state_count: u64,
    skipped_not_expired_count: u64,
    payload_bytes: u64,
    max_payload_bytes: u64,
    budget_limited: bool,
    truncated: bool,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(format!("{field_prefix}prefix"), prefix.to_string());
    append_optional_string_field(&mut fields, field_prefix, "marker", marker);
    append_optional_string_field(&mut fields, field_prefix, "next_marker", next_marker);
    append_optional_string_field(&mut fields, field_prefix, "owner", owner);
    append_optional_string_field(&mut fields, field_prefix, "state", state);
    for (name, value) in [
        ("transaction_count", transaction_count),
        ("total_transaction_count", total_transaction_count),
        ("scanned_count", scanned_count),
        ("matched_count", matched_count),
        ("skipped_terminal_count", skipped_terminal_count),
        ("skipped_owner_count", skipped_owner_count),
        ("skipped_state_count", skipped_state_count),
        ("skipped_not_expired_count", skipped_not_expired_count),
        ("payload_bytes", payload_bytes),
        ("max_payload_bytes", max_payload_bytes),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields.insert(
        format!("{field_prefix}only_expired"),
        only_expired.to_string(),
    );
    fields.insert(
        format!("{field_prefix}include_terminal"),
        include_terminal.to_string(),
    );
    fields.insert(
        format!("{field_prefix}budget_limited"),
        budget_limited.to_string(),
    );
    fields.insert(format!("{field_prefix}truncated"), truncated.to_string());
    fields
}

