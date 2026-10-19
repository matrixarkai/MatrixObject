// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataTransactionSummaryPage {
    pub transactions: Vec<MetadataTransactionSummary>,
    pub prefix: String,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub owner: Option<String>,
    pub state: Option<String>,
    pub only_expired: bool,
    pub include_terminal: bool,
    pub transaction_count: u64,
    pub total_transaction_count: u64,
    pub scanned_count: u64,
    pub matched_count: u64,
    pub skipped_terminal_count: u64,
    pub skipped_owner_count: u64,
    pub skipped_state_count: u64,
    pub skipped_not_expired_count: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub truncated: bool,
}

impl MetadataTransactionSummaryPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-TRANSACTION-SUMMARY-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_transaction_summary_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_transaction_summary_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_transaction_summary_page_proof_checksum =
            expected_metadata_transaction_summary_page_proof_checksum.as_ref();
        if expected_metadata_transaction_summary_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_transaction_summary_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata transaction summary page proof checksum mismatch: expected {} got {}",
                expected_metadata_transaction_summary_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = metadata_transaction_page_common_control_fields(
            &self.prefix,
            &self.marker,
            &self.next_marker,
            &self.owner,
            &self.state,
            self.only_expired,
            self.include_terminal,
            self.transaction_count,
            self.total_transaction_count,
            self.scanned_count,
            self.matched_count,
            self.skipped_terminal_count,
            self.skipped_owner_count,
            self.skipped_state_count,
            self.skipped_not_expired_count,
            self.payload_bytes,
            self.max_payload_bytes,
            self.budget_limited,
            self.truncated,
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}metadata_transaction_summary_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_transaction_summary_page_proof_checksum"),
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

