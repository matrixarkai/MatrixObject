#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageAuditRepairReceipt {
    pub after_chunk_id: Option<PageId>,
    pub max_chunks: u64,
    pub max_payload_bytes: u64,
    pub request_checksum: String,
    pub idempotency_key: String,
    pub report: PageReplicaAuditRepairReport,
    pub deduped: bool,
}

impl PageAuditRepairReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-AUDIT-REPAIR-RECEIPT1"
    }

    pub fn proof_checksum(&self) -> String {
        page_audit_repair_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_audit_repair_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_page_audit_repair_receipt_proof_checksum =
            expected_page_audit_repair_receipt_proof_checksum.as_ref();
        if expected_page_audit_repair_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_audit_repair_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block audit repair receipt proof checksum mismatch: expected {} got {}",
                expected_page_audit_repair_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("max_chunks", self.max_chunks),
            ("max_payload_bytes", self.max_payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}after_chunk_present"),
            self.after_chunk_id.is_some().to_string(),
        );
        if let Some(after_chunk_id) = &self.after_chunk_id {
            fields.extend(after_chunk_id.to_control_fields(&format!("{field_prefix}after_chunk.")));
        }
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.extend(
            self.report
                .to_control_fields(&format!("{field_prefix}report.")),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}block_audit_repair_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_audit_repair_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}
