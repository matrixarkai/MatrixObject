// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaStateInstallReceipt {
    pub page_id: PageId,
    pub request_checksum: String,
    pub payload_checksum: String,
    pub state_checksum: String,
    pub idempotency_key: String,
    pub logical_length: u64,
    pub write_sequence: u64,
    pub discard_range_count: u64,
    pub deduped: bool,
}

impl ReplicaStateInstallReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MOSRS-CHUNK-STATE-INSTALL-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_state_install_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_state_install_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_state_install_receipt_proof_checksum =
            expected_chunk_state_install_receipt_proof_checksum.as_ref();
        if expected_chunk_state_install_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_state_install_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk state install receipt proof checksum mismatch: expected {} got {}",
                expected_chunk_state_install_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .page_id
            .to_control_fields(&format!("{field_prefix}chunk."));
        for (name, value) in [
            ("volume_id", self.page_id.chunk_id.volume_id),
            ("segment_id", self.page_id.chunk_id.id),
            ("chunk_index", self.page_id.page_index),
            ("logical_length", self.logical_length),
            ("write_sequence", self.write_sequence),
            ("discard_range_count", self.discard_range_count),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("request_checksum", &self.request_checksum),
            ("payload_checksum", &self.payload_checksum),
            ("state_checksum", &self.state_checksum),
            ("idempotency_key", &self.idempotency_key),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}chunk_state_install_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_state_install_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReclaimReceipt {
    pub request_checksum: String,
    pub idempotency_key: String,
    pub max_chunks: u64,
    pub max_rewrite_bytes: u64,
    pub min_discarded_bytes: u64,
    pub min_discard_ratio_ppm: u64,
    pub max_discard_ranges: u64,
    pub max_trim_bytes: u64,
    pub report: ReplicaPartialReclaimReport,
    pub deduped: bool,
}

impl ReplicaReclaimReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MOSRS-CHUNK-RECLAIM-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_reclaim_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_reclaim_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_reclaim_receipt_proof_checksum =
            expected_chunk_reclaim_receipt_proof_checksum.as_ref();
        if expected_chunk_reclaim_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_reclaim_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk reclaim receipt proof checksum mismatch: expected {} got {}",
                expected_chunk_reclaim_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("max_chunks", self.max_chunks),
            ("max_rewrite_bytes", self.max_rewrite_bytes),
            ("min_discarded_bytes", self.min_discarded_bytes),
            ("min_discard_ratio_ppm", self.min_discard_ratio_ppm),
            ("max_discard_ranges", self.max_discard_ranges),
            ("max_trim_bytes", self.max_trim_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
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
            format!("{field_prefix}chunk_reclaim_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_reclaim_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRepairReceipt {
    pub page_id: PageId,
    pub request_checksum: String,
    pub idempotency_key: String,
    pub report: ReplicaRepairReport,
    pub deduped: bool,
}

impl ReplicaRepairReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-REPAIR-RECEIPT1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_repair_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_repair_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_repair_receipt_proof_checksum =
            expected_chunk_repair_receipt_proof_checksum.as_ref();
        if expected_chunk_repair_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_repair_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk repair receipt proof checksum mismatch: expected {} got {}",
                expected_chunk_repair_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .page_id
            .to_control_fields(&format!("{field_prefix}chunk."));
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.page_id.chunk_id.volume_id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_id"),
            self.page_id.chunk_id.id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_index"),
            self.page_id.page_index.to_string(),
        );
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
            format!("{field_prefix}chunk_repair_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_repair_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}
