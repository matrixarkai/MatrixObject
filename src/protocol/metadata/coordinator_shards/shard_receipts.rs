// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShard {
    pub shard_id: String,
    pub start_key: String,
    pub end_key: String,
    pub owner_node_id: String,
    pub epoch: u64,
    pub draining: bool,
}

impl MetadataShard {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}shard_id"), self.shard_id.clone());
        fields.insert(format!("{field_prefix}start_key"), self.start_key.clone());
        fields.insert(format!("{field_prefix}end_key"), self.end_key.clone());
        fields.insert(
            format!("{field_prefix}owner_node_id"),
            self.owner_node_id.clone(),
        );
        fields.insert(format!("{field_prefix}epoch"), self.epoch.to_string());
        fields.insert(format!("{field_prefix}draining"), self.draining.to_string());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardReceipt {
    pub idempotency_key: String,
    pub operation: String,
    pub request_checksum: String,
    pub shards: Vec<MetadataShard>,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub deduped: bool,
}

impl MetadataShardReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-SHARD-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_shard_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_shard_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_shard_receipt_proof_checksum =
            expected_metadata_shard_receipt_proof_checksum.as_ref();
        if expected_metadata_shard_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_shard_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard receipt proof checksum mismatch: expected {} got {}",
                expected_metadata_shard_receipt_proof_checksum, actual
            )));
        }
        Ok(())
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
        fields.insert(
            format!("{field_prefix}shard_count"),
            self.shards.len().to_string(),
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
            format!("{field_prefix}metadata_shard_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, shard) in self.shards.iter().enumerate() {
            fields.extend(shard.to_control_fields(&format!("{field_prefix}shard.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardReceiptPage {
    pub receipts: Vec<MetadataShardReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub operation: Option<String>,
    pub shard_id: Option<String>,
}

impl MetadataShardReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-SHARD-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_shard_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_shard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_shard_receipt_page_proof_checksum =
            expected_metadata_shard_receipt_page_proof_checksum.as_ref();
        if expected_metadata_shard_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_shard_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard receipt page proof checksum mismatch: expected {} got {}",
                expected_metadata_shard_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(&mut fields, field_prefix, "operation", &self.operation);
        append_optional_string_field(&mut fields, field_prefix, "shard_id", &self.shard_id);
        for (name, value) in [
            ("receipt_count", self.receipt_count),
            ("total_receipt_count", self.total_receipt_count),
            ("scanned_count", self.scanned_count),
            ("limit", self.limit),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_shard_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<MetadataShardReceiptPage> {
    pub fn verify_expected_metadata_shard_receipt_page_proof_checksum(
        &self,
        expected_metadata_shard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_metadata_shard_receipt_page_proof_checksum)
    }

    pub fn require_metadata_shard_receipt_page_proof_checksum(
        self,
        expected_metadata_shard_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_metadata_shard_receipt_page_proof_checksum(
            expected_metadata_shard_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}
