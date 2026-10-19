// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkWriteReceipt {
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub length: u64,
    pub request_checksum: String,
    pub payload_checksum: String,
    pub idempotency_key: String,
    pub deduped: bool,
}

impl ChunkWriteReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-WRITE-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_write_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_write_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_page_write_receipt_proof_checksum =
            expected_page_write_receipt_proof_checksum.as_ref();
        if expected_page_write_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_write_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "page write receipt proof checksum mismatch: expected {} got {}",
                expected_page_write_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}segment."));
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.chunk_id.volume_id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_id"),
            self.chunk_id.id.to_string(),
        );
        fields.insert(format!("{field_prefix}offset"), self.offset.to_string());
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}payload_checksum"),
            self.payload_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}block_write_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_write_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkBatchWriteReceipt {
    pub request_checksum: String,
    pub payload_checksum: String,
    pub idempotency_key: String,
    pub stats: ChunkBatchStats,
    pub deduped: bool,
}

impl ChunkBatchWriteReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-BLOCK-WRITE-BATCH-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        page_batch_write_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_page_write_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_page_write_batch_proof_checksum =
            expected_page_write_batch_proof_checksum.as_ref();
        if expected_page_write_batch_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_page_write_batch_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "page write batch proof checksum mismatch: expected {} got {}",
                expected_page_write_batch_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}payload_checksum"),
            self.payload_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}block_write_batch_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}block_write_batch_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

