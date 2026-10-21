// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaWriteReceipt {
    pub page_id: PageId,
    pub offset: u64,
    pub length: u64,
    pub request_checksum: String,
    pub payload_checksum: String,
    pub idempotency_key: String,
    pub deduped: bool,
}

impl ReplicaWriteReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-WRITE-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_write_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_write_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_write_receipt_proof_checksum =
            expected_chunk_write_receipt_proof_checksum.as_ref();
        if expected_chunk_write_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_write_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk write receipt proof checksum mismatch: expected {} got {}",
                expected_chunk_write_receipt_proof_checksum, actual
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
            format!("{field_prefix}chunk_write_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_write_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaDiscardReceipt {
    pub page_id: PageId,
    pub offset: u64,
    pub length: u64,
    pub request_checksum: String,
    pub idempotency_key: String,
    pub deduped: bool,
}

impl ReplicaDiscardReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-DISCARD-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_discard_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_discard_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_discard_receipt_proof_checksum =
            expected_chunk_discard_receipt_proof_checksum.as_ref();
        if expected_chunk_discard_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_discard_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk discard receipt proof checksum mismatch: expected {} got {}",
                expected_chunk_discard_receipt_proof_checksum, actual
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
        fields.insert(format!("{field_prefix}offset"), self.offset.to_string());
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}chunk_discard_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_discard_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBatchWriteReceipt {
    pub page_id: PageId,
    pub request_checksum: String,
    pub payload_checksum: String,
    pub idempotency_key: String,
    pub stats: ReplicaWriteBatchStats,
    pub deduped: bool,
}

impl ReplicaBatchWriteReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-WRITE-BATCH-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_batch_write_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_write_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_write_batch_proof_checksum =
            expected_chunk_write_batch_proof_checksum.as_ref();
        if expected_chunk_write_batch_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_write_batch_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk write batch proof checksum mismatch: expected {} got {}",
                expected_chunk_write_batch_proof_checksum, actual
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
            format!("{field_prefix}chunk_write_batch_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_write_batch_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBatchDiscardReceipt {
    pub page_id: PageId,
    pub request_checksum: String,
    pub idempotency_key: String,
    pub stats: ReplicaDiscardBatchStats,
    pub deduped: bool,
}

impl ReplicaBatchDiscardReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-CHUNK-DISCARD-BATCH-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_batch_discard_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_discard_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_discard_batch_proof_checksum =
            expected_chunk_discard_batch_proof_checksum.as_ref();
        if expected_chunk_discard_batch_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_discard_batch_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk discard batch proof checksum mismatch: expected {} got {}",
                expected_chunk_discard_batch_proof_checksum, actual
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
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}chunk_discard_batch_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_discard_batch_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}
