use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRecord {
    pub key: SharedText,
    pub value: String,
    pub version: u64,
    pub update_time_ms: u64,
    pub deleted: bool,
}

impl MetadataRecord {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}key"), self.key.to_string());
        fields.insert(format!("{field_prefix}value"), self.value.clone().into());
        fields.insert(format!("{field_prefix}version"), self.version.to_string().into());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string().into(),
        );
        fields.insert(format!("{field_prefix}deleted"), self.deleted.to_string().into());
        fields
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataLease {
    pub namespace: String,
    pub owner: String,
    pub epoch: u64,
    pub expire_time_ms: u64,
    pub ttl_ms: u64,
}

impl MetadataLease {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}namespace"), self.namespace.clone());
        fields.insert(format!("{field_prefix}owner"), self.owner.clone());
        fields.insert(format!("{field_prefix}epoch"), self.epoch.to_string());
        fields.insert(
            format!("{field_prefix}expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}ttl_ms"), self.ttl_ms.to_string());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataLeaseReceipt {
    pub idempotency_key: String,
    pub operation: String,
    pub request_checksum: String,
    pub lease: MetadataLease,
    pub released: bool,
    pub commit_index: u64,
    pub update_time_ms: u64,
    pub deduped: bool,
}

impl MetadataLeaseReceipt {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-LEASE-RECEIPT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_lease_receipt_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_lease_receipt_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_lease_receipt_proof_checksum =
            expected_metadata_lease_receipt_proof_checksum.as_ref();
        if expected_metadata_lease_receipt_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_lease_receipt_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata lease receipt proof checksum mismatch: expected {} got {}",
                expected_metadata_lease_receipt_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .lease
            .to_control_fields(&format!("{field_prefix}lease."));
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}operation"), self.operation.clone());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(format!("{field_prefix}released"), self.released.to_string());
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
            format!("{field_prefix}metadata_lease_receipt_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_lease_receipt_proof_checksum"),
            self.proof_checksum(),
        );
        fields
    }
}

