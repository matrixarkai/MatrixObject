// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRecord {
    pub key: SharedText,
    /// A projection of the object at this key, or the text any other record
    /// carries. Sixteen bytes inline either way -- see `RecordValue`.
    pub value: RecordValue,
    pub version: u64,
    pub update_time_ms: u64,
    pub deleted: bool,
}

/// A metadata record ordered and identified BY ITS OWN KEY, so the collection holding it
/// does not keep a second copy of that key.
///
/// # EQUALITY IS THE KEY ALONE, AND THAT IS NOT A PREFERENCE
///
/// `BTreeSet` requires `Ord` and `Eq` to agree: if `a == b` then `a.cmp(&b)` must be `Equal`,
/// or the set's ordering invariant is unspecified behaviour rather than a bug you can see. The
/// ordering has to be the key, because **the key order is the listing contract** -- it is what
/// `list_objects` serves a prefix and a marker from. So equality is the key too.
///
/// This type therefore says NOTHING about whether two records hold the same value. Ask the
/// record, through [`KeyedRecord::record`]. Nothing outside the collection should compare these
/// to each other, which is why the record is private rather than a public field.
///
/// # AND FILING USES `replace`, NOT `insert`
///
/// `BTreeSet::insert` KEEPS the element already there and returns `false`; it does not
/// overwrite. Filing a new version of a record with `insert` would therefore leave the old
/// version in place and report success.
/// `filing_a_record_over_an_existing_key_replaces_it` is the guard for that.
#[derive(Clone, Debug)]
pub struct KeyedRecord(MetadataRecord);

impl KeyedRecord {
    pub fn new(record: MetadataRecord) -> Self {
        Self(record)
    }

    pub fn record(&self) -> &MetadataRecord {
        &self.0
    }

    pub fn into_record(self) -> MetadataRecord {
        self.0
    }

    pub fn key(&self) -> &SharedText {
        &self.0.key
    }
}

impl Ord for KeyedRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.key.cmp(&other.0.key)
    }
}

impl PartialOrd for KeyedRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for KeyedRecord {}

impl PartialEq for KeyedRecord {
    fn eq(&self, other: &Self) -> bool {
        self.0.key == other.0.key
    }
}

/// So the collection can be searched by `&str` without building a record.
///
/// `Borrow` requires this type's `Ord` to agree with `str`'s, which it does: `SharedText`
/// orders by its characters.
impl std::borrow::Borrow<str> for KeyedRecord {
    fn borrow(&self) -> &str {
        self.0.key.as_str()
    }
}

/// The same, for callers holding the key as a shared handle rather than as characters.
impl std::borrow::Borrow<SharedText> for KeyedRecord {
    fn borrow(&self) -> &SharedText {
        &self.0.key
    }
}

impl From<MetadataRecord> for KeyedRecord {
    fn from(record: MetadataRecord) -> Self {
        Self(record)
    }
}

impl MetadataRecord {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}key"), self.key.to_string());
        fields.insert(format!("{field_prefix}value"), self.value.as_text().into_owned());
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

