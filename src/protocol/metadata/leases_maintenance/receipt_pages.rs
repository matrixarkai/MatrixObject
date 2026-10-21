// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataLeaseReceiptPage {
    pub receipts: Vec<MetadataLeaseReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub lease_namespace: Option<String>,
    pub owner: Option<String>,
    pub released: Option<bool>,
}

impl MetadataLeaseReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-META-LEASE-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        metadata_lease_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_metadata_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_lease_receipt_page_proof_checksum =
            expected_metadata_lease_receipt_page_proof_checksum.as_ref();
        if expected_metadata_lease_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_metadata_lease_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata lease receipt page proof checksum mismatch: expected {} got {}",
                expected_metadata_lease_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "lease_namespace",
            &self.lease_namespace,
        );
        append_optional_string_field(&mut fields, field_prefix, "owner", &self.owner);
        if let Some(released) = self.released {
            fields.insert(format!("{field_prefix}released"), released.to_string());
        }
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
            format!("{field_prefix}metadata_lease_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_lease_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<MetadataLeaseReceiptPage> {
    pub fn verify_expected_metadata_lease_receipt_page_proof_checksum(
        &self,
        expected_metadata_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_metadata_lease_receipt_page_proof_checksum)
    }

    pub fn require_metadata_lease_receipt_page_proof_checksum(
        self,
        expected_metadata_lease_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_metadata_lease_receipt_page_proof_checksum(
            expected_metadata_lease_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}

