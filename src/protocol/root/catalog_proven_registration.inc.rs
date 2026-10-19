// SPDX-License-Identifier: Apache-2.0
pub trait RootCatalogControlFields {
    fn to_root_catalog_control_fields(&self) -> BTreeMap<String, String>;
    fn require_root_catalog_route_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError>;
    fn root_catalog_liveness_receipt(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
    ) -> RootCatalogLivenessReceipt;
    fn require_root_catalog_liveness_receipt_checksum(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.root_catalog_liveness_receipt(method, term, commit_index, read_index, node_count)
            .require_checksum(expected_checksum)
    }
}

impl<T> ProvenRootCatalogRegistration<T> {
    pub fn last_receipt(&self) -> Option<&ObjectMetadata> {
        self.raft_receipts.last()
    }

    pub fn raft_receipt_sequence(&self) -> Vec<String> {
        self.raft_receipts
            .iter()
            .map(|receipt| receipt.content_type.to_string())
            .collect()
    }

    pub fn raft_receipt_sequence_checksum(&self) -> String {
        let mut material = Vec::new();
        append_string(&mut material, "MOS-RUST-ROOT-CATALOG-RECEIPT-SEQUENCE1");
        append_u64(&mut material, self.raft_receipts.len() as u64);
        for receipt in &self.raft_receipts {
            append_object_id(&mut material, &receipt.object_id);
            append_u64(&mut material, receipt.version);
            append_u64(&mut material, receipt.length);
            append_string(&mut material, &receipt.etag());
            append_string(&mut material, &receipt.content_type);
            append_u64(&mut material, receipt.update_time_ms);
        }
        etag(&material)
    }

    pub fn require_raft_receipt_sequence(
        &self,
        expected_content_types: &[&str],
    ) -> Result<(), ObjectError> {
        if self.raft_receipts.len() != expected_content_types.len() {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog raft receipt count mismatch: expected {}, got {}",
                expected_content_types.len(),
                self.raft_receipts.len()
            )));
        }
        for (index, expected) in expected_content_types.iter().enumerate() {
            let actual = &self.raft_receipts[index].content_type;
            if **actual != **expected {
                return Err(ObjectError::PreconditionFailed(format!(
                    "root catalog raft receipt {index} content type mismatch: expected {expected}, got {actual}"
                )));
            }
        }
        Ok(())
    }

    pub fn require_raft_receipt_sequence_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "root catalog raft receipt sequence checksum must be set".to_string(),
            ));
        }
        let actual = self.raft_receipt_sequence_checksum();
        if actual != expected_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog raft receipt sequence checksum mismatch: expected {expected_checksum}, got {actual}"
            )));
        }
        Ok(())
    }
}

impl<T: RootCatalogControlFields> ProvenRootCatalogRegistration<T> {
    pub fn require_catalog_route_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.registration
            .require_root_catalog_route_checksum(expected_checksum)
    }

    pub fn into_route_fenced(self, expected_checksum: &str) -> Result<Self, ObjectError> {
        self.require_catalog_route_checksum(expected_checksum)?;
        Ok(self)
    }

    pub fn root_catalog_liveness_receipt(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
    ) -> RootCatalogLivenessReceipt {
        self.registration.root_catalog_liveness_receipt(
            method,
            term,
            commit_index,
            read_index,
            node_count,
        )
    }

    pub fn require_root_catalog_liveness_receipt_checksum(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.registration
            .require_root_catalog_liveness_receipt_checksum(
                method,
                term,
                commit_index,
                read_index,
                node_count,
                expected_checksum,
            )
    }

    pub fn into_liveness_fenced(
        self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
        expected_checksum: &str,
    ) -> Result<Self, ObjectError> {
        self.require_root_catalog_liveness_receipt_checksum(
            method,
            term,
            commit_index,
            read_index,
            node_count,
            expected_checksum,
        )?;
        Ok(self)
    }

    pub fn to_liveness_control_fields(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
    ) -> BTreeMap<String, String> {
        let mut fields = self.to_control_fields();
        fields.extend(
            self.root_catalog_liveness_receipt(method, term, commit_index, read_index, node_count)
                .to_native_liveness_receipt_fields(),
        );
        fields
    }

    pub fn verify_liveness_control_fields(
        &self,
        fields: &BTreeMap<String, String>,
        method: &str,
        expected_node_id: &str,
        expected_service_kind: &str,
    ) -> Result<RootCatalogLivenessReceipt, ObjectError> {
        RootCatalogLivenessReceipt::require_native_liveness_receipt_fields(
            fields,
            method,
            expected_node_id,
            expected_service_kind,
        )
    }

    pub fn to_control_fields(&self) -> BTreeMap<String, String> {
        let mut fields = self.registration.to_root_catalog_control_fields();
        fields.insert(
            "raft_receipt_count".to_string(),
            self.raft_receipts.len().to_string(),
        );
        fields.insert(
            "raft_receipt_sequence_checksum".to_string(),
            self.raft_receipt_sequence_checksum(),
        );
        for (index, receipt) in self.raft_receipts.iter().enumerate() {
            let prefix = format!("raft_receipt.{index}.");
            fields.insert(format!("{prefix}bucket"), receipt.object_id.bucket.to_string());
            fields.insert(format!("{prefix}key"), receipt.object_id.key.to_string());
            fields.insert(format!("{prefix}version"), receipt.version.to_string());
            fields.insert(format!("{prefix}length"), receipt.length.to_string());
            fields.insert(format!("{prefix}etag"), receipt.etag().to_string());
            fields.insert(
                format!("{prefix}content_type"),
                receipt.content_type.to_string(),
            );
            fields.insert(
                format!("{prefix}update_time_ms"),
                receipt.update_time_ms.to_string(),
            );
        }
        fields
    }
}

