// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl CatalogObjectStoreRegistration {
    /// The route checksum this build emits: the binding form, whose discriminating
    /// term is a dedicated constant rather than the node kind.
    pub fn catalog_route_checksum(&self) -> String {
        catalog_route_binding_checksum(
            CATALOG_OBJECTSTORE_ROUTE_DISCRIMINATOR,
            &self.node,
            &self.catalog_source,
            &self.catalog_endpoint,
            self.catalog_endpoint_count,
        )
    }

    /// The retired kind-derived form. Kept ONLY so a checksum written by a build that
    /// shipped before the binding form still verifies; nothing emits it.
    pub fn legacy_kind_route_checksum(&self) -> String {
        catalog_route_checksum(
            "objectstore",
            &self.node,
            &self.catalog_source,
            &self.catalog_endpoint,
            self.catalog_endpoint_count,
        )
    }

    pub fn require_catalog_route_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        require_route_checksum_for_service(
            "objectstore",
            &[
                &self.catalog_route_checksum(),
                &self.legacy_kind_route_checksum(),
            ],
            expected_checksum,
        )
    }

    pub fn liveness_receipt(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
    ) -> CatalogLivenessReceipt {
        catalog_liveness_receipt(
            method,
            &self.node,
            term,
            commit_index,
            read_index,
            node_count,
        )
    }

    pub fn require_liveness_receipt_checksum(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.liveness_receipt(method, term, commit_index, read_index, node_count)
            .require_checksum(expected_checksum)
    }

    pub fn with_catalog_route(
        mut self,
        catalog_source: &str,
        catalog_endpoint: &str,
        catalog_endpoint_count: u64,
    ) -> Result<Self, ObjectError> {
        let (source, endpoint, count) = validate_catalog_route(
            "objectstore",
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        )?;
        self.catalog_source = source;
        self.catalog_endpoint = endpoint;
        self.catalog_endpoint_count = count;
        Ok(self)
    }

    pub fn to_control_fields(&self) -> BTreeMap<String, String> {
        catalog_registration_fields(
            "objectstore",
            CATALOG_OBJECTSTORE_ROUTE_DISCRIMINATOR,
            &self.node,
            &self.catalog_source,
            &self.catalog_endpoint,
            self.catalog_endpoint_count,
        )
    }
}

impl CatalogControlFields for CatalogObjectStoreRegistration {
    fn to_catalog_control_fields(&self) -> BTreeMap<String, String> {
        self.to_control_fields()
    }

    fn require_route_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.require_catalog_route_checksum(expected_checksum)
    }

    fn catalog_liveness_receipt(
        &self,
        method: &str,
        term: u64,
        commit_index: u64,
        read_index: u64,
        node_count: u64,
    ) -> CatalogLivenessReceipt {
        self.liveness_receipt(method, term, commit_index, read_index, node_count)
    }
}

