// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl CatalogPageRegistration {
    pub fn catalog_route_checksum(&self) -> String {
        catalog_route_checksum(
            "blockserver",
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
            "blockserver",
            &self.catalog_route_checksum(),
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
            "blockserver",
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
        let mut fields = catalog_registration_fields(
            "blockserver",
            &self.node,
            &self.catalog_source,
            &self.catalog_endpoint,
            self.catalog_endpoint_count,
        );
        fields.insert(
            "allocator_shard_index".to_string(),
            self.allocator_shard_index.to_string(),
        );
        fields.insert(
            "allocator_shard_count".to_string(),
            self.allocator_shard_count.to_string(),
        );
        fields
    }
}

impl CatalogControlFields for CatalogPageRegistration {
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

