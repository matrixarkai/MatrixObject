// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn validate_catalog_route(
    service_kind: &str,
    catalog_source: &str,
    catalog_endpoint: &str,
    catalog_endpoint_count: u64,
) -> Result<(String, String, u64), ObjectError> {
    if catalog_source.is_empty() {
        return Err(ObjectError::InvalidArgument(format!(
            "{service_kind} catalog source must be set"
        )));
    }
    if catalog_endpoint.is_empty() {
        return Err(ObjectError::InvalidArgument(format!(
            "{service_kind} catalog endpoint must be set"
        )));
    }
    if catalog_endpoint_count == 0 {
        return Err(ObjectError::InvalidArgument(format!(
            "{service_kind} catalog endpoint count must be greater than zero"
        )));
    }
    Ok((
        catalog_source.to_string(),
        catalog_endpoint.to_string(),
        catalog_endpoint_count,
    ))
}

impl CatalogChunkRegistration {
    /// The route binding checksum this build can verify but does not yet emit.
    pub fn catalog_route_binding_checksum(&self) -> String {
        catalog_route_binding_checksum(
            CATALOG_CHUNK_ROUTE_DISCRIMINATOR,
            &self.node,
            &self.catalog_source,
            &self.catalog_endpoint,
            self.catalog_endpoint_count,
        )
    }

    pub fn catalog_route_checksum(&self) -> String {
        catalog_route_checksum(
            "chunkserver",
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
            "chunkserver",
            &[
                &self.catalog_route_checksum(),
                &self.catalog_route_binding_checksum(),
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
            "chunkserver",
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
            "chunkserver",
            &self.node,
            &self.catalog_source,
            &self.catalog_endpoint,
            self.catalog_endpoint_count,
        )
    }
}

impl CatalogControlFields for CatalogChunkRegistration {
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

