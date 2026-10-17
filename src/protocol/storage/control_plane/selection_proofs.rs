impl ControlPlaneCatalogSelection {
    pub fn compute_catalog_route_checksum(&self) -> String {
        let mut material = Vec::with_capacity(
            encoded_string_len(&self.catalog_route_checksum_format)
                + encoded_string_len(&self.configured_mode)
                + encoded_string_len(&self.effective_mode)
                + encoded_string_len(&self.catalog_source)
                + encoded_string_len(&self.catalog_endpoint)
                + std::mem::size_of::<u64>() * 3,
        );
        append_string(&mut material, &self.catalog_route_checksum_format);
        append_string(&mut material, &self.configured_mode);
        append_string(&mut material, &self.effective_mode);
        append_string(&mut material, &self.catalog_source);
        append_string(&mut material, &self.catalog_endpoint);
        append_u64(&mut material, self.rootserver_required as u64);
        append_u64(&mut material, self.metaserver_endpoint_configured as u64);
        append_u64(&mut material, self.rootserver_endpoint_configured as u64);
        etag(&material)
    }

    pub fn require_catalog_route_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        require_control_plane_catalog_route_checksum(
            "catalog selection",
            &self.compute_catalog_route_checksum(),
            expected_checksum,
        )
    }

    pub fn to_control_fields(&self) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            "configured_control_plane_mode".to_string(),
            self.configured_mode.clone(),
        );
        fields.insert(
            "effective_control_plane_mode".to_string(),
            self.effective_mode.clone(),
        );
        fields.insert("catalog_source".to_string(), self.catalog_source.clone());
        fields.insert(
            "catalog_endpoint".to_string(),
            self.catalog_endpoint.clone(),
        );
        fields.insert(
            "catalog_route_checksum_format".to_string(),
            self.catalog_route_checksum_format.clone(),
        );
        fields.insert(
            "catalog_route_checksum".to_string(),
            self.catalog_route_checksum.clone(),
        );
        fields.insert(
            "rootserver_required".to_string(),
            self.rootserver_required.to_string(),
        );
        fields.insert(
            "metaserver_endpoint_configured".to_string(),
            self.metaserver_endpoint_configured.to_string(),
        );
        fields.insert(
            "metaserver_catalog_endpoint_configured".to_string(),
            self.metaserver_endpoint_configured.to_string(),
        );
        fields.insert(
            "rootserver_endpoint_configured".to_string(),
            self.rootserver_endpoint_configured.to_string(),
        );
        fields.insert(
            "rootserver_catalog_endpoint_configured".to_string(),
            self.rootserver_endpoint_configured.to_string(),
        );
        fields
    }
}

pub(crate) fn require_control_plane_catalog_route_checksum(
    label: &str,
    actual_checksum: &str,
    expected_checksum: &str,
) -> Result<(), ObjectError> {
    if expected_checksum.is_empty() {
        return Err(ObjectError::InvalidArgument(format!(
            "{label} catalog route checksum must be set"
        )));
    }
    if actual_checksum != expected_checksum {
        return Err(ObjectError::PreconditionFailed(format!(
            "{label} catalog route checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
        )));
    }
    Ok(())
}
