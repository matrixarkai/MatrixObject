#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlaneCatalogSelection {
    pub configured_mode: String,
    pub effective_mode: String,
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub rootserver_required: bool,
    pub metaserver_endpoint_configured: bool,
    pub rootserver_endpoint_configured: bool,
    pub catalog_route_checksum_format: String,
    pub catalog_route_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlaneCatalogRoute {
    pub catalog_source: String,
    pub catalog_endpoint: String,
}

impl ControlPlaneCatalogRoute {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}catalog_source"),
            self.catalog_source.clone(),
        );
        fields.insert(
            format!("{field_prefix}catalog_endpoint"),
            self.catalog_endpoint.clone(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlaneCatalogEndpointSet {
    pub configured_mode: String,
    pub effective_mode: String,
    pub rootserver_required: bool,
    pub metaserver_endpoint_configured: bool,
    pub rootserver_endpoint_configured: bool,
    pub selected: Option<ControlPlaneCatalogRoute>,
    pub endpoints: Vec<ControlPlaneCatalogRoute>,
    pub catalog_route_checksum_format: String,
    pub catalog_route_checksum: String,
}

impl ControlPlaneCatalogEndpointSet {
    pub fn endpoint_count(&self) -> u64 {
        self.endpoints.len() as u64
    }

    pub fn compute_catalog_route_checksum(&self) -> String {
        let selected_source = self
            .selected
            .as_ref()
            .map(|selected| selected.catalog_source.as_str())
            .unwrap_or("none");
        let selected_endpoint = self
            .selected
            .as_ref()
            .map(|selected| selected.catalog_endpoint.as_str())
            .unwrap_or("");
        let endpoint_material_len = self
            .endpoints
            .iter()
            .map(|route| {
                encoded_string_len(&route.catalog_source)
                    + encoded_string_len(&route.catalog_endpoint)
            })
            .sum::<usize>();
        let mut material = Vec::with_capacity(
            encoded_string_len(&self.catalog_route_checksum_format)
                + encoded_string_len(&self.configured_mode)
                + encoded_string_len(&self.effective_mode)
                + encoded_string_len(selected_source)
                + encoded_string_len(selected_endpoint)
                + endpoint_material_len
                + std::mem::size_of::<u64>() * 4,
        );
        append_string(&mut material, &self.catalog_route_checksum_format);
        append_string(&mut material, &self.configured_mode);
        append_string(&mut material, &self.effective_mode);
        append_u64(&mut material, self.rootserver_required as u64);
        append_u64(&mut material, self.metaserver_endpoint_configured as u64);
        append_u64(&mut material, self.rootserver_endpoint_configured as u64);
        append_string(&mut material, selected_source);
        append_string(&mut material, selected_endpoint);
        append_u64(&mut material, self.endpoint_count());
        for route in &self.endpoints {
            append_string(&mut material, &route.catalog_source);
            append_string(&mut material, &route.catalog_endpoint);
        }
        etag(&material)
    }

    pub fn require_catalog_route_checksum(
        &self,
        expected_checksum: &str,
    ) -> Result<(), ObjectError> {
        require_control_plane_catalog_route_checksum(
            "catalog endpoint set",
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
        fields.insert(
            "catalog_endpoint_count".to_string(),
            self.endpoint_count().to_string(),
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
        if let Some(selected) = &self.selected {
            fields.extend(selected.to_control_fields(""));
            fields.extend(selected.to_control_fields("selected."));
        } else {
            fields.insert("catalog_source".to_string(), "none".to_string());
            fields.insert("catalog_endpoint".to_string(), String::new());
            fields.insert("selected.catalog_source".to_string(), "none".to_string());
            fields.insert("selected.catalog_endpoint".to_string(), String::new());
        }
        for (idx, route) in self.endpoints.iter().enumerate() {
            fields.extend(route.to_control_fields(&format!("route.{idx}.")));
            fields.insert(
                format!("catalog_endpoint.{idx}"),
                route.catalog_endpoint.clone(),
            );
            fields.insert(
                format!("catalog_source.{idx}"),
                route.catalog_source.clone(),
            );
        }
        fields
    }
}
