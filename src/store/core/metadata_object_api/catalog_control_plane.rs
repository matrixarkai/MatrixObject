// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn select_catalog_endpoint_set_from_env(
        &self,
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        let mode = ControlPlaneMode::parse_from_env()?;
        let metaserver_endpoints =
            first_non_empty_env(&MATRIX_OBJECTSTORE_METASERVER_CATALOG_ENV_NAMES)?.or_else(|| {
                self.configured_metaserver_catalog_endpoints()
                    .map(str::to_string)
            });
        let rootserver_endpoint =
            first_non_empty_env(&MATRIX_OBJECTSTORE_GLOBAL_CATALOG_ENV_NAMES)?.or_else(|| {
                self.configured_placement_fallback_endpoint()
                    .map(str::to_string)
            });
        let endpoint_set = mode.select_catalog_endpoint_set(
            metaserver_endpoints.as_deref(),
            rootserver_endpoint.as_deref(),
        )?;
        require_control_plane_catalog_endpoint_set_env_checksums(endpoint_set)
    }

    /// Always false: the control plane is the metaserver, and no mode reads a root catalog.
    ///
    /// Kept as a reported value rather than deleted, because it is observable output.
    /// `rootserver_endpoint_configured` beside it still reports whether one was configured,
    /// which is how an operator would see a setting being ignored -- though nothing can get
    /// one past the refusals at construction, in the environment, and on snapshot load.
    pub fn placement_required(&self) -> bool {
        match self.options.control_plane_mode {
            ControlPlaneMode::Auto | ControlPlaneMode::MetaServer => false,
        }
    }

    /// Matched rather than returned as a constant, so adding a mode is a compile error
    /// here instead of a mode that silently reports itself as the metaserver.
    pub fn effective_control_plane_mode(&self) -> &'static str {
        match self.options.control_plane_mode {
            ControlPlaneMode::Auto | ControlPlaneMode::MetaServer => "metaserver",
        }
    }

    pub fn effective_control_plane_mode_for_routes(
        &self,
        metaserver_endpoints: Option<&str>,
        rootserver_endpoint: Option<&str>,
    ) -> Result<String, ObjectError> {
        Ok(self
            .select_catalog_endpoint_set(metaserver_endpoints, rootserver_endpoint)?
            .effective_mode)
    }

    pub fn list_placement_nodes(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        metadata_eligible_filter: Option<bool>,
        data_eligible_filter: Option<bool>,
        gateway_eligible_filter: Option<bool>,
    ) -> Result<PlacementNodePage, ObjectError> {
        self.meta_service.list_nodes_page(
            marker,
            limit,
            service_kind_filter,
            draining_filter,
            online_filter,
            metadata_eligible_filter,
            data_eligible_filter,
            gateway_eligible_filter,
        )
    }

    pub fn list_placement_node_usage(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        capacity_pressure_filter: Option<bool>,
        reclaim_pressure_filter: Option<bool>,
        placement_eligible_filter: Option<bool>,
        min_free_bytes: u64,
        min_reclaimable_bytes: u64,
    ) -> Result<PlacementNodeUsagePage, ObjectError> {
        self.meta_service.list_node_usage(
            marker,
            limit,
            service_kind_filter,
            draining_filter,
            online_filter,
            capacity_pressure_filter,
            reclaim_pressure_filter,
            placement_eligible_filter,
            min_free_bytes,
            min_reclaimable_bytes,
        )
    }

    pub fn plan_placement_reclaim_nodes(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        only_online: bool,
        include_draining: bool,
        min_reclaimable_bytes: u64,
        min_overcommitted_bytes: u64,
        min_free_bytes: u64,
        min_page_physical_discard_bytes: u64,
        min_page_physical_discard_ranges: u64,
    ) -> Result<PlacementReclaimNodePlanPage, ObjectError> {
        self.meta_service.plan_reclaim_nodes(
            marker,
            limit,
            service_kind_filter,
            only_online,
            include_draining,
            min_reclaimable_bytes,
            min_overcommitted_bytes,
            min_free_bytes,
            min_page_physical_discard_bytes,
            min_page_physical_discard_ranges,
        )
    }

    pub fn list_object_placements(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        node_id_filter: Option<&str>,
    ) -> Result<PlacementPage, ObjectError> {
        self.meta_service
            .list_object_placements(marker, limit, node_id_filter)
    }

    pub fn audit_placement_health(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        degraded_only: bool,
        max_payload_bytes: u64,
    ) -> Result<PlacementHealthPage, ObjectError> {
        self.meta_service.audit_placement_health(
            marker,
            limit,
            expected_replica_count,
            min_free_bytes,
            degraded_only,
            max_payload_bytes,
        )
    }

    pub fn object_placement(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.meta_service
            .object_placement(&ObjectId::new(bucket, key))
    }

    pub fn refresh_object_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.apply_command(ObjectCommand::RefreshObjectRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            replica_count,
            update_time_ms: now_millis(),
        })
    }

    pub fn refresh_all_object_placements(
        &mut self,
        max_objects: usize,
        replica_count: usize,
    ) -> Result<u64, ObjectError> {
        Ok(self
            .refresh_object_placements_page(None, max_objects, replica_count)?
            .refreshed_objects)
    }

    pub fn refresh_object_placements_page(
        &mut self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        replica_count: usize,
    ) -> Result<PlacementRefreshReport, ObjectError> {
        self.apply_refresh_object_placements_page(
            marker.cloned(),
            max_objects,
            replica_count,
            now_millis(),
        )
    }

}
