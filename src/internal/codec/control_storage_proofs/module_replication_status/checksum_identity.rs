fn append_module_identity(mut material: &mut Vec<u8>, status: &ObjectStoreModuleStatus) {
    append_string(&mut material, &status.control_checksum_format);
    append_u64(&mut material, status.ready as u64);
    append_string(&mut material, &status.reason);
    append_string(&mut material, &status.root_status);
    append_string(&mut material, &status.metadata_status);
    append_string(&mut material, &status.page_status);
    append_string(&mut material, &status.chunk_status);
    append_string(&mut material, &status.cache_status);
    append_string(&mut material, &status.capacity_status);
    append_string(&mut material, &status.repair_status);
    append_string(&mut material, &status.compression_status);
    append_string(&mut material, &status.ec_status);
    append_u64(&mut material, status.objectstore_open as u64);
    append_u64(&mut material, status.replicaserver_open as u64);
    append_u64(&mut material, status.pageserver_ready as u64);
    append_u64(&mut material, status.bsclient_ready as u64);
    append_u64(&mut material, status.root_ready as u64);
    append_u64(&mut material, status.root_leader_ready as u64);
    append_u64(&mut material, status.root_read_ready as u64);
    append_u64(&mut material, status.root_metadata_read_ready as u64);
    append_u64(&mut material, status.root_metadata_write_ready as u64);
    append_u64(&mut material, status.root_pageserver_read_ready as u64);
    append_u64(&mut material, status.root_pageserver_write_ready as u64);
    append_u64(&mut material, status.root_replicaserver_read_ready as u64);
    append_u64(&mut material, status.root_replicaserver_write_ready as u64);
    append_u64(&mut material, status.root_readiness_gate_read_ready as u64);
    append_u64(&mut material, status.root_readiness_gate_write_ready as u64);
    append_string(&mut material, &status.configured_control_plane_mode);
    append_string(&mut material, &status.control_plane_mode);
    append_string(&mut material, &status.catalog_source);
    append_string(&mut material, &status.catalog_endpoint);
    append_string(&mut material, &status.catalog_route_checksum_format);
    append_string(&mut material, &status.catalog_route_checksum);
    append_u64(
        &mut material,
        status.metaserver_catalog_endpoint_configured as u64,
    );
    append_u64(
        &mut material,
        status.rootserver_catalog_endpoint_configured as u64,
    );
    append_u64(&mut material, status.rootserver_required as u64);
    append_u64(&mut material, status.metadata_ready as u64);
    append_u64(&mut material, status.metadata_read_ready as u64);
    append_u64(&mut material, status.metadata_write_ready as u64);
    append_u64(&mut material, status.metadata_quorum_ready as u64);
    append_string(&mut material, &status.metadata_coordinator_backend);
    append_string(&mut material, &status.metadata_coordinator_policy_format);
    append_string(
        &mut material,
        &status.metadata_coordinator_preferred_backend,
    );
    append_u64(
        &mut material,
        status.metadata_coordinator_preferred_backend_active as u64,
    );
    append_u64(
        &mut material,
        status.metadata_coordinator_external_compatibility_mode as u64,
    );
    append_u64(
        &mut material,
        status.metadata_coordinator_production_ready as u64,
    );
    append_string(
        &mut material,
        &status.metadata_coordinator_external_dependency,
    );
    append_string(&mut material, &status.metadata_coordinator_config_checksum);
    append_u64(&mut material, status.metadata_coordinator_config_revision);
    append_u64(
        &mut material,
        status.metadata_coordinator_config_endpoint_count,
    );
    append_u64(
        &mut material,
        status.metadata_coordinator_config_receipt_count,
    );
    append_u64(
        &mut material,
        status.metadata_external_coordinator_required as u64,
    );
    append_u64(
        &mut material,
        status.metadata_external_coordinator_endpoints_ready as u64,
    );
    append_u64(
        &mut material,
        status.metadata_external_coordinator_owner_sessions_ready as u64,
    );
    append_u64(
        &mut material,
        status.metadata_external_coordinator_ready as u64,
    );
    append_u64(&mut material, status.page_ready as u64);
    append_u64(&mut material, status.page_readable as u64);
    append_u64(&mut material, status.page_writable as u64);
    append_u64(&mut material, status.chunk_active_replicas);
    append_u64(&mut material, status.chunk_readable_replicas);
    append_u64(&mut material, status.chunk_writable_replicas);
    append_u64(&mut material, status.chunk_draining_replicas);
    append_u64(&mut material, status.chunk_quarantined_replicas);
}
