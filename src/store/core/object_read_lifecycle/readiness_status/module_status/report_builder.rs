// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
fn build_objectstore_module_status(
    readiness: ObjectStoreDistributedReadiness,
    health: ModuleHealthLabels,
    metadata_replication_proof: MetadataReplicationProofReport,
    configured_control_plane_mode: String,
    control_plane_mode: String,
    catalog_source: String,
    catalog_endpoint: String,
    metaserver_catalog_endpoint_configured: bool,
    rootserver_catalog_endpoint_configured: bool,
    rootserver_required: bool,
) -> ObjectStoreModuleStatus {
    let mut status = ObjectStoreModuleStatus {
        ready: readiness.ready,
        reason: readiness.reason.clone(),
        root_status: health.root,
        metadata_status: health.metadata,
        page_status: health.page,
        chunk_status: health.chunk,
        cache_status: health.cache,
        capacity_status: health.capacity,
        repair_status: health.repair,
        compression_status: health.compression,
        ec_status: health.erasure_coding,
        objectstore_open: true,
        replicaserver_open: readiness.storage_active_replicas != 0,
        pageserver_ready: readiness.page_ready,
        bsclient_ready: readiness.page_ready,
        root_ready: readiness.root_ready,
        root_leader_ready: readiness.root_leader_ready,
        configured_control_plane_mode,
        control_plane_mode: control_plane_mode.clone(),
        catalog_source,
        catalog_endpoint,
        catalog_route_checksum_format: MODULE_CATALOG_ROUTE_CHECKSUM_FORMAT.to_string(),
        catalog_route_checksum: String::new(),
        metaserver_catalog_endpoint_configured: metaserver_catalog_endpoint_configured,
        rootserver_catalog_endpoint_configured: rootserver_catalog_endpoint_configured,
        rootserver_required: rootserver_required,
        metadata_ready: readiness.metadata_ready,
        metadata_read_ready: readiness.metadata_read_ready,
        metadata_write_ready: readiness.metadata_write_ready,
        metadata_quorum_ready: readiness.metadata_quorum_ready,
        page_ready: readiness.page_ready,
        page_readable: readiness.page.readable,
        page_writable: readiness.page.writable,
        chunk_active_replicas: readiness.storage_active_replicas,
        chunk_readable_replicas: readiness.storage_readable_replicas,
        chunk_writable_replicas: readiness.storage_writable_replicas,
        chunk_draining_replicas: readiness.storage_draining_replicas,
        chunk_quarantined_replicas: readiness.storage_quarantined_replicas,
        control_checksum_format: "MOSRSMODULESTATUS32".to_string(),
        control_checksum: String::new(),
        ..Default::default()
    };
    populate_root_module_status(&mut status, &readiness);
    populate_metadata_module_status(&mut status, &readiness, metadata_replication_proof);
    populate_storage_module_status(&mut status, &readiness);
    populate_lifecycle_module_status(&mut status, &readiness);
    status.catalog_route_checksum = objectstore_module_catalog_route_checksum(&status);
    status.control_checksum = objectstore_module_status_checksum(&status);
    status
}
