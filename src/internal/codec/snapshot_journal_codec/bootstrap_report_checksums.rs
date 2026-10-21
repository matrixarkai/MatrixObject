// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn meta_raft_bootstrap_report_checksum(report: &MetadataRaftBootstrapReport) -> String {
    let mut material = Vec::new();
    append_string(&mut material, META_RAFT_BOOTSTRAP_REPORT_FORMAT);
    append_string(&mut material, &report.mutation_id);
    append_u64(&mut material, report.target_voter_node_ids.len() as u64);
    for node_id in &report.target_voter_node_ids {
        append_string(&mut material, node_id);
    }
    append_string(
        &mut material,
        &raft_voter_checksum(&report.target_voter_node_ids),
    );
    append_u64(&mut material, report.target_healthy_peer_count);
    append_metadata_bootstrap_readiness(&mut material, &report.before_readiness);
    append_metadata_bootstrap_readiness(&mut material, &report.after_readiness);
    append_u64(&mut material, report.membership_receipt.is_some() as u64);
    if let Some(receipt) = &report.membership_receipt {
        append_raft_membership_receipt(&mut material, receipt);
    }
    append_optional_metadata(&mut material, &report.last_metadata);
    etag(&material)
}

pub(crate) fn meta_service_bootstrap_report_checksum(
    report: &MetadataServiceBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, META_SERVICE_BOOTSTRAP_REPORT_FORMAT);
    append_metadata_bootstrap_readiness(&mut material, &report.before_readiness);
    append_metadata_bootstrap_readiness(&mut material, &report.after_readiness);
    append_u64(&mut material, report.raft_report.is_some() as u64);
    if let Some(raft_report) = &report.raft_report {
        append_string(&mut material, raft_report.report_checksum_format());
        append_string(&mut material, &raft_report.report_checksum());
    }
    append_u64(&mut material, report.coordinator_report.is_some() as u64);
    if let Some(coordinator_report) = &report.coordinator_report {
        append_string(&mut material, coordinator_report.report_checksum_format());
        append_string(&mut material, &coordinator_report.report_checksum());
    }
    etag(&material)
}

pub(crate) fn meta_coordinator_bootstrap_report_checksum(
    report: &MetadataCoordinatorBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, META_COORDINATOR_BOOTSTRAP_REPORT_FORMAT);
    append_metadata_coordinator_config(&mut material, &report.config);
    append_string(&mut material, &report.config.normalized_backend());
    append_string(&mut material, &report.config.policy_checksum());
    append_metadata_coordinator_config(&mut material, &report.applied_config);
    append_string(&mut material, &report.applied_config.normalized_backend());
    append_string(&mut material, &report.applied_config.policy_checksum());
    append_metadata_bootstrap_readiness(&mut material, &report.before_readiness);
    append_metadata_bootstrap_readiness(&mut material, &report.after_readiness);
    etag(&material)
}

pub(crate) fn chunk_route_bootstrap_report_checksum(
    report: &ChunkRouteControlBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, PAGE_ROUTE_BOOTSTRAP_REPORT_FORMAT);
    append_u64(&mut material, report.target_commit_index);
    append_u64(&mut material, report.max_steps);
    append_u64(&mut material, report.applied_steps);
    append_page_route_control_proof(&mut material, &report.before_proof);
    append_page_route_control_proof(&mut material, &report.after_proof);
    append_optional_metadata(&mut material, &report.last_metadata);
    etag(&material)
}

pub(crate) fn chunk_service_bootstrap_report_checksum(
    report: &ChunkServiceBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, PAGE_SERVICE_BOOTSTRAP_REPORT_FORMAT);
    append_string(
        &mut material,
        &etag(&page_storage_readiness_fingerprint_material(
            &report.before_readiness,
        )),
    );
    append_string(
        &mut material,
        &etag(&page_storage_readiness_fingerprint_material(
            &report.after_readiness,
        )),
    );
    append_u64(&mut material, report.route_report.is_some() as u64);
    if let Some(route_report) = &report.route_report {
        append_string(&mut material, route_report.report_checksum_format());
        append_string(&mut material, &route_report.report_checksum());
    }
    etag(&material)
}

pub(crate) fn chunk_mutation_bootstrap_report_checksum(
    report: &ReplicaMutationControlBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, CHUNK_MUTATION_BOOTSTRAP_REPORT_FORMAT);
    append_u64(&mut material, report.replica_index as u64);
    append_u64(&mut material, report.target_commit_index);
    append_u64(&mut material, report.max_steps);
    append_u64(&mut material, report.applied_steps);
    append_replica_service_control_proof(&mut material, &report.before_proof);
    append_replica_service_control_proof(&mut material, &report.after_proof);
    append_optional_metadata(&mut material, &report.last_metadata);
    etag(&material)
}

pub(crate) fn replica_service_bootstrap_report_checksum(
    report: &ReplicaServiceBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, REPLICA_SERVICE_BOOTSTRAP_REPORT_FORMAT);
    append_u64(&mut material, report.replica_index as u64);
    append_chunk_storage_bootstrap_readiness(&mut material, &report.before_readiness);
    append_chunk_storage_bootstrap_readiness(&mut material, &report.after_readiness);
    append_u64(&mut material, report.mutation_report.is_some() as u64);
    if let Some(mutation_report) = &report.mutation_report {
        append_string(&mut material, mutation_report.report_checksum_format());
        append_string(&mut material, &mutation_report.report_checksum());
    }
    etag(&material)
}

pub(crate) fn root_catalog_bootstrap_report_checksum(
    report: &RootCatalogBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, ROOT_CATALOG_BOOTSTRAP_REPORT_FORMAT);
    append_u64(&mut material, report.target_commit_index);
    append_u64(&mut material, report.target_read_index);
    append_u64(&mut material, report.max_steps);
    append_u64(&mut material, report.applied_steps);
    append_string(&mut material, &report.heartbeat_node_id);
    append_string(&mut material, &report.before_status.control_checksum_format);
    append_string(&mut material, &report.before_status.control_checksum);
    append_string(&mut material, &report.after_status.control_checksum_format);
    append_string(&mut material, &report.after_status.control_checksum);
    append_optional_metadata(&mut material, &report.last_metadata);
    etag(&material)
}

pub(crate) fn root_service_bootstrap_report_checksum(
    report: &RootServiceBootstrapReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, ROOT_SERVICE_BOOTSTRAP_REPORT_FORMAT);
    append_string(&mut material, &report.before_status.control_checksum_format);
    append_string(&mut material, &report.before_status.control_checksum);
    append_string(&mut material, &report.after_status.control_checksum_format);
    append_string(&mut material, &report.after_status.control_checksum);
    append_u64(&mut material, report.catalog_report.is_some() as u64);
    if let Some(catalog_report) = &report.catalog_report {
        append_string(&mut material, catalog_report.report_checksum_format());
        append_string(&mut material, &catalog_report.report_checksum());
    }
    etag(&material)
}

