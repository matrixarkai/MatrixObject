// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_metadata_retention_config_report(
    out: &mut Vec<u8>,
    report: &MetadataRetentionConfigReport,
) {
    append_u64(out, report.change_log_retention_limit);
    append_u64(out, report.mutation_receipt_retention_limit);
    append_u64(out, report.trimmed_change_records);
    append_u64(out, report.trimmed_batch_receipts);
    append_u64(out, report.trimmed_lease_receipts);
    append_u64(out, report.trimmed_coordinator_config_receipts);
    append_u64(out, report.trimmed_coordinator_session_receipts);
    append_u64(out, report.trimmed_shard_receipts);
    append_u64(out, report.trimmed_transaction_receipts);
    append_u64(out, report.retained_change_records);
    append_u64(out, report.retained_mutation_receipts);
}

pub(crate) fn metadata_retention_config_request_checksum(
    change_log_retention_limit: usize,
    mutation_receipt_retention_limit: usize,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-RETENTION-CONFIG-REQUEST1");
    append_u64(&mut material, change_log_retention_limit as u64);
    append_u64(&mut material, mutation_receipt_retention_limit as u64);
    etag(&material)
}

pub(crate) fn prepared_metadata_retention_config_checksum(
    prepared: &PreparedMetadataRetentionConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-RETENTION-CONFIG3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.change_log_retention_limit as u64);
    append_u64(
        &mut material,
        prepared.mutation_receipt_retention_limit as u64,
    );
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_string(&mut material, &prepared.request_checksum);
    append_metadata_retention_config_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn metadata_admission_policy_request_checksum(
    policy: &MetadataAdmissionPolicy,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-ADMISSION-POLICY-REQUEST1");
    append_metadata_admission_policy(&mut material, policy);
    etag(&material)
}

pub(crate) fn prepared_metadata_admission_policy_checksum(
    prepared: &PreparedMetadataAdmissionPolicy,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-ADMISSION-POLICY3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_metadata_admission_policy(&mut material, &prepared.policy);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_string(&mut material, &prepared.request_checksum);
    etag(&material)
}

pub(crate) fn metadata_qos_config_request_checksum(config: &MetadataQosConfig) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-QOS-CONFIG-REQUEST1");
    append_metadata_qos_config(&mut material, config);
    etag(&material)
}

pub(crate) fn prepared_metadata_qos_config_checksum(
    prepared: &PreparedMetadataQosConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-QOS-CONFIG3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_metadata_qos_config(&mut material, &prepared.config);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_string(&mut material, &prepared.request_checksum);
    etag(&material)
}

pub(crate) fn append_metadata_route_cache_config(
    material: &mut Vec<u8>,
    config: &MetadataRouteCacheConfig,
) {
    append_u64(material, config.entries as u64);
    append_u64(material, config.ttl_ms);
}

pub(crate) fn metadata_route_cache_config_request_checksum(
    config: &MetadataRouteCacheConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-ROUTE-CACHE-CONFIG-REQUEST1");
    append_metadata_route_cache_config(&mut material, config);
    etag(&material)
}

pub(crate) fn prepared_metadata_route_cache_config_checksum(
    prepared: &PreparedMetadataRouteCacheConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-ROUTE-CACHE-CONFIG3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_metadata_route_cache_config(&mut material, &prepared.config);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.after_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_string(&mut material, &prepared.request_checksum);
    etag(&material)
}

