// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn metadata_coordinator_config_request_checksum(
    config: &MetadataCoordinatorConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-COORDINATOR-CONFIG-REQUEST1");
    append_metadata_coordinator_config(&mut material, config);
    etag(&material)
}

pub(crate) fn prepared_metadata_coordinator_config_checksum(
    prepared: &PreparedMetadataCoordinatorConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-COORDINATOR-CONFIG3");
    append_metadata_coordinator_config(&mut material, &prepared.config);
    append_string(&mut material, &prepared.idempotency_key);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

pub(crate) fn prepared_metadata_coordinator_session_expiry_checksum(
    prepared: &PreparedMetadataCoordinatorSessionExpiry,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-COORDINATOR-SESSION-EXPIRY3",
    );
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.now_ms);
    append_u64(&mut material, prepared.limit as u64);
    append_u64(&mut material, prepared.include_fenced as u64);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expired.len() as u64);
    for session in &prepared.expired {
        append_coordinator_session(&mut material, session);
    }
    etag(&material)
}

pub(crate) fn prepared_metadata_coordinator_session_checksum(
    prepared: &PreparedMetadataCoordinatorSession,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-COORDINATOR-SESSION2");
    append_string(&mut material, &prepared.operation);
    append_coordinator_session(&mut material, &prepared.session);
    append_u64(&mut material, prepared.released as u64);
    append_string(&mut material, &prepared.idempotency_key);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_now_ms);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}
