// SPDX-License-Identifier: Apache-2.0
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

pub(crate) fn append_prepared_metadata_coordinator_session_expiry_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedMetadataCoordinatorSessionExpiryEnvelope,
) {
    append_u64(out, envelope.now_ms);
    append_u64(out, envelope.limit);
    append_u64(out, envelope.include_fenced as u64);
    append_u64(out, envelope.expired_session_count);
    append_u64(out, envelope.endpoint_fenced_expired_session_count);
    append_u64(out, envelope.expired_payload_bytes);
    append_string(out, &envelope.first_session_id);
    append_string(out, &envelope.first_owner);
    append_u64(out, envelope.first_epoch);
    append_u64(out, envelope.first_expire_time_ms);
    append_string(out, &envelope.metadata_control_proof_format);
    append_string(out, &envelope.metadata_control_proof_checksum);
    append_u64(out, envelope.metadata_control_proof_bytes);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_metadata_coordinator_session_expiry_envelope_checksum(
    envelope: &PreparedMetadataCoordinatorSessionExpiryEnvelope,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-COORDINATOR-SESSION-EXPIRY-ENVELOPE3",
    );
    append_prepared_metadata_coordinator_session_expiry_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn prepared_metadata_coordinator_session_expiry_envelope(
    prepared: &PreparedMetadataCoordinatorSessionExpiry,
) -> Result<PreparedMetadataCoordinatorSessionExpiryEnvelope, ObjectError> {
    let mut expired_payload_bytes = 0u64;
    for session in &prepared.expired {
        expired_payload_bytes =
            expired_payload_bytes.saturating_add(coordinator_session_payload_bytes(session)?);
    }
    let first = prepared.expired.first();
    let endpoint_fenced_expired_session_count = prepared
        .expired
        .iter()
        .filter(|session| session.expire_time_ms > prepared.now_ms && prepared.include_fenced)
        .count() as u64;
    let mut envelope = PreparedMetadataCoordinatorSessionExpiryEnvelope {
        now_ms: prepared.now_ms,
        limit: prepared.limit as u64,
        include_fenced: prepared.include_fenced,
        expired_session_count: prepared.expired.len() as u64,
        endpoint_fenced_expired_session_count,
        expired_payload_bytes,
        first_session_id: first
            .map(|session| session.session_id.clone())
            .unwrap_or_default(),
        first_owner: first
            .map(|session| session.owner.clone())
            .unwrap_or_default(),
        first_epoch: first.map_or(0, |session| session.epoch),
        first_expire_time_ms: first.map_or(0, |session| session.expire_time_ms),
        metadata_control_proof_format: prepared.metadata_control_proof_format.clone(),
        metadata_control_proof_checksum: prepared.metadata_control_proof_checksum.clone(),
        metadata_control_proof_bytes: prepared.metadata_control_proof_bytes,
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum =
        prepared_metadata_coordinator_session_expiry_envelope_checksum(&envelope);
    Ok(envelope)
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
