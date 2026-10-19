// SPDX-License-Identifier: Apache-2.0
pub(crate) fn metadata_lease_request_checksum(
    operation: &str,
    namespace: &str,
    owner: &str,
    epoch: u64,
    ttl_ms: u64,
    now_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, operation);
    append_string(&mut material, namespace);
    append_string(&mut material, owner);
    append_u64(&mut material, epoch);
    append_u64(&mut material, ttl_ms);
    append_u64(&mut material, now_ms);
    etag(&material)
}

pub(crate) fn metadata_lease_expiry_request_checksum(
    prefix: &str,
    now_ms: u64,
    limit: usize,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-LEASE-EXPIRY-REQUEST1");
    append_string(&mut material, prefix);
    append_u64(&mut material, now_ms);
    append_u64(&mut material, limit as u64);
    etag(&material)
}

pub(crate) fn prepared_metadata_lease_checksum(prepared: &PreparedMetadataLease) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-LEASE1");
    append_string(&mut material, &prepared.operation);
    append_string(&mut material, &prepared.namespace);
    append_string(&mut material, &prepared.owner);
    append_u64(&mut material, prepared.epoch);
    append_u64(&mut material, prepared.ttl_ms);
    append_u64(&mut material, prepared.now_ms);
    append_metadata_lease(&mut material, &prepared.lease);
    append_u64(&mut material, prepared.released as u64);
    append_string(&mut material, &prepared.request_checksum);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.after_state_fingerprint);
    append_u64(&mut material, prepared.metadata_read_index);
    append_u64(&mut material, prepared.metadata_raft_term);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

pub(crate) fn prepared_metadata_lease_expiry_checksum(
    prepared: &PreparedMetadataLeaseExpiry,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-LEASE-EXPIRY3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.prefix);
    append_u64(&mut material, prepared.now_ms);
    append_u64(&mut material, prepared.limit as u64);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expired.len() as u64);
    for lease in &prepared.expired {
        append_metadata_lease(&mut material, lease);
    }
    etag(&material)
}

pub(crate) fn metadata_transaction_request_checksum(
    operation: &str,
    transaction_id: &str,
    owner: &str,
    ttl_ms: u64,
    mutations: &[MetadataMutation],
    now_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, operation);
    append_string(&mut material, transaction_id);
    append_string(&mut material, owner);
    append_u64(&mut material, ttl_ms);
    append_u64(&mut material, now_ms);
    append_u64(&mut material, mutations.len() as u64);
    for mutation in mutations {
        append_metadata_mutation(&mut material, mutation);
    }
    etag(&material)
}

pub(crate) fn metadata_coordinator_session_request_checksum(
    operation: &str,
    session_id: &str,
    owner: &str,
    endpoint: &str,
    epoch: u64,
    ttl_ms: u64,
    now_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, operation);
    append_string(&mut material, session_id);
    append_string(&mut material, owner);
    append_string(&mut material, endpoint);
    append_u64(&mut material, epoch);
    append_u64(&mut material, ttl_ms);
    append_u64(&mut material, now_ms);
    etag(&material)
}

pub(crate) fn metadata_coordinator_session_expiry_request_checksum(
    now_ms: u64,
    limit: usize,
    include_fenced: bool,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-META-COORDINATOR-SESSION-EXPIRY-REQUEST2",
    );
    append_u64(&mut material, now_ms);
    append_u64(&mut material, limit as u64);
    append_u64(&mut material, include_fenced as u64);
    etag(&material)
}

pub(crate) fn metadata_maintenance_request_checksum(
    now_ms: u64,
    lease_prefix: &str,
    lease_limit: usize,
    session_limit: usize,
    transaction_limit: usize,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-MAINTENANCE-REQUEST1");
    append_u64(&mut material, now_ms);
    append_string(&mut material, lease_prefix);
    append_u64(&mut material, lease_limit as u64);
    append_u64(&mut material, session_limit as u64);
    append_u64(&mut material, transaction_limit as u64);
    etag(&material)
}

