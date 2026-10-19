// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_coordinator_session(out: &mut Vec<u8>, session: &CoordinatorSession) {
    append_string(out, &session.session_id);
    append_string(out, &session.owner);
    append_string(out, &session.endpoint);
    append_u64(out, session.epoch);
    append_u64(out, session.acquire_time_ms);
    append_u64(out, session.last_renew_ms);
    append_u64(out, session.expire_time_ms);
    append_u64(out, session.ttl_ms);
}

pub(crate) fn append_metadata_coordinator_config(
    out: &mut Vec<u8>,
    config: &MetadataCoordinatorConfig,
) {
    append_string(out, &config.backend);
    append_u64(out, config.endpoints.len() as u64);
    for endpoint in &config.endpoints {
        append_string(out, endpoint);
    }
    append_string(out, &config.namespace_path);
    append_u64(out, config.session_ttl_ms);
    append_u64(out, config.revision);
}

pub(crate) fn append_metadata_coordinator_config_receipt(
    out: &mut Vec<u8>,
    receipt: &MetadataCoordinatorConfigReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.request_checksum);
    append_metadata_coordinator_config(out, &receipt.config);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn metadata_coordinator_config_receipt_proof_checksum(
    receipt: &MetadataCoordinatorConfigReceipt,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-COORDINATOR-CONFIG-RECEIPT-PROOF1");
    append_metadata_coordinator_config_receipt(&mut material, receipt);
    etag(&material)
}
