// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn validate_chunk_state_handoff(handoff: &ReplicaStateHandoff) -> Result<(), ObjectError> {
    if handoff.payload_checksum != etag(&handoff.state.data) {
        return Err(ObjectError::Corruption(
            "chunk state handoff payload checksum mismatch".to_string(),
        ));
    }
    validate_chunk_state(&handoff.state)?;
    if handoff.handoff_token.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "chunk state handoff token is required".to_string(),
        ));
    }
    if handoff.install_id.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "chunk state handoff install id is required".to_string(),
        ));
    }
    let legacy_expected =
        chunk_state_handoff_checksum(&handoff.page_id, &handoff.state, &handoff.payload_checksum);
    let token_bound_expected = chunk_state_handoff_checksum_v2(
        &handoff.page_id,
        &handoff.handoff_token,
        &handoff.state,
        &handoff.payload_checksum,
    );
    if handoff.proof_version > 2 || handoff.proof_version == 1 {
        return Err(ObjectError::InvalidArgument(
            "chunk state handoff proof version is unsupported".to_string(),
        ));
    }
    if handoff.proof_version == 2 && handoff.state_checksum != token_bound_expected {
        return Err(ObjectError::Corruption(
            "chunk state handoff token-bound envelope checksum mismatch".to_string(),
        ));
    }
    if handoff.proof_version == 0
        && handoff.state_checksum != token_bound_expected
        && handoff.state_checksum != legacy_expected
    {
        return Err(ObjectError::Corruption(
            "chunk state handoff envelope checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

pub(crate) const PAGE_STORAGE_READINESS_FINGERPRINT_FORMAT: &str = "MOSRSBLOCK-FINGERPRINT17";
pub(crate) const CHUNK_STORAGE_READINESS_FINGERPRINT_FORMAT: &str = "MOSRSCHUNK-AGG-FINGERPRINT8";

