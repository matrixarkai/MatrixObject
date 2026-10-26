// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_root_snapshot_proof_material(
    material: &mut Vec<u8>,
    offset: u64,
    payload_len: u64,
    page_checksum: &str,
    proof: &PlacementReadProof,
) {
    append_u64(material, offset);
    append_u64(material, payload_len);
    append_string(material, page_checksum);
    append_string(material, &proof.proof_format);
    append_string(material, &proof.operation);
    append_u64(material, proof.requested_root_read_index);
    append_u64(material, proof.requested_root_commit_index);
    append_u64(material, proof.root_applied_index);
    append_u64(material, proof.root_read_index_lag);
    append_u64(material, proof.root_raft_term);
    append_string(material, &proof.root_state_fingerprint);
    append_string(material, &proof.proof_checksum);
}

pub(crate) fn root_snapshot_proof_material_capacity(
    total_bytes: u64,
    max_payload_bytes: usize,
) -> usize {
    snapshot_proof_material_capacity(total_bytes, max_payload_bytes, 224)
}

pub(crate) fn finish_root_snapshot_proof_stream_report(
    material: Vec<u8>,
    proof_count: u64,
    operation: String,
    requested_root_read_index: u64,
    requested_root_commit_index: u64,
    root_applied_index: u64,
    root_raft_term: u64,
    root_state_fingerprint: String,
    total_payload_bytes: u64,
    first_offset: u64,
    last_end_offset: u64,
    full_snapshot_checksum: String,
) -> PlacementSnapshotProofStreamReport {
    PlacementSnapshotProofStreamReport {
        format: "MXOS-RUST-ROOT-SNAPSHOT-READ-PROOFS1".to_string(),
        proof_count,
        aggregate_checksum: etag(&material),
        operation,
        requested_root_read_index,
        requested_root_commit_index,
        root_applied_index,
        root_raft_term,
        root_state_fingerprint,
        total_payload_bytes,
        first_offset,
        last_end_offset,
        full_snapshot_checksum,
    }
}

pub(crate) fn verify_root_snapshot_proof_stream_report(
    expected: &PlacementSnapshotProofStreamReport,
    actual: &PlacementSnapshotProofStreamReport,
    min_read_index: u64,
    min_commit_index: u64,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "root snapshot proof report format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.requested_root_read_index < min_read_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "root snapshot proof report read fence {} is below required {}",
            expected.requested_root_read_index, min_read_index
        )));
    }
    if expected.requested_root_commit_index < min_commit_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "root snapshot proof report commit fence {} is below required {}",
            expected.requested_root_commit_index, min_commit_index
        )));
    }
    if expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "root snapshot proof report mismatch: expected aggregate {} got {}",
            expected.aggregate_checksum, actual.aggregate_checksum
        )));
    }
    Ok(())
}
