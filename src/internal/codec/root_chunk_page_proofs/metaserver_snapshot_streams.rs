// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_metaserver_snapshot_proof_material(
    material: &mut Vec<u8>,
    offset: u64,
    payload_len: u64,
    page_checksum: &str,
    snapshot_last_applied_index: u64,
    proof: &ObjectReadProof,
) {
    append_u64(material, offset);
    append_u64(material, payload_len);
    append_string(material, page_checksum);
    append_u64(material, snapshot_last_applied_index);
    append_u64(material, proof.requested_read_index);
    append_u64(material, proof.consensus_read_index);
    append_u64(material, proof.metadata_commit_index);
    append_u64(material, proof.read_fence_read_index_lag);
    append_u64(material, proof.read_fence_applied_lag);
}

pub(crate) fn metaserver_snapshot_proof_material_capacity(
    total_bytes: u64,
    max_payload_bytes: usize,
) -> usize {
    snapshot_proof_material_capacity(total_bytes, max_payload_bytes, 120)
}

pub(crate) fn finish_metaserver_snapshot_proof_stream_report(
    material: Vec<u8>,
    proof_count: u64,
    requested_read_index: u64,
    consensus_read_index: u64,
    metadata_commit_index: u64,
    total_payload_bytes: u64,
    first_offset: u64,
    last_end_offset: u64,
    snapshot_last_applied_index: u64,
    full_snapshot_checksum: String,
) -> MetaServerSnapshotProofStreamReport {
    MetaServerSnapshotProofStreamReport {
        format: "MXOS-RUST-METASERVER-SNAPSHOT-READ-PROOFS1".to_string(),
        proof_count,
        aggregate_checksum: etag(&material),
        requested_read_index,
        consensus_read_index,
        metadata_commit_index,
        total_payload_bytes,
        first_offset,
        last_end_offset,
        snapshot_last_applied_index,
        full_snapshot_checksum,
    }
}

pub(crate) fn verify_metaserver_snapshot_proof_stream_report(
    expected: &MetaServerSnapshotProofStreamReport,
    actual: &MetaServerSnapshotProofStreamReport,
    min_read_index: u64,
    min_commit_index: u64,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "metaserver snapshot proof report format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.consensus_read_index < min_read_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "metaserver snapshot proof report read fence {} is below required {}",
            expected.consensus_read_index, min_read_index
        )));
    }
    if expected.metadata_commit_index < min_commit_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "metaserver snapshot proof report commit fence {} is below required {}",
            expected.metadata_commit_index, min_commit_index
        )));
    }
    if expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "metaserver snapshot proof report mismatch: expected aggregate {} got {}",
            expected.aggregate_checksum, actual.aggregate_checksum
        )));
    }
    Ok(())
}

