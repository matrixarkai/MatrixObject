// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn verify_placement_snapshot_proof_stream_report(
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
    if expected.requested_placement_read_index < min_read_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "root snapshot proof report read fence {} is below required {}",
            expected.requested_placement_read_index, min_read_index
        )));
    }
    if expected.requested_placement_commit_index < min_commit_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "root snapshot proof report commit fence {} is below required {}",
            expected.requested_placement_commit_index, min_commit_index
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
