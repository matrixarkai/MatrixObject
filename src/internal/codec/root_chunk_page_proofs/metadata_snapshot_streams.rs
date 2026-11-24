// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn verify_metadata_snapshot_proof_stream_report(
    expected: &MetadataSnapshotProofStreamReport,
    actual: &MetadataSnapshotProofStreamReport,
    min_read_index: u64,
    min_commit_index: u64,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata snapshot proof report format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.consensus_read_index < min_read_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata snapshot proof report read fence {} is below required {}",
            expected.consensus_read_index, min_read_index
        )));
    }
    if expected.metadata_commit_index < min_commit_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata snapshot proof report commit fence {} is below required {}",
            expected.metadata_commit_index, min_commit_index
        )));
    }
    if expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata snapshot proof report mismatch: expected aggregate {} got {}",
            expected.aggregate_checksum, actual.aggregate_checksum
        )));
    }
    Ok(())
}

pub(crate) fn metadata_snapshot_install_source_proof_checksum(
    expected_snapshot_read_proof_checksum: &str,
    source_read_index: u64,
    source_commit_index: u64,
    source_applied_index: u64,
    source_term: u64,
    full_snapshot_checksum: &str,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-RUST-METADATA-SNAPSHOT-INSTALL-SOURCE-PROOF1",
    );
    append_string(&mut material, expected_snapshot_read_proof_checksum);
    append_u64(&mut material, source_read_index);
    append_u64(&mut material, source_commit_index);
    append_u64(&mut material, source_applied_index);
    append_u64(&mut material, source_term);
    append_string(&mut material, full_snapshot_checksum);
    etag(&material)
}

pub fn metadata_snapshot_install_source_proof_report(
    report: &MetadataSnapshotProofStreamReport,
) -> MetadataSnapshotInstallSourceProofReport {
    let envelope_checksum = metadata_snapshot_install_source_proof_checksum(
        &report.aggregate_checksum,
        report.consensus_read_index,
        report.metadata_commit_index,
        report.snapshot_last_applied_index,
        report.snapshot_term,
        &report.full_snapshot_checksum,
    );
    MetadataSnapshotInstallSourceProofReport {
        format: "MXOS-RUST-METADATA-SNAPSHOT-INSTALL-SOURCE-PROOF1".to_string(),
        expected_snapshot_read_proof_checksum: report.aggregate_checksum.clone(),
        source_read_index: report.consensus_read_index,
        source_commit_index: report.metadata_commit_index,
        source_applied_index: report.snapshot_last_applied_index,
        source_term: report.snapshot_term,
        full_snapshot_checksum: report.full_snapshot_checksum.clone(),
        envelope_checksum,
    }
}

pub(crate) fn verify_metadata_snapshot_install_source_proof_report(
    expected: &MetadataSnapshotInstallSourceProofReport,
    actual: &MetadataSnapshotInstallSourceProofReport,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata snapshot install source proof format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.expected_snapshot_read_proof_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "metadata snapshot install source proof is missing read-proof checksum".to_string(),
        ));
    }
    if expected.full_snapshot_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "metadata snapshot install source proof is missing snapshot checksum".to_string(),
        ));
    }
    if expected.source_read_index < expected.source_commit_index {
        return Err(ObjectError::PreconditionFailed(
            "metadata snapshot install source read index is below source commit index".to_string(),
        ));
    }
    if expected.source_commit_index < expected.source_applied_index {
        return Err(ObjectError::PreconditionFailed(
            "metadata snapshot install source commit index is below applied index".to_string(),
        ));
    }
    if expected.envelope_checksum != actual.envelope_checksum || expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "metadata snapshot install source proof mismatch: expected envelope {} got {}",
            expected.envelope_checksum, actual.envelope_checksum
        )));
    }
    Ok(())
}

