// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn verify_storage_snapshot_proof_stream_report(
    expected: &StorageSnapshotProofStreamReport,
    actual: &StorageSnapshotProofStreamReport,
    expected_source_control_kind: &str,
    expected_source_control_checksum: Option<&str>,
    min_read_index: u64,
    min_commit_index: u64,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report kind mismatch: expected {} got {}",
            expected_source_control_kind, expected.source_control_kind
        )));
    }
    if actual.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof stream kind mismatch: expected {} got {}",
            expected_source_control_kind, actual.source_control_kind
        )));
    }
    if let Some(expected_checksum) = expected_source_control_checksum {
        if expected.source_control_checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "storage snapshot proof report source control checksum mismatch: expected {} got {}",
                expected_checksum, expected.source_control_checksum
            )));
        }
    }
    if expected.source_read_index < min_read_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report read fence {} is below required {}",
            expected.source_read_index, min_read_index
        )));
    }
    if expected.source_commit_index < min_commit_index {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report commit fence {} is below required {}",
            expected.source_commit_index, min_commit_index
        )));
    }
    if expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot proof report mismatch: expected aggregate {} got {}",
            expected.aggregate_checksum, actual.aggregate_checksum
        )));
    }
    Ok(())
}

pub(crate) fn storage_snapshot_install_source_proof_checksum(
    source_control_kind: &str,
    expected_snapshot_read_proof_checksum: &str,
    expected_source_control_checksum: &str,
    source_read_index: u64,
    source_commit_index: u64,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MXOS-RUST-STORAGE-SNAPSHOT-INSTALL-SOURCE-PROOF1",
    );
    append_string(&mut material, source_control_kind);
    append_string(&mut material, expected_snapshot_read_proof_checksum);
    append_string(&mut material, expected_source_control_checksum);
    append_u64(&mut material, source_read_index);
    append_u64(&mut material, source_commit_index);
    etag(&material)
}

pub fn storage_snapshot_install_source_proof_report(
    report: &StorageSnapshotProofStreamReport,
) -> StorageSnapshotInstallSourceProofReport {
    let envelope_checksum = storage_snapshot_install_source_proof_checksum(
        &report.source_control_kind,
        &report.aggregate_checksum,
        &report.source_control_checksum,
        report.source_read_index,
        report.source_commit_index,
    );
    StorageSnapshotInstallSourceProofReport {
        format: "MXOS-RUST-STORAGE-SNAPSHOT-INSTALL-SOURCE-PROOF1".to_string(),
        source_control_kind: report.source_control_kind.clone(),
        expected_snapshot_read_proof_checksum: report.aggregate_checksum.clone(),
        expected_source_control_checksum: report.source_control_checksum.clone(),
        source_read_index: report.source_read_index,
        source_commit_index: report.source_commit_index,
        envelope_checksum,
    }
}

pub(crate) fn verify_storage_snapshot_install_source_proof_report(
    expected: &StorageSnapshotInstallSourceProofReport,
    actual: &StorageSnapshotInstallSourceProofReport,
    expected_source_control_kind: &str,
) -> Result<(), ObjectError> {
    if expected.format != actual.format {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof format mismatch: expected {} got {}",
            expected.format, actual.format
        )));
    }
    if expected.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof kind mismatch: expected {} got {}",
            expected_source_control_kind, expected.source_control_kind
        )));
    }
    if actual.source_control_kind != expected_source_control_kind {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof actual kind mismatch: expected {} got {}",
            expected_source_control_kind, actual.source_control_kind
        )));
    }
    if expected.expected_snapshot_read_proof_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "storage snapshot install source proof is missing read-proof checksum".to_string(),
        ));
    }
    if expected.expected_source_control_checksum.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "storage snapshot install source proof is missing source control checksum".to_string(),
        ));
    }
    if expected.source_read_index < expected.source_commit_index {
        return Err(ObjectError::PreconditionFailed(
            "storage snapshot install source read index is below source commit index".to_string(),
        ));
    }
    if expected.envelope_checksum != actual.envelope_checksum || expected != actual {
        return Err(ObjectError::PreconditionFailed(format!(
            "storage snapshot install source proof mismatch: expected envelope {} got {}",
            expected.envelope_checksum, actual.envelope_checksum
        )));
    }
    Ok(())
}

